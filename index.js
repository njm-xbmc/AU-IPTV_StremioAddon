/**
 * AU IPTV — v2.8.1 (EPG Background Load + Fast Catalog)
 * - Now Powered By ELFHOSTED thanks funkypenguin
 * - AU/NZ live channels (i.mjh.nz)
 * - Curated packs (A1X) + multi-quality variants
 * - Dynamic "Additional Packs" from external M3U (Rogue VIP works as before)
 * - Rogue/A1x XMLTV EPG (US/UK/Sports focus, trimmed XML) applied to curated/extras where IDs match
 * - Stremio addon + simple landing served from /public/index.html
 */

'use strict';

// ---- logging control ----
const LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase(); // debug|info|warn|error|silent
const LOG_MUTE  = (process.env.LOG_MUTE  || '').toLowerCase().split(',').map(s=>s.trim()).filter(Boolean);

const _orig = { log: console.log, warn: console.warn, error: console.error };

const sax = require('sax');
const { Readable } = require('stream');

function toNodeStream(webOrNode) {
  // undici (Lambda/Node18) -> WHATWG ReadableStream
  if (webOrNode && typeof webOrNode.getReader === 'function') {
    return Readable.fromWeb(webOrNode);
  }
  return webOrNode; // already Node stream
}
function isGzipLike(url, headers) {
  const ce = (headers.get('content-encoding') || '').toLowerCase();
  const ct = (headers.get('content-type') || '').toLowerCase();
  return /\.gz($|\?)/i.test(url) || ct.includes('gzip') || ct.includes('x-gzip');
}

/**
 * Stream-parse XMLTV -> { programmes: Map, nameIndex: Map }
 * Memory stays flat; no giant strings or xml2js object trees.
 */
function parseXmltvStream(nodeReadable) {
  return new Promise((resolve, reject) => {
    const saxStream = sax.createStream(true);
    const programmes = new Map();
    const nameIndex = new Map();

    let currentProgramme = null;
    let currentChannel = null;
    let currentTag = '';

    saxStream.on('error', reject);
    saxStream.on('end', () => resolve({ programmes, nameIndex }));

    saxStream.on('opentag', (node) => {
      currentTag = node.name;
      if (currentTag === 'programme') {
        currentProgramme = {
          start: node.attributes?.start || '',
          stop: node.attributes?.stop || '',
          channel: node.attributes?.channel || '',
          title: '',
        };
      } else if (currentTag === 'channel') {
        currentChannel = { id: node.attributes?.id || '', name: '' };
      }
    });

    saxStream.on('text', (txt) => {
      if (!txt || !txt.trim()) return;
      if (currentProgramme && currentTag === 'title') currentProgramme.title += txt;
      else if (currentChannel && currentTag === 'display-name') currentChannel.name += txt;
    });

    saxStream.on('closetag', (tag) => {
      if (tag === 'programme' && currentProgramme?.channel) {
        const arr = programmes.get(currentProgramme.channel) || [];
        arr.push({
          start: currentProgramme.start || '',
          stop:  currentProgramme.stop  || '',
          title: (currentProgramme.title || '').trim(),
        });
        programmes.set(currentProgramme.channel, arr);
        currentProgramme = null;
      } else if (tag === 'channel' && currentChannel?.id) {
        const id = currentChannel.id;
        const name = (currentChannel.name || '').trim();
        if (id && name) {
          // strict + loose + id forms (you already have normalizeId / normalizeIdLoose)
          const n1 = normalizeId(name);
          const n2 = normalizeIdLoose(name);
          if (n1 && !nameIndex.has(n1)) nameIndex.set(n1, id);
          if (n2 && !nameIndex.has(n2)) nameIndex.set(n2, id);
          const idNorm = normalizeId(id);
          if (idNorm && !nameIndex.has(idNorm)) nameIndex.set(idNorm, id);
        }
        currentChannel = null;
      }
      currentTag = '';
    });

    nodeReadable.pipe(saxStream);
  });
}


console.log = (...a) => {
  const first = (a[0] ?? '').toString().toLowerCase();
  if (LOG_MUTE.some(t => t && first.includes(t))) return;              // mute by tag
  if (!['debug','info'].includes(LOG_LEVEL)) return;                   // hide info/debug
  _orig.log(...a);
};
console.warn  = (...a) => { if (['debug','info','warn'].includes(LOG_LEVEL)) _orig.warn(...a); };
console.error = (...a) => { if (LOG_LEVEL !== 'silent') _orig.error(...a); };


const express = require('express');
const serverless = require('serverless-http');
const { addonBuilder, getRouter } = require('stremio-addon-sdk');
const xml2js = require('xml2js');
const zlib = require('zlib');
const path = require('path');
const fs = require('fs');

// native fetch (Node 18+), fallback to node-fetch
const fetch = globalThis.fetch
  ? (...a) => globalThis.fetch(...a)
  : ((...a) => import('node-fetch').then(({ default: f }) => f(...a)));

const app = express();

// ---- shared fetch helpers ----
const DEFAULT_FETCH_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36 AUIPTV-Addon',
  'Accept': '*/*'
};

// Updated: Add handling for 'ca' -> 'US'
function scopeToRegionKey(scope = '') {
  const s = String(scope).toLowerCase();
  if (s.includes('uk')) return 'UK';
  if (s.includes('us')) return 'US';
  if (s.includes('ca')) return 'US';  // NEW: Route CA to US EPG
  if (s.includes('sport') || s.includes('epl')) return 'SPORTS';
  if (s.includes('nz')) return 'NZ';
  return 'AU';
}

// Fire-and-forget warmup; do not await
function warmRogueIndex(scope = '') {
  rogueIndexForCatalog(scope).catch(() => {});
}

function fetchWithTimeout(url, opts = {}, ms = 10000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  return fetch(url, { ...opts, signal: ctrl.signal }).finally(() => clearTimeout(t));
}

/* ------------------------- CONFIG ------------------------- */
const DEFAULT_REGION = 'Brisbane';
const REGIONS = ['Adelaide','Brisbane','Canberra','Darwin','Hobart','Melbourne','Perth','Sydney'];
const REGION_TZ = {
  Adelaide: 'Australia/Adelaide', Brisbane: 'Australia/Brisbane', Canberra: 'Australia/Sydney',
  Darwin: 'Australia/Darwin', Hobart: 'Australia/Hobart', Melbourne: 'Australia/Melbourne',
  Perth: 'Australia/Perth', Sydney: 'Australia/Sydney'
};

// stremio-addons.net signature (optional)
const STREMIO_ADDONS_CONFIG = {
  issuer: 'https://stremio-addons.net',
  signature: process.env.STREMIO_ADDONS_SIGNATURE || null
};

// Posters via /images repo (optional map.json)
const IMAGES_BASE = process.env.IMAGES_BASE
  || 'https://raw.githubusercontent.com/josharghhh/AU-IPTV_StremioAddon/main';

// NOTE: the map.json lives under /images/ in your repo
const POSTER_MAP_PATH = process.env.POSTER_MAP_PATH || path.join(__dirname, 'map.json');
const POSTER_MAP_URL  = process.env.POSTER_MAP_URL  || `${IMAGES_BASE}/images/map.json`;

let POSTER_MAP = {};
// alias table derived from POSTER_MAP
let POSTER_ALIASES = {};

/* ------------------------- EPG CONFIG (Rogue US/UK/Sports) ------------------------- */
// Canonical XMLTV endpoints we’ll merge depending on toggles/selections.
// (Kayo & Foxtel contain the Fox Sports / beIN / Main Event schedules that A1X/Rogue need)
const EPG_SOURCES = {
  AU_REGION: (region) => `https://i.mjh.nz/au/${encodeURIComponent(region)}/epg.xml`,
  NZ:        `https://i.mjh.nz/nz/epg.xml`,
  KAYO:      `https://i.mjh.nz/Kayo/epg.xml`,
  FOXTEL:    'https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz'
};

// Rogue XMLTV shards (gzipped) - Focused on US, UK, Sports for efficiency, updated URLs without OPTUS
const ROGUE_EPG_URLS = {
  AU: [
    'https://epgshare01.online/epgshare01/epg_ripper_AU1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_DUMMY_CHANNELS.xml.gz',
    `https://i.mjh.nz/Kayo/epg.xml`
  ],
  NZ: [
    `https://i.mjh.nz/nz/epg.xml`,
    'https://epgshare01.online/epgshare01/epg_ripper_NZ1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_DUMMY_CHANNELS.xml.gz'
  ],
  UK: [
    'https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_DUMMY_CHANNELS.xml.gz'
  ],
  US: [
    'https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_US_LOCALS2.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_US_SPORTS1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_DUMMY_CHANNELS.xml.gz'
  ],
  SPORTS: [
    'https://epgshare01.online/epgshare01/epg_ripper_BEIN1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_US_SPORTS1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_DIRECTVSPORTS1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_DRAFTKINGS1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_FANDUEL1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_PAC-12.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_POWERNATION1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_SPORTKLUB1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_SSPORTPLUS1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_THESPORTPLUS1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_DUMMY_CHANNELS.xml.gz',
    `https://i.mjh.nz/Kayo/epg.xml`
  ]
};
const ROGUE_EPG_TTL_MS = 20 * 60 * 1000; // 20 minutes

// Light in-memory cache to avoid re-merging huge XMLTV blobs on every hit
const EPG_CACHE_TTL_MS = 20 * 60 * 1000; // 20 minutes
const EPG_CACHE = new Map(); // key => { xml, ts }

/**
 * EPG alias map: normalize curated slugs / cids from A1X + Rogue
 * to the XMLTV ids used by i.mjh.nz feeds (Kayo / Foxtel / AU region).
 * Keys are matched case-insensitively (we lowercase on lookup).
 */
const EPG_ALIASES = {
  // Fox Sports / Kayo (most common A1X channels)
  'foxcricket.au':        'FoxCricket.au',
  'foxcricket':           'FoxCricket.au',
  'fox.cricket':          'FoxCricket.au',
  'foxleague.au':         'FoxLeague.au',
  'foxleague':            'FoxLeague.au',
  'fox.league':           'FoxLeague.au',
  'foxfooty.au':          'FoxFooty.au',
  'foxfooty':             'FoxFooty.au',
  'fox.footy':            'FoxFooty.au',
  'foxsportsmore.au':     'FoxSportsMore.au',
  'foxsportsmore':        'FoxSportsMore.au',
  'foxmore':              'FoxSportsMore.au',
  'fox.sports.more':      'FoxSportsMore.au',
  'foxsports503.au':      'FoxSports503.au',
  'foxsports503':         'FoxSports503.au',
  'fox503':               'FoxSports503.au',
  'foxsports504.au':      'FoxSports504.au',
  'foxsports504':         'FoxSports504.au',
  'fox504':               'FoxSports504.au',
  'foxsports505.au':      'FoxSports505.au',
  'foxsports505':         'FoxSports505.au',
  'fox505':               'FoxSports505.au',
  'foxsports506.au':      'FoxSports506.au',
  'foxsports506':         'FoxSports506.au',
  'fox506':               'FoxSports506.au',
  'foxsports507.au':      'FoxSports507.au',
  'foxsports507':         'FoxSports507.au',
  'fox507':               'FoxSports507.au',

  // beIN Sports (common misspellings)
  'beinsport1.au':        'beINSports1.au',
  'beinsport1':           'beINSports1.au',
  'bein1':                'beINSports1.au',
  'beinsport2.au':        'beINSports2.au',
  'beinsport2':           'beINSports2.au',
  'bein2':                'beINSports2.au',
  'beinsport3.au':        'beINSports3.au',
  'beinsport3':           'beINSports3.au',
  'bein3':                'beINSports3.au',
  'beinsports1.au':       'beINSports1.au',
  'beinsports1':          'beINSports1.au',
  'beinsports2.au':       'beINSports2.au',
  'beinsports2':          'beINSports2.au',
  'beinsports3.au':       'beINSports3.au',
  'beinsports3':          'beINSports3.au',

  // Main Event
  'main.event.ufc.au':    'MainEvent.au',
  'mainevent.au':         'MainEvent.au',
  'mainevent':            'MainEvent.au',
  'main.event':           'MainEvent.au',

  // ESPN (US)
  'espn.us':              'ESPN.us',
  'espn':                 'ESPN.us',
  'espn1':                'ESPN.us',
  'espn2.us':             'ESPN2.us',
  'espn2':                'ESPN2.us',

  // Sky Sports (UK)
  'skysportsaction.uk':        'SkySportsAction.uk',
  'skysportsaction':           'SkySportsAction.uk',
  'sky.sports.action':         'SkySportsAction.uk',
  'skysportscricket.uk':       'SkySportsCricket.uk',
  'skysportscricket':          'SkySportsCricket.uk',
  'sky.sports.cricket':        'SkySportsCricket.uk',
  'skysportsf1.uk':            'SkySportsF1.uk',
  'skysportsf1':               'SkySportsF1.uk',
  'sky.sports.f1':             'SkySportsF1.uk',
  // ... (add all the other Sky Sports entries from the artifact)

  // TNT Sports (UK)
  'tntsports1.uk':        'TNTSports1.uk',
  'tntsports1':           'TNTSports1.uk',
  'tnt1':                 'TNTSports1.uk',
  'tnt.sports.1':         'TNTSports1.uk',
  // ... (add all TNT entries)

  // Other US/UK sports networks
  'usanetwork.us':        'USANetwork.us',
  'usanetwork':           'USANetwork.us',
  'usa.network':          'USANetwork.us',
  'nbcsports.us':         'NBCSports.us',
  'nbcsports':            'NBCSports.us',
  'nbc.sports':           'NBCSports.us',
  'foxsports1.us':        'FoxSports1.us',
  'foxsports1':           'FoxSports1.us',
  'fs1':                  'FoxSports1.us',
  'fox.sports.1':         'FoxSports1.us',
  'foxsports2.us':        'FoxSports2.us',
  'foxsports2':           'FoxSports2.us',
  'fs2':                  'FoxSports2.us',
  'fox.sports.2':         'FoxSports2.us',
};

/* ----------------- Poster URL + alias helpers (HOISTED) --- */

// Resolve a relative/absolute map entry into an absolute URL
function mapUrl(key) {
  const rel = POSTER_MAP?.[key];
  if (!rel) return null;
  if (/^https?:\/\//i.test(rel)) return rel;
  const p = rel.startsWith('/images/') ? rel : `/images/${String(rel).replace(/^\/+/, '')}`;
  return `${IMAGES_BASE}${p}`;
}

// Normalization/alias helpers
const REGION_SUFFIXES = new Set(['au','us','uk','nz','ca','eu']);
function camelToWords(s='') { return String(s).replace(/([a-z])([A-Z])/g, '$1 $2'); }
function normalizePosterLookup(s='') {
  return String(s || '')
    .toLowerCase()
    .replace(/\[[^\]]*\]/g, ' ')      // drop [BU], [HD], etc.
    .replace(/\([^)]+\)/g, ' ')       // drop (unstable), (US), etc.
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '.')      // everything -> dots
    .replace(/\.+/g, '.')
    .replace(/^\.|\.$/g, '');
}
function rebuildPosterAliases() {
  POSTER_ALIASES = {};
  const add = (alias, key) => {
    const norm = normalizePosterLookup(alias);
    if (!norm) return;
    if (!POSTER_ALIASES[norm]) POSTER_ALIASES[norm] = key;
  };

  for (const key of Object.keys(POSTER_MAP || {})) {
    const k = String(key);
    const variants = new Set([
      k,
      k.replace(/\./g, ' '),
      camelToWords(k),
      camelToWords(k).replace(/\./g, ' ')
    ]);
    for (const v of variants) {
      const norm = normalizePosterLookup(v);
      add(norm, key);

      // If last token is a region code, add version without it
      const parts = norm.split('.');
      const last = parts[parts.length - 1];
      if (REGION_SUFFIXES.has(last)) add(parts.slice(0, -1).join('.'), key);

      // espn.1 <-> espn1 variants
      add(norm.replace(/\.(\d+)/g, '$1'), key);
      add(norm.replace(/(\d+)/g, '.$1'), key);
    }
  }

  // Helpful manual aliases (only if the canonical key exists in the map)
  const MANUAL = {
    'fox.cricket': 'FoxCricket.au',
    'fox.league': 'FoxLeague.au',
    'fox.footy': 'FoxFooty.au',
    'fox.more': 'FoxSportsMore.au',
    'fox.sports.more': 'FoxSportsMore.au',
    'espn': 'ESPN.us',
    'espn.1': 'ESPN.us',
    'espn1': 'ESPN.us',
    'espn.2': 'ESPN2.us',
    'espn2': 'ESPN2.us',
    'bein.sports': 'beIN Sports',
    'bein.sports.1': 'beIN Sports 1',
    'bein.sports1': 'beIN Sports 1',
    'bein.sports.2': 'beIN Sports 2',
    'bein.sports2': 'beIN Sports 2',
    'fox.more+': 'FoxSportsMore.au',
    'fox.sports.503': 'FoxSports503.au',
    'fox.sports.505': 'FoxSports505.au',
    'fox.sports.506': 'FoxSports506.au',
  };
  for (const [alias, key] of Object.entries(MANUAL)) {
    if (POSTER_MAP[key]) add(alias, key);
  }
}
function resolvePosterAlias(s='') {
  const norm = normalizePosterLookup(s);
  return POSTER_ALIASES[norm] || null;
}
function posterFromAlias(s='') {
  const key = resolvePosterAlias(s);
  return key ? mapUrl(key) : null;
}

/* --------------------- Load poster map (uses aliases) ----- */
async function refreshPosterMap() {
  try {
    // 1) Prefer local file when present (local dev / bundled deploy)
    if (fs.existsSync(POSTER_MAP_PATH)) {
      const raw = fs.readFileSync(POSTER_MAP_PATH, 'utf8');
      POSTER_MAP = JSON.parse(raw);
      console.log(`[posters] loaded ${Object.keys(POSTER_MAP).length} keys from local map.json`);
      rebuildPosterAliases();
      return;
    }

    // 2) Remote fallbacks: try /images/map.json first, then root /map.json (just in case)
    const candidates = [
      POSTER_MAP_URL,                                  // .../images/map.json
      `${IMAGES_BASE}/map.json`,                       // .../map.json (fallback)
    ];

    for (const url of candidates) {
      try {
        const r = await fetch(url, { cache: 'no-store' });
        if (!r.ok) continue;
        const json = await r.json();
        if (json && typeof json === 'object' && Object.keys(json).length) {
          POSTER_MAP = json;
          console.log(`[posters] loaded ${Object.keys(POSTER_MAP).length} keys from ${url}`);
          rebuildPosterAliases();
          return;
        }
      } catch {}
    }

    // If nothing worked, leave the map empty (addon will fall back to generic)
    throw new Error('no usable map.json found');
  } catch (e) {
    console.warn('[posters] failed to load map.json:', e.message);
    POSTER_MAP = {};
    POSTER_ALIASES = {};
  }
}
// call once on boot
refreshPosterMap();

/* --------------------------- CACHE ------------------------ */
const CACHE_TTL = 20 * 60 * 1000; // 20 min
const SHORT_TTL = Number(process.env.SHORT_TTL_MS || 90 * 1000); // 90s for tokened links
const fresh = (e, ttl = CACHE_TTL) => e && (Date.now() - e.ts) < ttl;
const validRegion = (r) => (REGIONS.includes(r) ? r : DEFAULT_REGION);

const cache = {
  // AU/NZ
  m3u: new Map(), epg: new Map(), radioM3u: new Map(),
  nz_tv_m3u: new Map(), nz_radio_m3u: new Map(), nz_epg: new Map(),

  // curated
  a1x_text: null, a1x_entries: null, curated_groups: new Map(),

  // rogue epg
  rogue_epg_idx: new Map(), // key AU|NZ|UK|US|SPORTS|ALL -> { ts, programmes, nameIndex }

  // extras
  extras_text: null, extras_groups: null
};

// stats (naive)
const STATS_SEED = Number(process.env.STATS_SEED || 498);
let _memStats = { installs: STATS_SEED };
function markInstall() { _memStats.installs = (_memStats.installs || 0) + 1; }

/* ------------------------- PARSERS ------------------------- */
function parseM3U(text) {
  const lines = String(text || '').split(/\r?\n/);
  const channels = new Map();
  let cur = null;
  for (const lineRaw of lines) {
    const line = lineRaw.trim();
    if (!line) continue;
    if (line.startsWith('#EXTINF')) {
      const name = line.split(',').pop().trim();
      const idm   = line.match(/tvg-id="([^"]+)"/i);
      const logom = line.match(/tvg-logo="([^"]+)"/i);
      const grp   = line.match(/group-title="([^"]+)"/i);
      cur = { id: idm ? idm[1] : name, name, logo: logom ? logom[1] : null, group: grp ? grp[1] : null };
    } else if (line.startsWith('#EXTGRP:')) {
      if (cur) cur.group = line.slice(8).trim();
    } else if (cur && !line.startsWith('#')) {
      const m = line.match(/https?:\/\/\S+/);
      if (m) channels.set(cur.id, { ...cur, url: m[0] });
      cur = null;
    }
  }
  return channels;
}
function parseM3UEntries(text) {
  const lines = String(text || '').split(/\r?\n/);
  const out = [];
  let meta = null;
  for (const lineRaw of lines) {
    const line = lineRaw.trim();
    if (!line) continue;
    if (line.startsWith('#EXTINF')) {
      const name  = line.split(',').pop().trim();
      const idm   = line.match(/tvg-id="([^"]+)"/i);
      const logom = line.match(/tvg-logo="([^"]+)"/i);
      const grp   = line.match(/group-title="([^"]+)"/i);
      meta = { id: idm ? idm[1] : name, name, logo: logom ? logom[1] : null, group: grp ? grp[1] : null };
    } else if (line.startsWith('#EXTGRP:')) {
      if (meta) meta.group = line.slice(8).trim();
    } else if (meta && !line.startsWith('#')) {
      const m = line.match(/https?:\/\/\S+/);
      if (m) out.push({ ...meta, url: m[0] });
      meta = null;
    }
  }
  return out;
}
function normalizeTVJson(json) {
  let items;
  if (Array.isArray(json)) items = json;
  else if (Array.isArray(json?.channels)) items = json.channels;
  else if (Array.isArray(json?.streams)) items = json.streams;
  else if (json && typeof json === 'object') {
    items = Object.entries(json).map(([k, v]) => ({
      id: v?.id || v?.tvg_id || v?.guideId || v?.channel || v?.slug || k, ...v
    }));
  } else items = [];

  const channels = new Map();
  for (const it of items) {
    const id   = it.id || it.tvg_id || it.guideId || it.channel || it.slug || it.name;
    const name = it.name || it.title || it.channel || id;
    const logo = it.logo || it.tvg_logo || it.icon || null;
    const url  = it.url || (Array.isArray(it.streams) ? it.streams[0]?.url : undefined);
    if (id && name && url) channels.set(id, { id, name, logo, url });
  }
  return channels;
}
function parseEPG(xml) {
  return new Promise((resolve, reject) => {
    xml2js.parseString(xml, (err, res) => {
      if (err) return reject(err);
      const map = new Map();
      const progs = res?.tv?.programme || [];
      for (const p of progs) {
        const cid = p.$?.channel || '';
        const start = p.$?.start || '';
        const stop  = p.$?.stop  || '';
        const title = (Array.isArray(p.title) ? p.title[0] : p.title) || '';
        if (!map.has(cid)) map.set(cid, []);
        map.get(cid).push({ start, stop, title });
      }
      resolve(map);
    });
  });
}


/**
 * Parse XMLTV with channel name indexing for Rogue EPG
 * Returns { programmes: Map(channelId -> programmes[]), nameIndex: Map(normalizedName -> channelId) }
 */
async function parseXmltvWithIndex(xmlString, buildNameIndex = true) {
  return new Promise((resolve, reject) => {
    xml2js.parseString(xmlString, (err, result) => {
      if (err) return reject(err);
      
      const programmes = new Map();
      const nameIndex = new Map();
      
      try {
        const tv = result?.tv || {};
        
        // Build name index from channels if requested
        if (buildNameIndex && tv.channel) {
          for (const ch of tv.channel) {
            const channelId = ch.$?.id || '';
            if (!channelId) continue;
            
            // Get display name
            const displayName = ch['display-name'] && ch['display-name'][0] 
              ? (typeof ch['display-name'][0] === 'string' ? ch['display-name'][0] : ch['display-name'][0]._)
              : '';
            
            if (displayName) {
              // Add various normalized versions to the index
              const normalized = normalizeId(displayName);
              const normalizedLoose = normalizeIdLoose(displayName);
              
              if (normalized && !nameIndex.has(normalized)) {
                nameIndex.set(normalized, channelId);
              }
              if (normalizedLoose && normalizedLoose !== normalized && !nameIndex.has(normalizedLoose)) {
                nameIndex.set(normalizedLoose, channelId);
              }
              
              // Also index the channel ID itself
              const idNormalized = normalizeId(channelId);
              if (idNormalized && !nameIndex.has(idNormalized)) {
                nameIndex.set(idNormalized, channelId);
              }
            }
          }
        }
        
        // Parse programmes
        if (tv.programme) {
          for (const prog of tv.programme) {
            const channelId = prog.$?.channel || '';
            const start = prog.$?.start || '';
            const stop = prog.$?.stop || '';
            
            // Extract title
            let title = '';
            if (prog.title) {
              if (Array.isArray(prog.title)) {
                const titleObj = prog.title[0];
                title = typeof titleObj === 'string' ? titleObj : (titleObj?._ || titleObj || '');
              } else {
                title = typeof prog.title === 'string' ? prog.title : (prog.title?._ || prog.title || '');
              }
            }
            
            if (channelId && start) {
              if (!programmes.has(channelId)) {
                programmes.set(channelId, []);
              }
              programmes.get(channelId).push({
                start,
                stop,
                title: String(title || '').trim()
              });
            }
          }
        }
        
        resolve({ programmes, nameIndex });
        
      } catch (parseError) {
        reject(new Error(`XMLTV parsing error: ${parseError.message}`));
      }
    });
  });
}


function parseTime(s) {
  if (/^\d{14}\s+[+\-]\d{4}$/.test(s)) {
    const y=+s.slice(0,4), mo=+s.slice(4,6)-1, d=+s.slice(6,8), h=+s.slice(8,10), m=+s.slice(10,12), sec=+s.slice(12,14);
    const off = s.slice(15);
    const sign = off[0] === '-' ? -1 : 1;
    const offMin = sign * (parseInt(off.slice(1,3))*60 + parseInt(off.slice(3,5)));
    return new Date(Date.UTC(y,mo,d,h,m,sec) - offMin*60000);
  }
  const d = new Date(s);
  return isNaN(d) ? new Date() : d;
}
const fmtLocal = (s, tz) => {
  const d = parseTime(s);
  try {
    return new Intl.DateTimeFormat('en-AU', { timeZone: tz, hour: 'numeric', minute: '2-digit' }).format(d);
  } catch {
    const hh12 = d.getHours() % 12 || 12;
    const mm2 = `${d.getMinutes()}`.padStart(2,'0');
    const ap = d.getHours() >= 12 ? 'pm' : 'am';
    return `${hh12}:${mm2}${ap}`;
  }
};
function nowProgramme(list) {
  const now = Date.now();
  for (const p of list || []) {
    const s = parseTime(p.start).getTime();
    const e = parseTime(p.stop ).getTime();
    if (!Number.isNaN(s) && !Number.isNaN(e) && s <= now && now < e) return p;
  }
  return null;
}

/* ----------------- AU classification + order --------------- */
function isOtherChannel(name = '') {
  const u = String(name).toUpperCase();
  const normalized = u.replace(/[^A-Z0-9 ]+/g, ' ').replace(/\s+/g, ' ').trim();
  const OTHER_CHANNELS = [
    'ABC BUSINESS','ABC BUSINESS IN 90 SECONDS','ABC NEWS IN 90 SECONDS',
    'ABC SPORT IN 90 SECONDS','ABC WEATHER IN 90 SECONDS','SBS ARABIC','SBS CHILL','SBS POPASIA',
    'SBS RADIO 1','SBS RADIO 2','SBS RADIO 3','SBS SOUTH ASIAN','SBS WORLD MOVIES','SBS WORLD WATCH','SBS WORLDWATCH',
    'SBS FOOD','SBS VICELAND','8 OUT OF 10 CATS'
  ];
  if (OTHER_CHANNELS.includes(u)) return true;
  if (/\bHAVE YOU BEEN PAYING ATTENTION\b/.test(normalized)) return true;
  if (/\bHYBPA\b/.test(normalized)) return true;
  if (/(?:\b8\b|\bEIGHT\b)\s*(?:OUT\s*OF\s*)?\b10\b\s*CATS(?:\s*DOES\s*COUNTDOWN)?\b/.test(normalized)) return true;
  return false;
}
const TRADITIONAL_CHANNELS = [
  ['ABC TV','ABC','ABC NEWS','ABC ME','ABC KIDS','ABC TV PLUS','ABC ENTERTAINS','ABC FAMILY'],
  ['SBS','NITV'],
  ['SEVEN','7TWO','7MATE','7FLIX','7BRAVO','CHANNEL 7','NETWORK 7'],
  ['NINE','9GEM','9GO','9GO!','9LIFE','9RUSH','CHANNEL 9','NINE'],
  ['TEN','10','10 BOLD','10 PEACH','10 SHAKE','10 COMEDY','10 DRAMA','CHANNEL 10','NETWORK 10']
];
function isTraditionalChannel(name='') {
  const u = name.toUpperCase();
  for (const group of TRADITIONAL_CHANNELS) if (group.some(g => u.includes(g))) return true;
  return false;
}
const AU_TRAD_ORDER = [
  'ABC ENTERTAINS','ABC FAMILY','ABC KIDS','ABC NEWS','ABC TV',
  'SBS','NITV',
  'SEVEN','7TWO','7MATE','7FLIX','7BRAVO',
  '9GEM','9GO!','9GO','9LIFE','9RUSH','CHANNEL 9','NINE',
  '10','10 BOLD','10 PEACH','10 SHAKE','10 COMEDY','10 DRAMA','CHANNEL 10','NETWORK 10'
];
const AU_TRAD_INDEX = new Map(AU_TRAD_ORDER.map((n,i)=>[n,i]));
const auTradOrderValue = (name='') => {
  const u = name.toUpperCase();
  for (const key of AU_TRAD_ORDER) if (u.includes(key)) return AU_TRAD_INDEX.get(key);
  return 10000;
};

/* ----------------- Poster helpers -------------------------- */

// simple slug (can't rely on later slugify const)
function _slug(s='') {
  return String(s).toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'');
}

// exact / hash-collapsed lookups only
function posterFromMapExact(k) {
  const tryKey = (x) => mapUrl(x);

  // exact id match
  let abs = tryKey(k);
  if (abs) return abs;

  // Extras "event-<num>-<hash>" -> "event-<hash>"
  const mEvent = /^event-(\d+)-([a-z0-9]+)$/i.exec(k);
  if (mEvent) {
    abs = tryKey(`event-${mEvent[2]}`);
    if (abs) return abs;
  }

  // Generic "<slug>-<num>-<hash>" -> "<slug>-<hash>"
  const mGeneric = /^([a-z0-9-]+?)-(\d+)-([a-z0-9]+)$/i.exec(k);
  if (mGeneric) {
    abs = tryKey(`${mGeneric[1]}-${mGeneric[3]}`);
    if (abs) return abs;
  }

  // FIX: brand collapse "<brand>-<anything>-<hash>" -> "<brand>-<hash>"
  const mBrand = /^(epl|dirtvision|dazn|tnt(?:-sports)?|sky(?:-sports?)?|fox|espn|bein(?:-sports)?|main(?:-event)?|stan(?:-sport)?|ppv)-[a-z0-9-]+-([a-z0-9]+)$/i.exec(k);
  if (mBrand) {
    const brand = mBrand[1]
      .toLowerCase()
      .replace(/^tnt(?:-sports)?$/, 'tnt')
      .replace(/^sky(?:-sports?)?$/, 'sky')
      .replace(/^bein(?:-sports)?$/, 'bein-sports')
      .replace(/^main(?:-event)?$/, 'main-event')
      .replace(/^stan(?:-sport)?$/, 'stan');
    abs = tryKey(`${brand}-${mBrand[2].toLowerCase()}`);
    if (abs) return abs;
  }

  return null;
}

// broad longest-prefix (use as a last resort only)
function posterFromMapPrefix(k) {
  try {
    const keys = Object.keys(POSTER_MAP || {});
    let best = null;
    for (const key of keys) {
      if (k.startsWith(key) && (!best || key.length > best.length)) best = key;
    }
    return best ? mapUrl(best) : null;
  } catch {
    return null;
  }
}

function extrasGroupPoster(slug) {
  return mapUrl(`ex-${slug}`) || mapUrl(`extras-${slug}`) || mapUrl('extras-generic');
}
function sportsGenericPoster() {
  return mapUrl('sports-generic') || mapUrl('extras-generic');
}

/**
 * ALWAYS use your /images repo. Never use M3U or i.mjh.nz logos.
 */
function posterAny(regionOrKey, ch) {
  // 1) exact / hash-collapsed map override by id (or alias)
  let mapped = posterFromMapExact(ch.id) || posterFromAlias(ch.id) || posterFromMapExact(ch.posterId) || posterFromAlias(ch.posterId);

  // F1 special hash mapping
  if (!mapped && /(?:^|\b)f1\b/i.test(ch.name || '') && /-([a-z0-9]{5,9})$/i.test(ch.id)) {
    const h = ch.id.match(/-([a-z0-9]{5,9})$/i)[1].toLowerCase();
    mapped = posterFromMapExact(`f1-${h}`) || posterFromMapPrefix(`f1-${h}`);
  }

  // 2) alias by stable channel-name (handles "Fox Cricket", "ESPN 1 (unstable)" etc.)
  if (!mapped) mapped = posterFromAlias(ch.name);

  // 3) match by stable channel-name slug
  if (!mapped) {
    const nameKey = _slug(ch.name).replace(/-\d+$/,'');
    mapped = posterFromMapExact(nameKey) || posterFromMapPrefix(nameKey);
  }

  // 4) pack/region fallbacks (still from your repo)
  if (!mapped) {
    if (String(regionOrKey || '').startsWith('EX')) {
      const slug = String(regionOrKey).split(':')[1] || '';
      mapped = extrasGroupPoster(slug) || mapUrl('extras-generic');
    } else if (String(regionOrKey || '').startsWith('SP')) {
      mapped = sportsGenericPoster() || mapUrl('extras-generic');
    } else {
      mapped = mapUrl('extras-generic');
    }
  }
  return mapped || '';
}

// Intentionally disabled — we always use posters from /images
function m3uLogoAny(_regionOrKey, _ch) {
  return '';
}

/* --------------------- AU/NZ sources ----------------------- */
const base = (region) => `https://i.mjh.nz/au/${encodeURIComponent(region)}`;
const m3uUrl       = (region) => `${base(region)}/raw-tv.m3u8`;
const radioM3uUrl  = (region) => `${base(region)}/raw-radio.m3u8`;
const epgUrl       = (region) => `${base(region)}/epg.xml`;

const baseNZ        = () => `https://i.mjh.nz/nz`;
const m3uUrlNZ      = () => `${baseNZ()}/raw-tv.m3u8`;
const radioM3uUrlNZ = () => `${baseNZ()}/raw-radio.m3u8`;
const epgUrlNZ      = () => `${baseNZ()}/epg.xml`;

/* ---------------- Curated (A1X) helpers -------------------- */
function normLower(s='') { return String(s||'').toLowerCase(); }
function isUHD(name = '', url = '') {
  const n = normLower(name), u = normLower(url);
  return /\b(uhd|4k|2160p?)\b/.test(n) || /(2160|uhd|4k|hevc|main10|h\.?265)/.test(u);
}
function isFHD(name = '', url = '') { const s = normLower(name + ' ' + url); return /\b(fhd|1080p?)\b/.test(s) || /(^|[^0-9])1080([^0-9]|$)/.test(s); }
function isHD (name = '', url = '') { const s = normLower(name + ' ' + url); return /\bhd\b/.test(s) || /\b720p?\b/.test(s) || /(^|[^0-9])720([^0-9]|$)/.test(s); }
function qualityLabel(name='',url=''){ if (isUHD(name,url)) return 'UHD / 4K'; if (isFHD(name,url)) return 'FHD / 1080p'; if (isHD(name,url)) return 'HD / 720p'; return 'SD'; }
function qualityRank(label=''){ const l=String(label).toUpperCase(); if (l.includes('UHD')) return 3; if (l.includes('FHD')) return 2; if (l.includes('HD')) return 1; return 0; }
function baseNameClean(name=''){ return String(name).replace(/^\s*(?:UKI?\s*\|\s*|\[[^\]]+\]\s*)/i,'').replace(/\b(UHD|4K|2160p?|FHD|1080p|HD|720p|SD)\b/ig,'').replace(/\s{2,}/g,' ').trim(); }

const tvMatcher = (...labels) =>
  new RegExp(`^\\s*(?:${labels.join('|')})\\s*(?:TV\\s*Channels?|Channels?)\\s*$`, 'i');
const sportsMatcher = (...labels) =>
  new RegExp(`^\\s*(?:${labels.join('|')})\\s*Sports?(?:\\s*Channels?)?\\s*$`, 'i');

const A1X_GROUPS = {
  epl:              /^EPL$/i,
  uk_sports:        sportsMatcher('UK','United\\s*Kingdom'),
  us_sports:        sportsMatcher('US','USA','United\\s*States'),
  ca_sports:        sportsMatcher('CA','Canada'),
  au_sports:        sportsMatcher('AU','Australia'),
  nz_sports:        sportsMatcher('NZ','New\\s*Zealand'),
  eu_sports:        sportsMatcher('EU','Europe','European'),
  world_sports:     sportsMatcher('World','International'),

  uk_tv:            tvMatcher('UK','United\\s*Kingdom'),
  us_tv:            tvMatcher('US','USA','United\\s*States'),
  ca_tv:            tvMatcher('CA','Canada'),
};

const EXTRAS_M3U_URL = process.env.EXTRAS_M3U_URL ||
  'https://gist.githubusercontent.com/One800burner/dae77ddddc1b83d3a4d7b34d2bd96a5e/raw/1roguevip.m3u';

// Curated (A1X)
const A1X_CURATED_PRIMARY = 'https://bit.ly/a1xstream';
const A1X_CURATED_BACKUP = 'https://a1xs.vip/a1xstream';
// FIX: correct GitHub raw URL (refs/heads does not work on raw.githubusercontent)
const A1X_CURATED_DIRECT = 'https://raw.githubusercontent.com/a1xmedia/m3u/main/a1x.m3u';


async function fetchCuratedM3U(forceFresh=false) {
  if (!forceFresh && cache.a1x_text && fresh(cache.a1x_text, CACHE_TTL)) return cache.a1x_text.text;
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36 AUIPTV-Addon',
    'Accept': '*/*'
  };
  let text = '';
  const sources = [A1X_CURATED_PRIMARY, A1X_CURATED_BACKUP, A1X_CURATED_DIRECT];
  for (const source of sources) {
    try {
      const r = await fetch(source, { redirect: 'follow', headers });
      if (r.ok) { text = await r.text(); if (text && text.length > 100) break; }
    } catch {}
  }
  cache.a1x_text = { ts: Date.now(), text: String(text || '') };
  return String(text || '');
}
async function fetchCuratedEntries(forceFresh=false) {
  if (!forceFresh && cache.a1x_entries && fresh(cache.a1x_entries, CACHE_TTL)) return cache.a1x_entries.entries;
  const text = await fetchCuratedM3U(forceFresh);
  const entries = parseM3UEntries(text);
  cache.a1x_entries = { ts: Date.now(), entries };
  return entries;
}
async function getCuratedGroup(key, { forceFresh = false } = {}) {
  const ck = `curated:${key}`;
  const c = cache.curated_groups.get(ck);
  if (!forceFresh && c && fresh(c, CACHE_TTL)) return c.channels;

  const matcher = A1X_GROUPS[key];
  let channels = new Map();

  try {
    const entries = await fetchCuratedEntries(forceFresh);

    const grouped = new Map();
    for (const e of entries) {
      const grp = String(e.group || '').trim();
      if (!matcher || !matcher.test(grp)) continue;

      const base = baseNameClean(e.name);
      const label = qualityLabel(e.name, e.url);
      const rank = qualityRank(label);

      if (!grouped.has(base)) grouped.set(base, { id: e.id || base, name: base, logo: e.logo || null, variants: [] });
      const g = grouped.get(base);

      const existingIdx = g.variants.findIndex(v => v.label === label);
      const preferThis = /a1xs\.vip/i.test(e.url);
      if (existingIdx >= 0) {
        const existing = g.variants[existingIdx];
        if (preferThis && !/a1xs\.vip/i.test(existing.url)) g.variants[existingIdx] = { label, url: e.url, rank };
      } else {
        g.variants.push({ label, url: e.url, rank });
      }
      if (!g.logo && e.logo) g.logo = e.logo;
    }

    channels = new Map();
    for (const [_, obj] of grouped) {
      obj.variants.sort((a,b) => b.rank - a.rank);
      obj.url = obj.variants[0]?.url || null;
      const id = obj.id || obj.name;
      channels.set(id, { id, name: obj.name, logo: obj.logo, url: obj.url, variants: obj.variants });
    }
  } catch (_) {}

  cache.curated_groups.set(ck, { ts: Date.now(), channels });
  return channels;
}

/* ---------------------- EXTRAS (Dynamic) ------------------- */
const slugify = (s='') =>
  String(s).toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'').slice(0,48) || 'pack';

function hash32(s='') { // stable-ish short id
  let h = 0; for (let i=0;i<s.length;i++) { h = (h<<5) - h + s.charCodeAt(i); h |= 0; }
  return (h>>>0).toString(36);
}

async function fetchExtrasM3U(forceFresh = false) {
  if (!forceFresh && cache.extras_text && fresh(cache.extras_text, SHORT_TTL)) return cache.extras_text.text;
  try {
    const r = await fetch(EXTRAS_M3U_URL, { redirect: 'follow' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const t = await r.text();
    cache.extras_text = { ts: Date.now(), text: t || '' };
    return t || '';
  } catch (e) {
    console.error('[fetchExtrasM3U] Failed to fetch M3U:', e.message, 'URL:', EXTRAS_M3U_URL);
    cache.extras_text = { ts: Date.now(), text: '' };
    return '';
  }
}

// IMPORTANT: group by name (slug) and merge duplicates as variants
async function getExtrasGroups({ forceFresh = false } = {}) {
  if (!forceFresh && cache.extras_groups && fresh(cache.extras_groups, SHORT_TTL))
    return cache.extras_groups.groups;

  const text = await fetchExtrasM3U(forceFresh).catch(e => {
    console.error('[getExtrasGroups] Fetch failed:', e);
    return '';
  });

  const entries = parseM3UEntries(text);

  const map = new Map(); // slug -> { slug, name, channels: Map(nameSlug -> channelObj) }
  for (const e of entries) {
    const gname = (e.group || 'Other').trim();
    const gslug = slugify(gname);
    if (!map.has(gslug)) map.set(gslug, { slug: gslug, name: gname, channels: new Map() });

    const nameSlug = slugify(e.name) || slugify(e.id);
    const label = qualityLabel(e.name, e.url);
    const rank  = qualityRank(label);

    const chMap = map.get(gslug).channels;
    const existing = chMap.get(nameSlug);
    if (!existing) {
      const stableId = `${nameSlug}-${hash32(e.url)}`;
      chMap.set(nameSlug, {
        id: stableId,
        posterId: e.id || null,
        name: e.name,
        logo: e.logo || null,
        url: e.url,
        variants: [{ label, url: e.url, rank }]
      });
    } else {
      if (!existing.posterId && e.id) existing.posterId = e.id;
      if (!existing.variants.some(v => v.url === e.url)) {
        existing.variants.push({ label, url: e.url, rank });
        existing.variants.sort((a,b)=> b.rank - a.rank);
        existing.url = existing.variants[0].url;
      }
    }
  }

  cache.extras_groups = { ts: Date.now(), groups: map };
  return map;
}

/* ----------------------- AU/NZ fetchers -------------------- */
async function getChannels(region, kind = 'tv') {
  if (kind === 'radio') {
    const key = `${region}:radio_m3u`;
    const c = cache.radioM3u.get(key);
    if (c && fresh(c)) return c.channels;

    let channels = new Map();
    try { const text = await (await fetch(radioM3uUrl(region))).text(); channels = parseM3U(text); } catch {}
    cache.radioM3u.set(key, { ts: Date.now(), channels });
    return channels;
  }

  const key = `${region}:m3u`;
  const c = cache.m3u.get(key);
  if (c && fresh(c)) return c.channels;
  const text = await (await fetch(m3uUrl(region))).text();
  const channels = parseM3U(text);
  cache.m3u.set(key, { ts: Date.now(), channels });
  return channels;
}
async function getEPG(region) {
  const key = `${region}:epg`;
  const c = cache.epg.get(key);
  if (c && fresh(c)) return c.map;
  const xml = await (await fetch(epgUrl(region))).text();
  const map = await parseEPG(xml);
  cache.epg.set(key, { ts: Date.now(), map });
  return map;
}
// NZ
async function getNZChannels(kind='tv') {
  if (kind === 'radio') {
    const c = cache.nz_radio_m3u.get('nz');
    if (c && fresh(c)) return c.channels;
    let channels = new Map();
    try { const text = await (await fetch(radioM3uUrlNZ())).text(); channels = parseM3U(text); } catch {}
    cache.nz_radio_m3u.set('nz', { ts: Date.now(), channels }); return channels;
  }
  const c = cache.nz_tv_m3u.get('nz');
  if (c && fresh(c)) return c.channels;
  let channels = new Map();
  try { const text = await (await fetch(m3uUrlNZ())).text(); channels = parseM3U(text); } catch {}
  cache.nz_tv_m3u.set('nz', { ts: Date.now(), channels }); return channels;
}
async function getNZEPG() {
  const c = cache.nz_epg.get('nz');
  if (c && fresh(c)) return c.map;
  const xml = await (await fetch(epgUrlNZ())).text();
  const map = await parseEPG(xml);
  cache.nz_epg.set('nz', { ts: Date.now(), map });
  return map;
}

// ------- DAILY EPG CACHE (memory) -------
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const EPGLONG_TTL_MS = Number(process.env.EPGLONG_TTL_MS || ONE_DAY_MS);
const DAILY_XML_CACHE = new Map(); // url -> { ts, parsed }

async function fetchXmlDaily(url, { timeout = 60000 } = {}) {
  const hit = DAILY_XML_CACHE.get(url);
  if (hit && (Date.now() - hit.ts) < EPGLONG_TTL_MS && hit.parsed) return hit.parsed;

  let lastErr;
  for (let i = 0; i < 2; i++) {
    try {
      const resp = await fetchWithTimeout(url, { redirect: 'follow', headers: DEFAULT_FETCH_HEADERS }, timeout);
      if (!resp.ok || !resp.body) throw new Error(`HTTP ${resp.status}`);

      let inStream = toNodeStream(resp.body);

      // If it *looks* gzipped and fetch hasn't already decompressed, gunzip it.
      const looksGz = isGzipLike(url, resp.headers);
      const enc = (resp.headers.get('content-encoding') || '').toLowerCase();
      const alreadyDecoded = enc.includes('gzip') || enc.includes('x-gzip');

      if (looksGz && !alreadyDecoded) {
        const gunzip = zlib.createGunzip();
        inStream = inStream.pipe(gunzip);
      }

      const parsed = await parseXmltvStream(inStream);

      // Sort programmes per channel by start time once
      for (const [cid, arr] of parsed.programmes) {
        arr.sort((a, b) => String(a.start).localeCompare(String(b.start)));
      }

      DAILY_XML_CACHE.set(url, { ts: Date.now(), parsed });
      return parsed;
    } catch (e) {
      lastErr = e;
      await new Promise(r => setTimeout(r, 400));
    }
  }
  throw lastErr || new Error('epg fetch failed');
}


// getRogueEPGIndex with this (daily cached + merged) - Now supports US/UK/SPORTS


async function getRogueEPGIndex(region = 'ALL') {
  const key = `rogue:${region}`;
  const c = cache.rogue_epg_idx.get(key);
  if (c && fresh(c, EPGLONG_TTL_MS)) {
    console.log(`[getRogueEPGIndex] Using cached ${region}, channels: ${c.programmes.size}`);
    return c;
  }

  console.log(`[getRogueEPGIndex] Building fresh index for ${region}...`);
  const list = region === 'ALL'
    ? [...ROGUE_EPG_URLS.AU, ...ROGUE_EPG_URLS.NZ, ...ROGUE_EPG_URLS.UK, ...ROGUE_EPG_URLS.US, ...ROGUE_EPG_URLS.SPORTS]
    : (ROGUE_EPG_URLS[region] || []);

  const merged = { programmes: new Map(), nameIndex: new Map() };
  let successCount = 0;

  // PARALLEL: Fetch/parse all shards concurrently
  const parsedPromises = list.map(async (u) => {
    try {
      console.log(`[getRogueEPGIndex] Streaming ${u}...`);
      return await fetchXmlDaily(u, { timeout: 90000 });  // Per-shard timeout still 90s
    } catch (e) {
      console.error('[getRogueEPGIndex] failed', u, e.message);
      return null;  // Skip failed shards
    }
  });

  const parsedList = await Promise.all(parsedPromises);

  for (const parsed of parsedList) {
    if (!parsed) continue;  // Skip failures
    successCount++;

    // Merge programmes
    for (const [cid, arr] of parsed.programmes) {
      const cur = merged.programmes.get(cid) || [];
      merged.programmes.set(cid, cur.length ? cur.concat(arr) : arr.slice());
    }

    // Merge name index
    for (const [k, v] of parsed.nameIndex) {
      if (!merged.nameIndex.has(k)) merged.nameIndex.set(k, v);
    }
  }

  console.log(`[getRogueEPGIndex] ${region} complete: ${successCount}/${list.length} sources, ${merged.programmes.size} channels, ${merged.nameIndex.size} name mappings`);

  // Trimming logic unchanged (already efficient)
  if (['US', 'UK', 'SPORTS'].includes(region)) {
    // ... (your existing trimming code)
  }

  const out = { ts: Date.now(), ...merged };
  cache.rogue_epg_idx.set(key, out);
  return out;
}


const ROGUE_BUILD_PROMISES = new Map();

async function getOrBuildRogueIndex(regionKey, maxWaitMs = 60000) { // Changed from 8000 to 15000
  const k = `rogue:${regionKey}`;
  const hit = cache.rogue_epg_idx.get(k);
  if (hit && fresh(hit, EPGLONG_TTL_MS)) return hit;

  let p = ROGUE_BUILD_PROMISES.get(k);
  if (!p) {
    p = getRogueEPGIndex(regionKey).finally(() => ROGUE_BUILD_PROMISES.delete(k));
    ROGUE_BUILD_PROMISES.set(k, p);
  }
  
  try {
    return await Promise.race([
      p,
      new Promise((_, rej) => setTimeout(() => rej(new Error('soft-timeout')), maxWaitMs))
    ]);
  } catch (e) {
    console.error(`[getOrBuildRogueIndex] Timeout/error for ${regionKey} after ${maxWaitMs}ms:`, e.message);
    return null;
  }
}


// --- Rogue EPG Preload & Region Helper ---


// Boot-time preload (fire and forget) - Commented out to avoid cold start issues on Lambda
// (async () => {
// for (const region of Object.keys(ROGUE_EPG_URLS)) {
// getRogueEPGIndex(region).catch(e => console.warn('[rogue preload]', region, e.message));
// }
// })();


// Scope index load by curated key instead of forcing ALL
async function rogueIndexForCatalog(curatedKeyOrSlug = '') {
  const key = String(curatedKeyOrSlug).toLowerCase();
  if (key.includes('uk')) return getRogueEPGIndex('UK');
  if (key.includes('us')) return getRogueEPGIndex('US');
  if (key.includes('sport') || key.includes('epl')) return getRogueEPGIndex('SPORTS');
  if (key.includes('nz')) return getRogueEPGIndex('NZ');
  return getRogueEPGIndex('AU');
}
// Optional manual refresh route (hit in browser if needed)
app.get('/debug/epg/refresh', (_req, res) => {
  DAILY_XML_CACHE.clear();
  cache.rogue_epg_idx.clear();
  res.json({ ok: true, cleared: true });
});

function normalizeId(s='') { return String(s).toLowerCase().replace(/[^a-z0-9]+/g,''); }
function resolveEpgChannelId({ cid, name, idx, aliases = EPG_ALIASES }) {
  // 1) direct/alias hits
  const lowerStrict = normalizeId(cid);
  if (aliases && aliases[lowerStrict]) return aliases[lowerStrict];
  if (idx.programmes.has(cid)) return cid;

  // 2) try strict id + loose id
  const byIdStrict = idx.nameIndex.get(lowerStrict);
  if (byIdStrict) return byIdStrict;
  const byIdLoose = idx.nameIndex.get(normalizeIdLoose(cid));
  if (byIdLoose) return byIdLoose;

  // 3) try name (strict then loose)
  const byNameStrict = idx.nameIndex.get(normalizeId(name));
  if (byNameStrict) return byNameStrict;
  const byNameLoose = idx.nameIndex.get(normalizeIdLoose(name));
  if (byNameLoose) return byNameLoose;

  // 4) progressive collapse of cid parts (and a loose version)
  const parts = String(cid).split(/[._-]/g).filter(Boolean);
  for (let i = parts.length; i > 0; i--) {
    const joined = parts.slice(0, i).join(' ');
    const probeStrict = normalizeId(joined);
    const hitStrict = idx.nameIndex.get(probeStrict);
    if (hitStrict) return hitStrict;
    const probeLoose = normalizeIdLoose(joined);
    const hitLoose = idx.nameIndex.get(probeLoose);
    if (hitLoose) return hitLoose;
  }

  // 5) final attempt: drop simple region tags from the name
  const alt = cleanNameForEpg(name).replace(/\b(uk|us|usa|nz|au|ca|eu)\b/ig, '').trim();
  if (alt && alt !== name) {
    const hit = idx.nameIndex.get(normalizeIdLoose(alt));
    if (hit) return hit;
  }
  return null;
}
// Clean noisy names like "Sky Sport 1 [BU] (HD)"
function cleanNameForEpg(name = '') {
  return String(name)
    .replace(/\[[^\]]*\]/g, ' ')       // [BU], [HD], etc
    .replace(/\([^)]*\)/g, ' ')         // (unstable), (US), etc
    .replace(/\b(UHD|4K|2160p|FHD|1080p|HD|720p|SD)\b/ig, ' ')
    .replace(/\b(backup|alt|feed|test|unstable)\b/ig, ' ')
    .replace(/\s{2,}/g, ' ')            // collapse spaces
    .trim();
}

function normalizeIdLoose(s = '') {
  return cleanNameForEpg(s).toLowerCase().replace(/[^a-z0-9]+/g, '');
}

/* ---------------------- Manifest helpers ------------------ */
function genreCity(selected) {
  const s = String(selected||'').toLowerCase().replace(/[^a-z0-9+ ]+/gi,' ').trim();
  const m = s.match(/^(adelaide|brisbane|canberra|darwin|hobart|melbourne|perth|sydney)\s*tv$/);
  return m ? m[1][0].toUpperCase() + m[1].slice(1) : null;
}
const genreIs = (selected, ...opts) => opts.some(o => String(selected||'').toLowerCase().trim() === String(o).toLowerCase());

function buildHeaders(url) {
  let origin = 'https://www.google.com';
  let referer = 'https://www.google.com/';
  try {
    const uo = new URL(url);
    origin = `${uo.protocol}//${uo.hostname}`;
    referer = `${origin}/`;
  } catch {}
  return {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
    'Referer': referer,
    'Origin': origin,
    'Accept': '*/*',
    'Accept-Language': 'en-AU,en;q=0.9'
  };
}
/* ---------------------- Manifest (v3) --------------------- */
function buildManifestV3(selectedRegion, genreOptions) {
  return {
    id: 'com.joshargh.auiptv',
    version: '2.8.1',
    name: `AU IPTV (${selectedRegion})`,
    description: 'Australian + NZ live streams with optional international TV, Sports and Additional Packs.',
    types: ['tv'],
    catalogs: [{
      type: 'tv',
      id: `au_tv_${selectedRegion}`,
      name: `AU IPTV - ${selectedRegion}`,
      extra: [ { name: 'search' }, { name: 'genre', options: genreOptions, isRequired: false } ]
    }],
    resources: ['catalog','meta','stream']
  };
}

/* ---------------------- Addon Builder --------------------- */
const builder = new addonBuilder(buildManifestV3(DEFAULT_REGION, [
  'Traditional Channels','Other Channels','All TV Channels','Regional Channels','Radio'
]));



/* ---------------------- Catalog Handler ------------------- */
builder.defineCatalogHandler(async ({ type, id, extra }) => {
  if (type !== 'tv') return { metas: [] };
  const m = id.match(/^au_tv_(.+)$/);
  const region = validRegion(m ? m[1] : DEFAULT_REGION);

  const selectedGenre = String(extra?.genre || 'Traditional Channels');
  let contentRegion = region;
  let contentKind = 'tv';
  let catalogType = 'traditional';
  let isNZ = false;
  let isCurated = false; let curatedKey = null;
  let isExtras = false; let extrasSlug = null;

  if (genreIs(selectedGenre, 'traditional channels','traditional')) catalogType = 'traditional';
  else if (genreIs(selectedGenre, 'other channels','other')) catalogType = 'other';
  else if (genreIs(selectedGenre, 'all tv channels','all tv','all')) catalogType = 'all';
  else if (genreIs(selectedGenre, 'regional channels','regional')) catalogType = 'regional';
  else if (genreIs(selectedGenre, 'radio')) contentKind = 'radio';
  else if (genreIs(selectedGenre, 'nz tv','nz')) { isNZ = true; contentKind = 'tv'; catalogType = 'all'; }
  else if (genreIs(selectedGenre, 'nz radio')) { isNZ = true; contentKind = 'radio'; }
  else if (/^extra:\s*/i.test(selectedGenre)) { isExtras = true; extrasSlug = slugify(selectedGenre.replace(/^extra:\s*/i,'')); }
  else {
    const lower = selectedGenre.toLowerCase();
    const keyMap = {
      'uk tv':'uk_tv','uk channels':'uk_tv',
      'uk sports':'uk_sports','uk sport':'uk_sports',
      'us tv':'us_tv','usa tv':'us_tv','us channels':'us_tv','usa channels':'us_tv',
      'us sports':'us_sports','usa sports':'us_sports',
      'ca tv':'ca_tv','canada tv':'ca_tv','ca channels':'ca_tv',
      'ca sports':'ca_sports','canada sports':'ca_sports',
      'au sports':'au_sports','nz sports':'nz_sports',
      'eu sports':'eu_sports','world sports':'world_sports','epl':'epl'
    };
    curatedKey = keyMap[lower] || null;
    isCurated = !!curatedKey;
    if (!isCurated) {
      const cityName = genreCity(selectedGenre);
      if (cityName) { contentRegion = cityName; catalogType = 'all'; }
    }
  }

  const genreOptions = ['Traditional Channels','Other Channels','All TV Channels','Regional Channels','Radio',
                        'NZ TV','NZ Radio',
                        'UK TV','UK Sports','US TV','US Sports','CA TV','CA Sports','AU Sports','NZ Sports','EU Sports','World Sports','EPL'];
  try {
    const groups = await getExtrasGroups({ forceFresh: false });
    for (const [, g] of groups) genreOptions.push(`Extra: ${g.name}`);
  } catch {}

  builder.manifest = buildManifestV3(region, genreOptions);

  // ---------------- EPG: preload rogue indexes (US/UK/Sports focus) -----------------
  // We only load these (cheap, cached) indexes if we are actually showing
  // curated or extras catalogs. AU/NZ keep using the regional EPG.
  // ---------------- EPG: preload rogue indexes (best-effort) -----------------
  let rogueIdx = null;
  if (contentKind === 'tv' && (isCurated || isExtras)) {
    const scope = (curatedKey || extrasSlug || '').toLowerCase();
    const regionKey = scopeToRegionKey(scope);
    rogueIdx = cache.rogue_epg_idx.get(`rogue:${regionKey}`);
    if (!rogueIdx) {
      // Fire in background, do not await
      getOrBuildRogueIndex(regionKey, 60000).catch(e => console.error('Background EPG load failed:', e));
    }
  }


  let channels;
  if (isNZ) channels = await getNZChannels(contentKind);
  else if (isCurated) channels = await getCuratedGroup(curatedKey);
  else if (isExtras) {
    const groups = await getExtrasGroups({ forceFresh: false });
    const g = groups.get(extrasSlug);
    channels = g ? g.channels : new Map();
  } else channels = await getChannels(contentRegion, contentKind);

  const tz = isNZ ? 'Pacific/Auckland' : (isCurated ? 'UTC' : (REGION_TZ[contentRegion] || 'Australia/Sydney'));
  const epg = (contentKind === 'tv')
    ? (isNZ ? await getNZEPG() : (isCurated || isExtras ? new Map() : await getEPG(contentRegion)))
    : new Map();

  // Helper to pull EPG list for curated/extras, using Rogue (US/UK/Sports)
  const epgFromIndexes = (ch) => {
    if (!rogueIdx) return [];
    const id = resolveEpgChannelId({ cid: ch.id, name: ch.name, idx: rogueIdx });
    if (id && rogueIdx.programmes.has(id)) return rogueIdx.programmes.get(id) || [];
    return [];
  };

  const metas = [];
  for (const [cid, ch] of channels) {
    if (contentKind === 'tv') {
      let include = true; let sortVal;
      if (!isNZ && !isCurated && !isExtras) {
        const traditional = isTraditionalChannel(ch.name);
        const other = isOtherChannel(ch.name);
        const regional = false;
        if (catalogType === 'traditional') { include = traditional && !regional && !other; if (include) sortVal = auTradOrderValue(ch.name); }
        else if (catalogType === 'other') { include = (other || (!traditional && !regional)) && !regional; }
        else if (catalogType === 'regional') { include = regional; }
        else include = !regional;
      }
      if (!include) continue;

      const list = (isCurated || isExtras) ? epgFromIndexes(ch) : (epg.get(cid) || []);
      const nowp = nowProgramme(list);
      const release = nowp
        ? `${fmtLocal(nowp.start, tz)} | ${nowp.title}`
        : (list[0]
            ? `${fmtLocal(list[0].start, tz)} | ${list[0].title}`
            : ((isCurated || isExtras)
                ? (rogueIdx ? 'Curated TV' : 'EPG warming up…')
                : (isNZ ? 'Live NZ TV' : 'Live AU TV')));


      const regionKey = isCurated ? `SP:${curatedKey}` : (isExtras ? `EX:${extrasSlug}` : (isNZ ? 'NZ' : contentRegion));
      const poster = posterAny(regionKey, ch);

      metas.push({
        id: isCurated ? `au|SP:${curatedKey}|${cid}|tv`
            : isExtras ? `au|EX:${extrasSlug}|${cid}|tv`
            : `au|${isNZ?'NZ':contentRegion}|${cid}|tv`,
        type: 'tv',
        name: ch.name,
        poster,
        logo: poster,
        posterShape: 'square',
        description: isNZ ? 'New Zealand TV' : (isCurated ? 'Curated' : (isExtras ? 'Additional Pack' : 'Live AU TV')),
        releaseInfo: release,
        _sortOrder: (catalogType === 'traditional' && !isCurated && !isExtras && !isNZ) ? (sortVal ?? 10000) : undefined
      });
    } else {
      const regionKey = isNZ ? 'NZ' : contentRegion;
      const poster = posterAny(regionKey, ch);
      metas.push({
        id: `au|${regionKey}|${cid}|radio`,
        type: 'tv',
        name: ch.name,
        poster,
        logo: poster,
        posterShape: 'square',
        description: isNZ ? 'New Zealand Radio' : 'Live AU Radio',
        releaseInfo: isNZ ? 'Live NZ Radio' : 'Live Radio'
      });
    }
  }

  if (!isCurated && !isExtras && catalogType === 'traditional')
    metas.sort((a,b)=>(a._sortOrder-b._sortOrder)||a.name.localeCompare(b.name));
  else metas.sort((a,b)=>a.name.localeCompare(b.name));
  metas.forEach(m=>delete m._sortOrder);

  return { metas };
});

/* ------------------------ Meta Handler -------------------- */
function parseItemId(id) {
  const p = String(id||'').split('|');
  if (p.length < 3 || p[0] !== 'au') return null;
  const [, regionRaw, cid, kindRaw = 'tv'] = p;
  if (String(regionRaw).startsWith('SP:')) return { region: 'SP', curatedKey: regionRaw.split(':')[1], cid, kind: kindRaw };
  if (String(regionRaw).startsWith('EX:')) return { region: 'EX', extrasSlug: regionRaw.split(':')[1], cid, kind: kindRaw };
  const region = (regionRaw === 'NZ') ? 'NZ' : validRegion(regionRaw);
  return { region, cid, kind: kindRaw };
}

builder.defineMetaHandler(async ({ type, id }) => {
  if (type !== 'tv') return { meta: {} };

  const parsed = parseItemId(id);
  if (!parsed) return { meta: {} };

  const { region, cid, kind } = parsed;

  // ----- load channel -----
  let ch, tz = 'Australia/Sydney';
  if (region === 'NZ') {
    tz = 'Pacific/Auckland';
    ch = (await getNZChannels(kind)).get(cid);
  } else if (region === 'SP') {
    tz = 'UTC';
    ch = (await getCuratedGroup(parsed.curatedKey)).get(cid);
  } else if (region === 'EX') {
    tz = 'UTC';
    const g = (await getExtrasGroups({ forceFresh: false })).get(parsed.extrasSlug);
    ch = g?.channels.get(cid);
  } else {
    tz = REGION_TZ[region] || 'Australia/Sydney';
    ch = (await getChannels(region, kind)).get(cid);
  }
  if (!ch) return { meta: {} };

  const regionKey =
    region === 'SP' ? `SP:${parsed.curatedKey}` :
    region === 'EX' ? `EX:${parsed.extrasSlug}` :
    region;

  const squarePoster = posterAny(regionKey, ch);

  // ----- radio unchanged -----
  if (kind === 'radio') {
    return {
      meta: {
        id,
        type: 'tv',
        name: ch.name,
        poster: squarePoster,
        background: squarePoster,
        logo: squarePoster,
        posterShape: 'square',
        description: region === 'NZ' ? 'Live NZ radio streaming' : 'Live radio streaming',
        releaseInfo: region === 'NZ' ? 'Live NZ Radio' : 'Live Radio'
      }
    };
  }

  // ----- TV: build EPG line (no trailing label) -----
  let list = [];
  try {
    if (region === 'NZ') {
      const epg = await getNZEPG();
      list = epg.get(cid) || [];
    } else if (region === 'SP' || region === 'EX') {
      const scope = (parsed.curatedKey || parsed.extrasSlug || '').toLowerCase();
      const regionKeyForScope = scopeToRegionKey(scope);
      let idx = cache.rogue_epg_idx.get(`rogue:${regionKeyForScope}`);
      if (!idx) {
        // Fire in background, do not await
        getOrBuildRogueIndex(regionKeyForScope, 60000).catch(e => console.error('Background EPG load failed:', e));
      }
      if (idx) {
        const resolved = resolveEpgChannelId({ cid: ch.id, name: ch.name, idx });
        if (resolved && idx.programmes.has(resolved)) list = idx.programmes.get(resolved);
      }
      warmRogueIndex(scope);
    } else {
      const epg = await getEPG(region);
      list = epg.get(cid) || [];
    }
  } catch {}

  const nowp = nowProgramme(list);
  const epgLine = nowp
    ? `${fmtLocal(nowp.start, tz)} - ${fmtLocal(nowp.stop, tz)} | ${nowp.title}`
    : (list[0] ? `${fmtLocal(list[0].start, tz)} | ${list[0].title}` : '');

  return {
    meta: {
      id,
      type: 'tv',
      name: ch.name,              // keep channel name to avoid selection issues
      poster: squarePoster,
      background: squarePoster,
      logo: squarePoster,
      posterShape: 'square',
      description: epgLine || ch.name, // SUMMARY shows current/next EPG only
      releaseInfo: ch.name             // small subtitle = channel name
    }
  };
});


/* ------------------------- Stream Handler ----------------- */
function curatedRedirectUrl({ curatedKey, cid, label }) {
  const base = publicBase();
  const u = `${base}/redir/${encodeURIComponent(curatedKey)}/${encodeURIComponent(cid)}.m3u8`;
  return label ? `${u}?label=${encodeURIComponent(label)}` : u;
}
let PUBLIC_BASE = process.env.PUBLIC_BASE_URL || null;
app.use((req, _res, next) => { try {
  const b = baseUrl(req); if (b && (!PUBLIC_BASE || PUBLIC_BASE !== b)) PUBLIC_BASE = b;
} catch{} next(); });
const publicBase = () => PUBLIC_BASE || process.env.PUBLIC_BASE_URL || '';

builder.defineStreamHandler(async ({ type, id }) => {
  if (type !== 'tv') return { streams: [] };
  const parsed = parseItemId(id);
  if (!parsed) return { streams: [] };
  const { region, cid, kind } = parsed;

  // AU/NZ
  if (region !== 'SP' && region !== 'EX') {
    const channels = region === 'NZ' ? await getNZChannels(kind) : await getChannels(region, kind);
    const ch = channels.get(cid);
    if (!ch) return { streams: [] };
    const streamUrl = ch.url || '';
    const isHLS = /m3u8/i.test(streamUrl);
    if (!isHLS) return { streams: [{ name:'Direct Stream', url:streamUrl, behaviorHints:{ notWebReady:false } }] };
    return { streams: [{
      name:'HLS Stream', url:streamUrl, description:'Direct HLS stream',
      behaviorHints:{ notWebReady:false, bingeGroup:'hls-direct' }
    }] };
  }

  // Curated or Extras
  let ch = null;

  if (region === 'SP') {
    const force = /(sports|epl)/i.test(parsed.curatedKey);
    const group = await getCuratedGroup(parsed.curatedKey, { forceFresh: force });
    ch = group.get(cid);
  } else {
    const g = (await getExtrasGroups({ forceFresh: true })).get(parsed.extrasSlug);
    ch = g?.channels.get(cid);
  }
  if (!ch) return { streams: [] };

  const variants = Array.isArray(ch.variants) && ch.variants.length ? ch.variants : [{ label:'Play', url: ch.url }];

  const streams = [];
  const seen = new Set();
  for (const v of variants) {
    if (!v?.url) continue;
    const direct = String(v.url).trim();
    if (seen.has(direct)) continue;
    seen.add(direct);
    streams.push({
      url: direct,
      title: v.label || 'Stream',
      behaviorHints: { notWebReady: false, bingeGroup: region === 'SP' ? 'curated' : 'extras' },
      proxyHeaders: { request: buildHeaders(direct) }
    });
  }

  if (!streams.length && ch.url) {
    const key = region === 'SP' ? parsed.curatedKey : `ex:${parsed.extrasSlug}`;
    streams.push({ url: curatedRedirectUrl({ curatedKey: key, cid, label:'Play' }), title: 'Play (fallback)' });
  }
  return { streams };
});

/* --------------------- Redirect (tokens) ------------------ */
app.get('/redir/:curatedKey/:cid.m3u8', async (req, res) => {
  try {
    const { curatedKey, cid } = req.params;

    let target = null;
    if (curatedKey.startsWith('ex:')) {
      const slug = curatedKey.slice(3);
      const groups = await getExtrasGroups({ forceFresh: true });
      const g = groups.get(slug);
      target = g?.channels.get(cid)?.url || null;
    } else {
      const force = /(sports|epl)/i.test(curatedKey);
      const group = await getCuratedGroup(curatedKey, { forceFresh: force });
      target = group.get(cid)?.url || null;
    }

    if (!target) return res.status(404).send('Channel not found');

    res.set('Access-Control-Allow-Origin', '*');
    res.set('Cache-Control', 'no-store, max-age=0, must-revalidate');
    res.redirect(302, target);
  } catch (e) {
    res.status(502).send('Media redirect error');
  }
});

// ---------------- remainder of file (routes + export) is unchanged ----------------

// Pull helpers from xml2js we already required above
const { parseStringPromise, Builder } = xml2js;

// NOTE: avoid clobbering the earlier `norm()` used by quality detection.
function normKey(s) {
  return (s || '').toString().toLowerCase().replace(/[^a-z0-9]+/g, '');
}
function uniqBy(arr, keyFn) {
  const seen = new Set();
  return arr.filter(x => {
    const k = keyFn(x);
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

// Merge multiple XMLTVs: combine <channel> & <programme> and de-dupe by ids & start times.
async function mergeXmltvs(xmlStrings) {
  const docs = await Promise.all(xmlStrings.map(x => parseStringPromise(x, { explicitArray: true, mergeAttrs: true })));
  const out = { tv: { channel: [], programme: [] } };

  for (const d of docs) {
    const tv = d.tv || {};
    if (tv.channel)   out.tv.channel.push(...tv.channel);
    if (tv.programme) out.tv.programme.push(...tv.programme);
  }

  // De-dupe channels by id
  out.tv.channel = uniqBy(out.tv.channel, ch => (ch.$?.id || '').toString());

  // De-dupe programmes by (channel|start|title)
  out.tv.programme = uniqBy(out.tv.programme, pr => {
    const id = pr.$?.channel || '';
    const st = pr.$?.start || '';
    const t  = normKey((pr.title?.[0]?._) || (pr.title?.[0]) || '');
    return `${id}|${st}|${t}`;
  });

  // Rebuild XML
  const xml = new Builder({ headless: true }).buildObject(out);
  return xml;
}

async function fetchText(u) {
  const r = await fetch(u, { redirect: 'follow' });
  if (!r.ok) throw new Error(`EPG fetch failed ${r.status} for ${u}`);
  return await r.text();
}

// Figure out which feeds we need based on the URL flags the user selected on the landing page
function computeEpgFeedList(region, flags) {
  const urls = new Set();

  // Always include AU region unless explicitly disabled
  if (!flags.noau) urls.add(EPG_SOURCES.AU_REGION(region));

  // NZ toggles or Extras explicitly NZ
  if (flags.nz || flags.nzradio || flags.nzsports || flags.nzdefault || flags.extrasRegion === 'nz') {
    urls.add(EPG_SOURCES.NZ);
  }

  // Sports packs
  const anySports =
    flags.ausports || flags.uksports || flags.ussports || flags.casports ||
    flags.eusports || flags.worldsports || flags.epl || flags.extrasHasSports;

  if (anySports) {
    // Extras routing:
    if (flags.extrasRegion === 'au') {
      // Rogue KAYO Sports Extra: AU -> pull KAYO (+Foxtel for Fox/beIN/Main Event ids)
      urls.add(EPG_SOURCES.KAYO);
      urls.add(EPG_SOURCES.FOXTEL);
    } else if (flags.extrasRegion === 'nz') {
      // Extra: NZ | SKY sport -> NZ XMLTV only
      urls.add(EPG_SOURCES.NZ);
    } else if (flags.extrasRegion === 'uk') {
      // Extra: UK sport -> rely on Rogue/A1X mapping; don't add AU/NZ feeds here
      // (no action)
    } else {
      // Non-extras or unspecified region sports -> default AU sports (Kayo/Foxtel)
      urls.add(EPG_SOURCES.KAYO);
      urls.add(EPG_SOURCES.FOXTEL);
    }
  }

  return Array.from(urls);
}

// Parse selection flags from request path for the EPG merge pipeline (boolean fields expected)
function parseEpgFlagsFromPath(pathname) {
  const has = seg => pathname.includes(`/${seg}`);
  // detect extras group slug if present, e.g. /extras/exgrp-au-kayo-sports/...
  const parts = pathname.split('/').filter(Boolean);
  const exgrp = parts.find(p => /^exgrp-/i.test(p)) || '';
  const exgrpLower = exgrp.toLowerCase();

  let extrasRegion = null;
  if (/\bau\b/.test(exgrpLower) || /\bkayo\b/.test(exgrpLower)) extrasRegion = 'au';
  else if (/\bnz\b/.test(exgrpLower) || /\bsky\b/.test(exgrpLower)) extrasRegion = 'nz';
  else if (/\buk\b/.test(exgrpLower)) extrasRegion = 'uk';
  else if (/\bus\b/.test(exgrpLower)) extrasRegion = 'us';
  else if (/\bca\b/.test(exgrpLower) || /canad/.test(exgrpLower)) extrasRegion = 'ca';
  else if (/\beu\b/.test(exgrpLower) || /europe/.test(exgrpLower)) extrasRegion = 'eu';

  return {
    noau:        has('noau'),
    radio:       has('radio'),
    ausports:    has('ausports'),
    nz:          has('nz'),
    nzradio:     has('nzradio'),
    nzdefault:   has('nzdefault'),
    nzsports:    has('nzsports'),
    uktv:        has('uktv'),
    uksports:    has('uksports'),
    ustv:        has('ustv'),
    ussports:    has('ussports'),
    catv:        has('catv'),
    casports:    has('casports'),
    eusports:    has('eusports'),
    worldsports: has('worldsports'),
    epl:         has('epl'),
    extras:      has('extras'),
    extrasRegion,
    // include 'sky' too so "SKY sport" triggers sports handling
    extrasHasSports: /exgrp-.*(sport|kayo|fox|bein|mainevent|sky)/i.test(pathname),
  };
}

// Merge and cache final EPG XML for the given region+flags key
async function getMergedEpg(region, pathname) {
  const key = `epg:${region}:${pathname}`;
  const now = Date.now();
  const cached = EPG_CACHE.get(key);
  if (cached && now - cached.ts < EPG_CACHE_TTL_MS) return cached.xml;

  const flags = parseEpgFlagsFromPath(pathname);
  const feeds = computeEpgFeedList(region, flags);
  const xmls = await Promise.all(feeds.map(fetchText));
  const merged = await mergeXmltvs(xmls);

  EPG_CACHE.set(key, { xml: merged, ts: now });
  return merged;
}

/* ---------------------- EXPRESS ROUTES -------------------- */
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Cache-Control', 'no-store, max-age=0');
  next();
});

app.get('/healthz', (_req, res) => res.type('text/plain').send('ok'));
app.get('/stats', (_req, res) => res.json({ installs: _memStats.installs || 0 }));

// --- Debug: EPG status & matcher ---
app.get('/debug/epg/status', async (_req, res) => {
  try {
    const rg  = await getRogueEPGIndex('ALL').catch(() => null);
    res.json({
      rogue: rg ? { channels: rg.nameIndex.size, programmeChannels: rg.programmes.size } : null
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/debug/epg/resolve', async (req, res) => {
  try {
    const name = String(req.query.name || '');
    const cid  = String(req.query.cid || name);
    const out = { input: { name, cid } };
    const rg  = await getRogueEPGIndex('ALL').catch(() => null);

    if (rg) {
      const id = resolveEpgChannelId({ cid, name, idx: rg });
      out.rogue = {
        matched: id,
        sample: id ? (rg.programmes.get(id) || []).slice(0, 3) : []
      };
    }
    res.json(out);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// New route for full EPG debug
app.get('/epg/debug/all', async (_req, res) => {
  try {
    const rogue = await getRogueEPGIndex('ALL');
    res.json({
      programmeChannels: Array.from(rogue.programmes.keys()).sort(),
      nameIndex: Array.from(rogue.nameIndex.entries()).sort((a,b) => a[0].localeCompare(b[0]))
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// static assets (public/)
const PUBLIC_DIR = path.join(__dirname, 'public');
app.use(express.static(PUBLIC_DIR, { extensions: ['html'] }));

// logo & favicon
app.get(['/AUIPTVLOGO.svg','/favicon.svg'], (_req, res) => {
  const p = path.join(__dirname, 'AUIPTVLOGO.svg');
  if (fs.existsSync(p)) return res.type('image/svg+xml').sendFile(p);
  return res.status(404).end();
});
app.get('/favicon.ico', (_req, res) => res.redirect(302, '/AUIPTVLOGO.svg'));

// extras groups for UI
app.get('/extras/groups', async (_req, res) => {
  try {
    const groups = await getExtrasGroups({ forceFresh: false });
    const arr = [];
    for (const [, g] of groups) arr.push({ slug: g.slug, name: g.name, count: (g.channels?.size || 0) });
    res.json({ groups: arr });
  } catch (e) {
    console.error('[/extras/groups] Error:', e);
    res.status(500).json({ error: e?.message || 'failed' });
  }
});


// Helper base URL
function baseUrl(req) {
  const proto = req.headers['x-forwarded-proto'] || req.protocol || 'https';
  const host  = req.headers['x-forwarded-host']  || req.headers.host;
  return `${proto}://${host}`;
}
// Parse flags from request path for the *manifest* routes (returns { regionRaw, flags:Set })
function parseRequestFlagsFromPath(reqPath) {
  const parts = reqPath.split('/').filter(Boolean);
  const flags = new Set(parts.slice(1));
  if (flags.has('sports')) ['uksports','ussports','casports','ausports','nzsports','eusports','worldsports','epl'].forEach(f=>flags.add(f));
  return { regionRaw: decodeURIComponent(parts[0] || DEFAULT_REGION), flags };
}

/* ----------------------- Manifest endpoints ---------------- */
function buildGenresFromFlags(region, flags, extrasList = []) {
  const opts = ['Traditional Channels','Other Channels','All TV Channels','Regional Channels','Radio'];
  REGIONS.filter(r => r !== region).forEach(city => opts.push(`${city} TV`));
  opts.push('NZ TV','NZ Radio','UK TV','UK Sports','US TV','US Sports','CA TV','CA Sports','AU Sports','NZ Sports','EU Sports','World Sports','EPL');
  for (const g of extrasList) opts.push(`Extra: ${g.name}`);
  return opts;
}

// ---------------- FunAPI Proxy ----------------
app.get('/fun/:type', async (req, res) => {
  try {
    const type = req.params.type || 'compliment';
    const url = `https://my-fun-api.onrender.com/${encodeURIComponent(type)}`;

    const r = await fetch(url, { headers: { 'Accept': 'application/json' } });
    if (!r.ok) throw new Error(`Upstream error: ${r.status}`);

    const data = await r.json();
    res.set('Access-Control-Allow-Origin', '*'); // allow browser calls
    res.json(data);
  } catch (e) {
    console.error('[FunAPI proxy error]', e);
    res.status(500).json({ error: 'FunAPI proxy failed' });
  }
});


app.get(/^\/[^^/]+(?:\/[^/]+)*\/manifest\.json$/, async (req, res) => {
  try {
    markInstall();
    const { regionRaw, flags } = parseRequestFlagsFromPath(req.path);
    const region = validRegion(regionRaw);

    let extrasList = [];
    if (flags.has('extras')) {
      try {
        const groups = await getExtrasGroups({ forceFresh: false });
        extrasList = [...groups.values()].map(g => ({ slug: g.slug, name: g.name }));
      } catch {}
    }

    const man = buildManifestV3(region, buildGenresFromFlags(region, flags, extrasList));
    man.logo = man.icon = `${baseUrl(req)}/AUIPTVLOGO.svg`;
    if (STREMIO_ADDONS_CONFIG.signature) man.stremioAddonsConfig = STREMIO_ADDONS_CONFIG;

    res.json(man);
  } catch (e) {
    res.status(500).json({ error: e?.message || String(e) });
  }
});

app.get('/:region/manifest.json', (req, res) => {
  const region = validRegion(req.params.region);
  const man = buildManifestV3(region, buildGenresFromFlags(region, new Set(), []));
  man.logo = man.icon = `${baseUrl(req)}/AUIPTVLOGO.svg`;
  if (STREMIO_ADDONS_CONFIG.signature) man.stremioAddonsConfig = STREMIO_ADDONS_CONFIG;
  res.json(man);
});

app.get('/', (req, res) => {
  const idx = path.join(__dirname, 'public', 'index.html');
  if (fs.existsSync(idx)) return res.sendFile(idx);
  res.type('text/plain').send('UI not packaged. Place your index.html in /public.');
});

// Return merged XMLTV for the current selection (used by clients that want XML)
app.get(/^\/([^/]+)(?:\/.*)?\/epg\.xml$/, async (req, res) => {
  try {
    const region = decodeURIComponent(req.params[0] || 'Brisbane');
    const xml = await getMergedEpg(region, req.path);
    res.setHeader('Content-Type', 'application/xml; charset=utf-8');
    res.setHeader('Cache-Control', 'public, max-age=60'); // clients can re-use a minute
    res.send(xml);
  } catch (e) {
    res.status(500).send(`<!-- EPG error: ${e.message} -->`);
  }
});

// Optional: JSON view for debugging (handy when testing Rogue mapping)
app.get(/^\/([^/]+)(?:\/.*)?\/epg\.json$/, async (req, res) => {
  try {
    const region = decodeURIComponent(req.params[0] || 'Brisbane');
    const xml = await getMergedEpg(region, req.path);
    const obj = await parseStringPromise(xml, { explicitArray: true, mergeAttrs: true });
    res.json({ channels: (obj.tv?.channel || []).length, programmes: (obj.tv?.programme || []).length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ----------------------- SDK Router ----------------------- */
const sdkRouter = getRouter(builder.getInterface());
app.use((req, _res, next) => {
  const targets = ['/catalog/','/meta/','/stream/'];
  let idx = -1;
  for (const t of targets) { const i = req.url.indexOf(t); if (i >= 0) idx = (idx === -1 ? i : Math.min(idx, i)); }
  if (idx > 0) req.url = req.url.slice(idx);
  next();
});
app.use('/', sdkRouter);

// Quick visibility into what we indexed from Rogue
app.get('/epg/debug', async (_req, res) => {
  try {
    const rogue = await getRogueEPGIndex('ALL');
    const sample = (idx) => Array.from(idx.programmes.entries()).slice(0, 3)
      .map(([id, arr]) => ({ id, items: arr.length, first: arr[0]?.title }));
    res.json({
      rogue: { channels: rogue.nameIndex.size, programmeChannels: rogue.programmes.size, sample: sample(rogue) }
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Try to resolve a single channel against the indexes
app.get('/epg/resolve', async (req, res) => {
  const name = req.query.name || '';
  const cid  = req.query.cid  || '';
  try {
    const rogue = await getRogueEPGIndex('ALL');
    const tryIdx = (label, idx) => {
      const id = resolveEpgChannelId({ cid, name, idx });
      const programmes = id ? (idx.programmes.get(id) || []) : [];
      return { label, resolvedId: id, count: programmes.length, first: programmes[0]?.title };
    };
    res.json({ rogue: tryIdx('rogue', rogue) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ------------------- Export / Local run ------------------- */
// module.exports.handler = serverless(app);

if (require.main === module) {
  const PORT = process.env.PORT || 7000;
  app.listen(PORT, () => console.log('Listening on', PORT));
}

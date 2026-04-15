// NuVizz API proxy — plain .js CommonJS (matches working warehouse scanner pattern)
const https = require('https');
const { URL } = require('url');

const COMPANY = process.env.NUVIZZ_COMPANY || 'davis';
const USERNAME = process.env.NUVIZZ_USERNAME || process.env.NUVIZZ_USER || '';
const PASSWORD = process.env.NUVIZZ_PASSWORD || process.env.NUVIZZ_PASS || '';
const BASE_URL = process.env.NUVIZZ_BASE_URL || 'https://portal.nuvizz.com/deliverit/openapi/v7';

function request(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const req = https.request({
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: opts.method || 'GET',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        ...opts.headers,
      },
      timeout: 15000,
    }, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => resolve({ status: res.statusCode, body }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

function basicAuth() {
  return 'Basic ' + Buffer.from(`${USERNAME}:${PASSWORD}`).toString('base64');
}

async function nuvizzGet(path) {
  const url = `${BASE_URL}${path}`;
  const res = await request(url, {
    headers: { 'Authorization': basicAuth() },
  });
  return res;
}

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  // Parse the path: /api/nuvizz/stops, /api/nuvizz/stop/123, etc.
  const path = event.path.replace(/^\/.netlify\/functions\/nuvizz/, '').replace(/^\/api\/nuvizz/, '') || '/';
  const qs = event.queryStringParameters || {};

  try {
    // GET /diag or /status — diagnostic
    if (path === '/diag' || path === '/status' || path === '/' || path === '') {
      // Test auth by hitting the token endpoint
      let authResult = 'untested';
      try {
        const authRes = await nuvizzGet(`/auth/token/${encodeURIComponent(COMPANY)}`);
        authResult = authRes.status === 200 ? 'OK' : `HTTP ${authRes.status}: ${authRes.body.substring(0, 200)}`;
      } catch (e) {
        authResult = `Error: ${e.message}`;
      }
      return {
        statusCode: 200,
        headers: CORS,
        body: JSON.stringify({
          env: { NUVIZZ_COMPANY: COMPANY, NUVIZZ_USERNAME: USERNAME, BASE_URL },
          has_password: !!PASSWORD,
          auth: authResult,
        }),
      };
    }

    // GET /stops?fromDTTM=&toDTTM=
    if (path === '/stops') {
      const fromDTTM = qs.fromDTTM || '';
      const toDTTM = qs.toDTTM || '';
      const params = new URLSearchParams();
      if (fromDTTM) params.set('fromDTTM', fromDTTM);
      if (toDTTM) params.set('toDTTM', toDTTM);
      if (qs.stopType) params.set('stopType', qs.stopType);
      if (qs.zipCode) params.set('zipCode', qs.zipCode);

      const res = await nuvizzGet(`/stop/info/customer/${encodeURIComponent(COMPANY)}?${params}`);
      return { statusCode: res.status, headers: CORS, body: res.body };
    }

    // GET /stop/events?stopNbr=
    if (path === '/stop/events') {
      const q = qs.stopNbr ? `stopNbr=${qs.stopNbr}` : `stopId=${qs.stopId || ''}`;
      const res = await nuvizzGet(`/stop/eventinfo/${encodeURIComponent(COMPANY)}?${q}`);
      return { statusCode: res.status, headers: CORS, body: res.body };
    }

    // GET /stop/eta?stopNbr=
    if (path === '/stop/eta') {
      const q = qs.stopNbr ? `stopNbr=${qs.stopNbr}` : `stopId=${qs.stopId || ''}`;
      const res = await nuvizzGet(`/stop/etainfo/${encodeURIComponent(COMPANY)}?${q}`);
      return { statusCode: res.status, headers: CORS, body: res.body };
    }

    // GET /stop/:stopNbr
    const stopMatch = path.match(/^\/stop\/([^/]+)$/);
    if (stopMatch) {
      const res = await nuvizzGet(`/stop/info/${encodeURIComponent(stopMatch[1])}/${encodeURIComponent(COMPANY)}`);
      return { statusCode: res.status, headers: CORS, body: res.body };
    }

    // GET /load/:loadNbr
    const loadMatch = path.match(/^\/load\/([^/]+)$/);
    if (loadMatch) {
      const res = await nuvizzGet(`/load/info/${encodeURIComponent(loadMatch[1])}/${encodeURIComponent(COMPANY)}`);
      return { statusCode: res.status, headers: CORS, body: res.body };
    }

    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Unknown route', path }) };

  } catch (err) {
    return {
      statusCode: 502,
      headers: CORS,
      body: JSON.stringify({
        error: 'NuVizz proxy error',
        detail: err.message || String(err),
        base_url: BASE_URL,
        path_attempted: path,
      }),
    };
  }
};

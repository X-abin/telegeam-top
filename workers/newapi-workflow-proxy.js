const DEFAULT_ALLOWED_ORIGIN = 'https://x-abin.github.io';
const ALLOWED_API_PATHS = ['/api/channel/', '/api/log/'];

function corsHeaders(request, env) {
  const origin = request.headers.get('Origin') || '';
  const allowedOrigin = env.ALLOWED_ORIGIN || DEFAULT_ALLOWED_ORIGIN;
  return {
    'Access-Control-Allow-Origin': origin === allowedOrigin ? origin : allowedOrigin,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Accept',
    'Access-Control-Max-Age': '86400',
    Vary: 'Origin',
  };
}

function jsonResponse(request, env, status, payload) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      ...corsHeaders(request, env),
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-store',
    },
  });
}

function assertAllowedUrl(rawUrl) {
  const url = new URL(rawUrl);
  if (url.origin !== 'https://maolaoapi.com') {
    throw new Error('Only https://maolaoapi.com is allowed.');
  }
  if (!ALLOWED_API_PATHS.includes(url.pathname)) {
    throw new Error('Only /api/channel/ and /api/log/ are allowed.');
  }
  return url;
}

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(request, env) });
    }

    if (request.method !== 'POST') {
      return jsonResponse(request, env, 405, { success: false, message: 'Method not allowed.' });
    }

    let payload;
    try {
      payload = await request.json();
    } catch {
      return jsonResponse(request, env, 400, { success: false, message: 'Invalid JSON body.' });
    }

    let url;
    try {
      url = assertAllowedUrl(payload.url);
    } catch (error) {
      return jsonResponse(request, env, 400, { success: false, message: error.message });
    }

    const inputHeaders = payload.headers || {};
    const cookie = String(inputHeaders.Cookie || inputHeaders.cookie || '').trim();
    const apiUser = String(inputHeaders['New-Api-User'] || inputHeaders['new-api-user'] || '').trim();

    if (!cookie || !apiUser) {
      return jsonResponse(request, env, 400, {
        success: false,
        message: 'Cookie and New-Api-User are required.',
      });
    }

    const upstream = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        Accept: 'application/json',
        Cookie: cookie,
        'New-Api-User': apiUser,
      },
      redirect: 'manual',
      cf: { cacheTtl: 0, cacheEverything: false },
    });

    const body = await upstream.text();
    return new Response(body, {
      status: upstream.status,
      headers: {
        ...corsHeaders(request, env),
        'Content-Type': upstream.headers.get('Content-Type') || 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    });
  },
};

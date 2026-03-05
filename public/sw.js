// Nova File Manager — Service Worker v3 (PWA Enhanced)
// Strategy: stale-while-revalidate for shell, network-only for API

const CACHE_NAME  = 'nova-v3';
const OFFLINE_URL = '/offline.html';
const PRECACHE    = [
  '/',
  '/offline.html',
  '/manifest.json',
  '/favicon.svg',
  '/logo.svg',
  '/apple-touch-icon.png',
  '/icon-120.png',
  '/icon-152.png',
  '/icon-167.png',
  '/icon-192.png',
  '/icon-512.png'
];

// ── Install ───────────────────────────────────────────────────────
self.addEventListener('install', ev => {
  ev.waitUntil(
    caches.open(CACHE_NAME).then(c => c.addAll(PRECACHE))
  );
  self.skipWaiting();
});

// ── Activate: remove old caches ───────────────────────────────────
self.addEventListener('activate', ev => {
  ev.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

// ── Fetch ─────────────────────────────────────────────────────────
self.addEventListener('fetch', ev => {
  const { request } = ev;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;

  // API: always network-only — never cache live file data
  if (url.pathname.startsWith('/api/')) return;

  // Navigate requests: serve cached shell, fallback to offline
  if (request.mode === 'navigate') {
    ev.respondWith(
      caches.open(CACHE_NAME).then(cache =>
        cache.match('/').then(cached => {
          return fetch(request)
            .then(response => {
              if (response.ok) cache.put(request, response.clone());
              return response;
            })
            .catch(() => cached || cache.match(OFFLINE_URL));
        })
      )
    );
    return;
  }

  // Everything else: stale-while-revalidate
  ev.respondWith(
    caches.open(CACHE_NAME).then(cache =>
      cache.match(request).then(cached => {
        const fetched = fetch(request)
          .then(response => {
            if (response.ok) cache.put(request, response.clone());
            return response;
          })
          .catch(() => cached || caches.match(OFFLINE_URL));
        return cached || fetched;
      })
    )
  );
});

// ── Push Notifications (future-ready) ────────────────────────────
self.addEventListener('push', ev => {
  if (!ev.data) return;
  const data = ev.data.json();
  ev.waitUntil(
    self.registration.showNotification(data.title || 'Nova', {
      body: data.body || '',
      icon: '/icon-192.png',
      badge: '/icon-120.png',
      data: data
    })
  );
});

self.addEventListener('notificationclick', ev => {
  ev.notification.close();
  ev.waitUntil(clients.openWindow('/'));
});

// ── Background Sync (future-ready) ───────────────────────────────
self.addEventListener('sync', ev => {
  if (ev.tag === 'nova-sync') {
    ev.waitUntil(Promise.resolve());
  }
});

const CACHE_NAME = 'turu-home-v14-logo-typo4';
const APP_SHELL = [
  '/offline',
  '/static/css/style.css',
  '/static/img/TuruCAR_logo.png',
  '/static/img/brand_t_mark.png',
  '/static/img/icon-megaphone.png',
  '/static/img/icon-wash-order.png',
  '/static/img/icon-control.png',
  '/static/img/icon-complete.png',
  '/static/img/icons/favicon-64.png',
  '/static/img/icons/icon-192.png',
  '/static/img/icons/icon-512.png',
  '/static/img/icons/maskable-512.png'
];

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)));
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(caches.keys().then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)))));
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const request = event.request;
  const url = new URL(request.url);
  if (request.method !== 'GET' || url.origin !== self.location.origin) return;
  if (request.mode === 'navigate') {
    event.respondWith(fetch(request).catch(() => caches.match('/offline')));
    return;
  }
  if (url.pathname.startsWith('/static/')) {
    event.respondWith(caches.match(request).then((cached) => {
      const fresh = fetch(request).then((response) => {
        if (response && response.ok) caches.open(CACHE_NAME).then((cache) => cache.put(request, response.clone()));
        return response;
      }).catch(() => cached);
      return cached || fresh;
    }));
  }
});

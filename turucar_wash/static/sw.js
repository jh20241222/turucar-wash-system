const CACHE_NAME = 'turu-app-loader-v47';
const APP_SHELL = [
  '/offline',
  '/static/css/style.css?v=47',
  '/static/js/support_alerts.js',
  '/static/js/app_loader.js',
  '/static/img/turucar_logo_brand.png',
  '/static/img/icon-megaphone.png',
  '/static/img/icon-wash-order.png',
  '/static/img/icon-control.png',
  '/static/img/icon-complete.png',
  '/static/img/icon-urgent-wash.png',
  '/static/img/icon-support.png',
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
      const fresh = fetch(request).then((networkResponse) => {
        if (networkResponse && networkResponse.ok) {
          const toCache = networkResponse.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(request, toCache));
        }
        return networkResponse;
      }).catch(() => cached);
      return cached || fresh;
    }));
  }
});

// 웹 푸시 알림 (긴급세차 요청 등)
self.addEventListener('push', (event) => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch (e) {}
  const title = data.title || '투루카';
  const options = {
    body: data.body || '',
    icon: '/static/img/icons/icon-192.png',
    badge: '/static/img/icons/favicon-64.png',
    data: { url: data.url || '/urgent_wash' }
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || '/urgent_wash';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((list) => {
      for (const client of list) {
        if (client.url.includes(targetUrl) && 'focus' in client) return client.focus();
      }
      if (clients.openWindow) return clients.openWindow(targetUrl);
    })
  );
});

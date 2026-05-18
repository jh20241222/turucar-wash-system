(function () {
  const MOBILE_QUERY = '(max-width: 900px), (display-mode: standalone)';
  const MIN_VISIBLE_MS = 180;
  const SHOW_DELAY_MS = 70;
  let showTimer = null;
  let shownAt = 0;

  function isAppLike() {
    return window.matchMedia(MOBILE_QUERY).matches || window.navigator.standalone === true;
  }

  function overlay() {
    return document.getElementById('turuAppLoader');
  }

  function showLoader() {
    if (!isAppLike()) return;
    const el = overlay();
    if (!el) return;
    window.clearTimeout(showTimer);
    showTimer = window.setTimeout(function () {
      shownAt = Date.now();
      el.classList.add('is-visible');
      el.setAttribute('aria-hidden', 'false');
    }, SHOW_DELAY_MS);
  }

  function hideLoader() {
    window.clearTimeout(showTimer);
    const el = overlay();
    if (!el) return;
    const elapsed = Date.now() - shownAt;
    const wait = shownAt && elapsed < MIN_VISIBLE_MS ? MIN_VISIBLE_MS - elapsed : 0;
    window.setTimeout(function () {
      el.classList.remove('is-visible');
      el.setAttribute('aria-hidden', 'true');
      shownAt = 0;
    }, wait);
  }

  function isModifiedClick(event) {
    return event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0;
  }

  function shouldIgnoreLink(link) {
    if (!link) return true;
    if (link.hasAttribute('data-no-loader')) return true;
    if (link.target && link.target !== '_self') return true;
    const href = link.getAttribute('href') || '';
    if (!href || href === '#' || href.startsWith('#') || href.startsWith('javascript:')) return true;
    try {
      const url = new URL(link.href, window.location.href);
      if (url.origin !== window.location.origin) return true;
      if (url.pathname === window.location.pathname && url.search === window.location.search && url.hash) return true;
    } catch (e) {
      return true;
    }
    return false;
  }

  document.addEventListener('click', function (event) {
    if (event.defaultPrevented || isModifiedClick(event)) return;
    const link = event.target.closest('a[href]');
    if (shouldIgnoreLink(link)) return;
    if (event.target.closest('[data-no-loader], .pwa-install, [onclick*="openMobileFilter"], [onclick*="closeMobileFilter"]')) return;
    showLoader();
  }, true);

  document.addEventListener('submit', function (event) {
    if (event.defaultPrevented) return;
    const form = event.target;
    if (!form || form.hasAttribute('data-no-loader')) return;
    showLoader();
  }, true);

  window.addEventListener('pageshow', hideLoader);
  window.addEventListener('pagehide', showLoader);
  window.addEventListener('load', hideLoader);
  window.addEventListener('beforeunload', showLoader);
  document.addEventListener('visibilitychange', function () {
    if (document.visibilityState === 'visible') hideLoader();
  });

  window.TuruAppLoader = { show: showLoader, hide: hideLoader };
})();

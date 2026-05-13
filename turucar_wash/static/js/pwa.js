(function () {
  const installButtons = () => Array.from(document.querySelectorAll('.pwa-install'));
  let deferredPrompt = null;

  function isStandalone() {
    return window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone === true;
  }

  function setButtonsVisible(visible) {
    installButtons().forEach((button) => {
      button.hidden = !visible;
    });
  }

  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
      navigator.serviceWorker.register('/service-worker.js').catch(() => {});
    });
  }

  window.addEventListener('beforeinstallprompt', (event) => {
    if (isStandalone()) return;
    event.preventDefault();
    deferredPrompt = event;
    setButtonsVisible(true);
  });

  document.addEventListener('click', async (event) => {
    const button = event.target.closest('.pwa-install');
    if (!button || !deferredPrompt) return;

    button.disabled = true;
    deferredPrompt.prompt();
    await deferredPrompt.userChoice.catch(() => null);
    deferredPrompt = null;
    setButtonsVisible(false);
    button.disabled = false;
  });

  window.addEventListener('appinstalled', () => {
    deferredPrompt = null;
    setButtonsVisible(false);
  });
})();

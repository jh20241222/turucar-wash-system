(function () {
  function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
    const rawData = atob(base64);
    return Uint8Array.from([...rawData].map((c) => c.charCodeAt(0)));
  }

  function bells() {
    return Array.from(document.querySelectorAll('.push-bell-btn'));
  }

  // 종 아이콘 상태 표시: 허용(구독됨) = 오렌지색 종 / 미허용(미구독) = 회색 빗금 종
  function setBellState(on) {
    bells().forEach((btn) => {
      const icon = btn.querySelector('i');
      btn.classList.toggle('is-on', !!on);
      if (icon) icon.className = on ? 'ti ti-bell' : 'ti ti-bell-off';
    });
  }

  async function subscribeNow() {
    if (!('serviceWorker' in navigator) || !('PushManager' in window) || !('Notification' in window)) {
      return false;
    }
    try {
      const keyResp = await fetch('/push/vapid_public_key');
      const keyData = await keyResp.json();
      if (!keyData.available || !keyData.key) return false;

      const reg = await navigator.serviceWorker.ready;
      let sub = await reg.pushManager.getSubscription();

      if (!sub) {
        const perm = Notification.permission === 'granted'
          ? 'granted'
          : await Notification.requestPermission();
        if (perm !== 'granted') return false;
        sub = await reg.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: urlBase64ToUint8Array(keyData.key)
        });
      }

      await fetch('/push/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(sub)
      });
      return true;
    } catch (e) {
      console.error('[Push] 구독 실패', e);
      return false;
    }
  }

  async function refreshBellState() {
    if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
      setBellState(false);
      return;
    }
    try {
      const reg = await navigator.serviceWorker.ready;
      const sub = await reg.pushManager.getSubscription();
      setBellState(!!sub);
    } catch (e) {
      setBellState(false);
    }
  }

  document.addEventListener('click', function (event) {
    const btn = event.target.closest('.push-bell-btn');
    if (!btn) return;
    if (btn.classList.contains('is-on')) return; // 이미 켜져 있으면 클릭해도 그대로 유지
    subscribeNow().then(refreshBellState);
  });

  document.addEventListener('DOMContentLoaded', async function () {
    await refreshBellState();
    // 버튼 클릭 없이도 가능한 경우 자동으로 구독을 시도한다 (권한이 이미 허용된 상태이거나
    // 브라우저가 자동 요청을 막지 않는 경우에는 사용자가 아무것도 안 눌러도 알림이 켜진다).
    const reg = ('serviceWorker' in navigator) ? await navigator.serviceWorker.ready.catch(() => null) : null;
    const sub = reg ? await reg.pushManager.getSubscription().catch(() => null) : null;
    if (!sub && typeof Notification !== 'undefined' && Notification.permission !== 'denied') {
      const ok = await subscribeNow();
      if (ok) setBellState(true);
    }
  });
})();

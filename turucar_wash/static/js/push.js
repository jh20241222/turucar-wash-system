(function () {
  function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
    const rawData = atob(base64);
    return Uint8Array.from([...rawData].map((c) => c.charCodeAt(0)));
  }

  function setBtnState(btn, subscribed) {
    if (!btn) return;
    btn.textContent = subscribed ? '알림 켜짐' : '알림 받기';
    btn.disabled = subscribed;
  }

  async function turuEnablePush(btn) {
    if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
      alert('이 브라우저(또는 앱 설치 방식)는 푸시 알림을 지원하지 않습니다.');
      return;
    }
    try {
      const keyResp = await fetch('/push/vapid_public_key');
      const keyData = await keyResp.json();
      if (!keyData.available || !keyData.key) {
        alert('서버에 푸시 알림이 아직 설정되지 않았습니다.');
        return;
      }
      const perm = await Notification.requestPermission();
      if (perm !== 'granted') {
        alert('알림 권한이 허용되지 않았습니다.');
        return;
      }
      const reg = await navigator.serviceWorker.ready;
      let sub = await reg.pushManager.getSubscription();
      if (!sub) {
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
      setBtnState(btn, true);
    } catch (e) {
      console.error('[Push] 구독 실패', e);
      alert('알림 켜기에 실패했습니다. 잠시 후 다시 시도해주세요.');
    }
  }

  document.addEventListener('click', function (event) {
    const btn = event.target.closest('.push-enable-btn');
    if (!btn) return;
    turuEnablePush(btn);
  });

  document.addEventListener('DOMContentLoaded', async function () {
    if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;
    try {
      const reg = await navigator.serviceWorker.ready;
      const sub = await reg.pushManager.getSubscription();
      document.querySelectorAll('.push-enable-btn').forEach((btn) => setBtnState(btn, !!sub));
    } catch (e) {}
  });
})();

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

  // 브라우저 알림 권한 요청은 반드시 "사용자 클릭" 안에서 호출해야 한다.
  // 클릭 없이 자동으로 요청하면 크롬이 스팸으로 판단해 이후 권한 요청 자체를 영구 차단해버린다
  // ("Notifications permission has been blocked as the user has ignored the permission prompt
  //  several times" 콘솔 경고가 그 증거). 그래서 종 아이콘 클릭 시에만 요청한다.
  async function subscribeNow() {
    if (!('serviceWorker' in navigator) || !('PushManager' in window) || !('Notification' in window)) {
      alert('이 브라우저(또는 앱 설치 방식)는 푸시 알림을 지원하지 않습니다.');
      return false;
    }
    if (Notification.permission === 'denied') {
      alert('알림이 브라우저에서 차단된 상태입니다. 주소창 옆 사이트 설정에서 알림 권한을 초기화한 뒤 다시 시도해주세요.');
      return false;
    }
    try {
      const keyResp = await fetch('/push/vapid_public_key');
      const keyData = await keyResp.json();
      if (!keyData.available || !keyData.key) {
        alert('서버에 푸시 알림이 아직 설정되지 않았습니다.');
        return false;
      }

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
    if (btn.classList.contains('is-on')) return; // 이미 켜져 있으면 그대로 둔다
    subscribeNow().then((ok) => { if (ok) setBellState(true); });
  });

  document.addEventListener('DOMContentLoaded', refreshBellState);
})();

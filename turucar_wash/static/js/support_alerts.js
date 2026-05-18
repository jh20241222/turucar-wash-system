(function () {
    const cfg = window.TURU_SUPPORT_ALERTS || {};
    if (!cfg.pollUrl || !cfg.manageUrl) return;

    const STORAGE_KEY = 'turu_support_last_seen_ticket_id_' + (cfg.username || 'admin');
    const FIRST_RUN_KEY = 'turu_support_alerts_initialized_' + (cfg.username || 'admin');
    const POLL_INTERVAL_MS = 10000;

    let polling = false;
    let timerId = null;

    function getLastSeen() {
        const value = Number(localStorage.getItem(STORAGE_KEY) || '0');
        return Number.isFinite(value) ? value : 0;
    }

    function setLastSeen(value) {
        if (Number.isFinite(Number(value))) {
            localStorage.setItem(STORAGE_KEY, String(value));
        }
    }

    function ensureToastRoot() {
        let root = document.querySelector('.support-alert-toast-root');
        if (!root) {
            root = document.createElement('div');
            root.className = 'support-alert-toast-root';
            document.body.appendChild(root);
        }
        return root;
    }


    function formatBadgeCount(count) {
        const value = Number(count || 0);
        if (!Number.isFinite(value) || value <= 0) return '';
        return value > 99 ? '99+' : String(value);
    }

    function updateSupportBadge(totalCount) {
        const badge = document.querySelector('[data-support-badge]');
        if (!badge) return;
        const text = formatBadgeCount(totalCount);
        if (!text) {
            badge.textContent = '';
            badge.classList.add('is-hidden');
            return;
        }
        badge.textContent = text;
        badge.classList.remove('is-hidden');
        badge.classList.add('bump');
        setTimeout(function () {
            badge.classList.remove('bump');
        }, 360);
    }

    function showToast(count, latestTicket) {
        const root = ensureToastRoot();
        const toast = document.createElement('button');
        toast.type = 'button';
        toast.className = 'support-alert-toast';
        toast.innerHTML = `
            <span class="support-alert-icon" aria-hidden="true">💬</span>
            <span class="support-alert-copy">
                <strong>문의가 ${count}건 등록되었습니다.</strong>
                <small>${latestTicket && latestTicket.requester ? latestTicket.requester + ' · ' : ''}문의 관리에서 확인해주세요.</small>
            </span>
            <span class="support-alert-arrow" aria-hidden="true">›</span>
        `;
        toast.addEventListener('click', function () {
            window.location.href = cfg.manageUrl;
        });
        root.appendChild(toast);

        requestAnimationFrame(function () {
            toast.classList.add('show');
        });

        setTimeout(function () {
            toast.classList.remove('show');
            setTimeout(function () {
                toast.remove();
            }, 260);
        }, 6500);
    }

    async function pollSupportTickets() {
        if (polling || document.hidden) return;
        polling = true;
        try {
            const lastSeen = getLastSeen();
            const url = new URL(cfg.pollUrl, window.location.origin);
            url.searchParams.set('since_id', String(lastSeen));
            const response = await fetch(url.toString(), {
                credentials: 'same-origin',
                headers: { 'Accept': 'application/json' }
            });
            if (!response.ok) return;
            const data = await response.json();
            if (!data || !data.ok) return;

            updateSupportBadge(data.total_count || 0);

            const hasInitialized = localStorage.getItem(FIRST_RUN_KEY) === '1';
            if (!hasInitialized) {
                setLastSeen(data.max_id || 0);
                localStorage.setItem(FIRST_RUN_KEY, '1');
                return;
            }

            if ((data.new_count || 0) > 0) {
                showToast(data.new_count, data.latest_ticket);
                setLastSeen(data.max_id || lastSeen);
            } else if ((data.max_id || 0) > lastSeen) {
                setLastSeen(data.max_id);
            }
        } catch (err) {
            console.warn('support alert polling failed', err);
        } finally {
            polling = false;
        }
    }

    function startPolling() {
        if (timerId) clearInterval(timerId);
        pollSupportTickets();
        timerId = setInterval(pollSupportTickets, POLL_INTERVAL_MS);
    }

    document.addEventListener('visibilitychange', function () {
        if (!document.hidden) pollSupportTickets();
    });

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', startPolling);
    } else {
        startPolling();
    }
})();

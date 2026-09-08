/*
 * 오프라인 세차 완료처리 큐 (2026-09-08)
 * =========================================================
 * 지하주차장처럼 신호가 아예 안 잡히는 곳에서도 작업자가 세차 완료처리(사진+폼 제출)를
 * 끝내고 바로 다음 차량으로 넘어갈 수 있게 하기 위한 모듈. car_detail.html의
 * turuHandleWashSubmit()이 /wash_complete/<id> 제출을 fetch로 시도했다가 네트워크
 * 자체가 안 되면(진짜 오프라인 — TypeError로 fetch가 reject됨) 이 큐에 폼 전체(입력값 +
 * 사진 파일)를 통째로 담아두고, 신호가 잡히는 즉시(다른 차 사진을 찍는 동안이든, 지상으로
 * 올라오든) 자동으로 순서대로(먼저 쌓인 것부터) 서버에 다시 보낸다. 여러 대를 연달아
 * 오프라인 상태로 완료처리해도 전부 큐에 쌓였다가 신호 잡히는 대로 한 번에 전송된다.
 *
 * 서버(app.py wash_complete)는 처리 후 그 오더를 wash_list에서 지운다(완료=0인 행만
 * 삭제). 그래서 큐에 있는 요청이 뒤늦게 도착했을 때 이미 다른 경로로 처리돼 있어도
 * (예: 같은 오더를 중복으로 큐에 담은 경우) 서버는 "이미 완료 처리됐거나 존재하지 않는
 * 오더입니다"라는 안내만 돌려줄 뿐 중복 반영되지 않는다 — 서버까지 어떻게든 도달만
 * 하면(성공이든 이미-처리됨 안내든) 이 요청은 이제 서버가 알고 처리한 것이므로 큐에서
 * 안전하게 지워도 된다는 뜻이다. 오직 fetch 자체가 실패(reject)하는 경우, 즉 아직도
 * 진짜 오프라인인 경우에만 큐에 남겨두고 다음 기회에 다시 시도한다.
 */
(function () {
    'use strict';

    var DB_NAME = 'turu_offline_queue';
    var DB_VERSION = 1;
    var STORE = 'completions';

    function openDb() {
        return new Promise(function (resolve, reject) {
            if (!window.indexedDB) {
                reject(new Error('이 브라우저는 오프라인 저장(IndexedDB)을 지원하지 않습니다.'));
                return;
            }
            var req = indexedDB.open(DB_NAME, DB_VERSION);
            req.onupgradeneeded = function () {
                var db = req.result;
                if (!db.objectStoreNames.contains(STORE)) {
                    db.createObjectStore(STORE, { keyPath: 'id', autoIncrement: true });
                }
            };
            req.onsuccess = function () { resolve(req.result); };
            req.onerror = function () { reject(req.error); };
        });
    }

    function dbAdd(record) {
        return openDb().then(function (db) {
            return new Promise(function (resolve, reject) {
                var tx = db.transaction(STORE, 'readwrite');
                tx.objectStore(STORE).add(record);
                tx.oncomplete = function () { resolve(); };
                tx.onerror = function () { reject(tx.error); };
            });
        });
    }

    function dbGetAll() {
        return openDb().then(function (db) {
            return new Promise(function (resolve, reject) {
                var tx = db.transaction(STORE, 'readonly');
                var req = tx.objectStore(STORE).getAll();
                req.onsuccess = function () { resolve(req.result || []); };
                req.onerror = function () { reject(req.error); };
            });
        });
    }

    function dbDelete(id) {
        return openDb().then(function (db) {
            return new Promise(function (resolve, reject) {
                var tx = db.transaction(STORE, 'readwrite');
                tx.objectStore(STORE).delete(id);
                tx.oncomplete = function () { resolve(); };
                tx.onerror = function () { reject(tx.error); };
            });
        });
    }

    // formEl(폼 엘리먼트) 자체를 그대로 받아서, 지금 이 순간의 입력값/파일을 전부
    // FormData로 스냅샷 떠서 저장한다 — 나중에 폼이 초기화되거나 다른 화면으로 이동해도
    // 영향받지 않는다.
    function add(url, carId, formEl) {
        var fd = new FormData(formEl);
        var entries = [];
        var it = fd.entries();
        var pair = it.next();
        while (!pair.done) {
            entries.push(pair.value); // [key, value] — value는 문자열 또는 File(Blob)
            pair = it.next();
        }
        return dbAdd({ url: url, carId: carId, entries: entries, queuedAt: Date.now() }).then(function () {
            notifyChange();
        });
    }

    var flushing = false;
    function flush() {
        if (flushing) return Promise.resolve();
        flushing = true;
        return dbGetAll().then(function (records) {
            var chain = Promise.resolve();
            var synced = 0;
            var stop = false;
            records.forEach(function (record) {
                chain = chain.then(function () {
                    if (stop) return;
                    var fd = new FormData();
                    record.entries.forEach(function (pair) { fd.append(pair[0], pair[1]); });
                    return fetch(record.url, { method: 'POST', body: fd, credentials: 'same-origin' })
                        .then(function () {
                            synced++;
                            return dbDelete(record.id);
                        })
                        .catch(function () {
                            // 아직 오프라인 — 이번 라운드는 여기서 멈추고 다음 기회에 이어서 시도.
                            // (먼저 쌓인 순서대로 시도하다가 첫 실패에서 멈추면, 뒤에 있는 것들도
                            // 어차피 같은 이유로 실패할 가능성이 높아 불필요한 시도를 줄인다.)
                            stop = true;
                        });
                });
            });
            return chain.then(function () { return synced; });
        }).then(function (synced) {
            flushing = false;
            notifyChange();
            if (synced > 0) notifyFlushed(synced);
            return synced;
        }).catch(function (e) {
            flushing = false;
            console.warn('[오프라인 큐] flush 실패', e);
            return 0;
        });
    }

    function getCount() {
        return dbGetAll().then(function (r) { return r.length; }).catch(function () { return 0; });
    }

    function getQueuedCarIds() {
        return dbGetAll().then(function (r) { return r.map(function (x) { return x.carId; }); }).catch(function () { return []; });
    }

    // ---- 대기 중 건수 배지 (모바일/데스크탑 공통, 화면 아무데서나 눌러서 수동 재시도 가능) ----
    var badgeEl = null;
    function ensureBadge() {
        if (badgeEl || !document.body) return badgeEl;
        badgeEl = document.createElement('div');
        badgeEl.id = 'turu-offline-badge';
        badgeEl.style.cssText = 'position:fixed;left:50%;bottom:82px;transform:translateX(-50%);z-index:9997;' +
            'background:#212121;color:#fff;font-size:12px;font-weight:700;padding:9px 16px;border-radius:999px;' +
            'box-shadow:0 4px 14px rgba(0,0,0,0.28);display:none;cursor:pointer;' +
            "font-family:'Pretendard','Apple SD Gothic Neo',sans-serif;white-space:nowrap;text-align:center;";
        badgeEl.title = '눌러서 지금 바로 다시 전송을 시도합니다';
        badgeEl.addEventListener('click', function () { flush(); });
        document.body.appendChild(badgeEl);
        return badgeEl;
    }
    function notifyChange() {
        getCount().then(function (count) {
            var el = ensureBadge();
            if (!el) return;
            if (count > 0) {
                el.textContent = '🔄 동기화 대기 ' + count + '건 (눌러서 재시도)';
                el.style.display = 'block';
            } else {
                el.style.display = 'none';
            }
        });
    }
    function notifyFlushed(n) {
        var el = ensureBadge();
        if (!el) return;
        el.style.display = 'block';
        el.textContent = '✔ ' + n + '건 자동 전송 완료';
        setTimeout(notifyChange, 2500);
    }

    // ---- 자동 재시도 트리거: 페이지 진입 시, 온라인 복귀 시, 대기 항목이 있는 동안 주기적으로 ----
    window.addEventListener('online', function () { flush(); });
    setInterval(function () { flush(); }, 20000);

    function init() {
        notifyChange();
        flush();
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    window.TuruOfflineQueue = {
        add: add,
        flush: flush,
        getCount: getCount,
        getQueuedCarIds: getQueuedCarIds,
    };
})();

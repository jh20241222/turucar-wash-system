/*
 * turu_slot_photo_cache.js (2026-09-08)
 *
 * 지하주차장처럼 신호가 아예 안 잡히는 곳에서 세차 사진(슬롯)을 찍으면, 지금까지는
 * "찍는 즉시 백그라운드 업로드"가 실패해도 브라우저의 file input에 원본이 남아있어
 * 완료처리 제출 때 같이 올라가도록 안전장치가 있었다. 하지만 그 사이에 페이지를
 * 새로고침하거나, 앱을 완전히 닫거나, 신호를 오래 못 잡아 브라우저가 탭을 정리해버리면
 * — file input의 내용은 브라우저 메모리에만 있던 것이라 그대로 사라진다. 이게 실제로
 * "사진 유실"이 일어날 수 있는 가장 큰 구멍이다(서버 요청이 실패하는 문제가 아니라
 * 애초에 요청을 보내보기도 전에 화면 자체가 없어지는 문제).
 *
 * 그래서 슬롯 사진은 "찍는 순간" 바로 이 IndexedDB에도 원본을 복사해둔다. 백그라운드
 * 업로드가 성공하면(=서버에 안전하게 저장됨) 캐시에서 지우고, 완료처리 제출이 성공하거나
 * 오프라인 큐에 등록되면(=어떻게든 안전하게 보관됨) 그 차량 주문의 캐시를 전부 지운다.
 * 반대로 페이지가 다시 열렸는데 아직 캐시에 남아있는 사진이 있으면(=아직 어디에도
 * 안전하게 도달 못한 사진), 화면 로드 시 자동으로 복구해서 다시 업로드를 시도한다 —
 * 작업자가 사진을 다시 찍을 필요 없이 그대로 이어서 진행된다.
 */
(function () {
    var DB_NAME = 'turu_slot_photo_cache';
    var DB_VERSION = 1;
    var STORE = 'photos';

    function openDb() {
        return new Promise(function (resolve, reject) {
            var req = indexedDB.open(DB_NAME, DB_VERSION);
            req.onupgradeneeded = function () {
                var db = req.result;
                if (!db.objectStoreNames.contains(STORE)) {
                    db.createObjectStore(STORE, { keyPath: 'key' });
                }
            };
            req.onsuccess = function () { resolve(req.result); };
            req.onerror = function () { reject(req.error); };
        });
    }

    function makeKey(carId, slotKey) {
        return String(carId) + '::' + String(slotKey);
    }

    function withStore(mode, fn) {
        return openDb().then(function (db) {
            return new Promise(function (resolve, reject) {
                var tx = db.transaction(STORE, mode);
                var store = tx.objectStore(STORE);
                var result;
                Promise.resolve(fn(store)).then(function (r) { result = r; }).catch(reject);
                tx.oncomplete = function () { resolve(result); db.close(); };
                tx.onerror = function () { reject(tx.error); db.close(); };
                tx.onabort = function () { reject(tx.error); db.close(); };
            });
        });
    }

    function save(carId, slotKey, file) {
        return withStore('readwrite', function (store) {
            store.put({
                key: makeKey(carId, slotKey),
                carId: carId,
                slotKey: slotKey,
                fileName: file.name || 'photo.jpg',
                fileType: file.type || 'image/jpeg',
                blob: file,
                savedAt: Date.now()
            });
        }).catch(function (e) {
            // 캐시 저장 실패(예: 저장공간 부족)는 조용히 무시한다 — 이건 어디까지나
            // "한 겹 더" 안전장치일 뿐이라, 실패해도 기존 흐름(파일이 input에 남아있다가
            // 완료처리 때 같이 제출됨)은 그대로 동작한다.
            console.warn('[슬롯사진캐시] 저장 실패(무시하고 진행):', e);
        });
    }

    function remove(carId, slotKey) {
        return withStore('readwrite', function (store) {
            store.delete(makeKey(carId, slotKey));
        }).catch(function (e) {
            console.warn('[슬롯사진캐시] 삭제 실패:', e);
        });
    }

    function getAllForCar(carId) {
        return withStore('readonly', function (store) {
            return new Promise(function (resolve, reject) {
                var out = [];
                var req = store.openCursor();
                req.onsuccess = function () {
                    var cursor = req.result;
                    if (!cursor) { resolve(out); return; }
                    var rec = cursor.value;
                    if (String(rec.carId) === String(carId)) {
                        out.push(rec);
                    }
                    cursor.continue();
                };
                req.onerror = function () { reject(req.error); };
            });
        }).catch(function (e) {
            console.warn('[슬롯사진캐시] 조회 실패:', e);
            return [];
        });
    }

    function clearForCar(carId) {
        return getAllForCar(carId).then(function (recs) {
            return withStore('readwrite', function (store) {
                recs.forEach(function (rec) { store.delete(rec.key); });
            });
        }).catch(function (e) {
            console.warn('[슬롯사진캐시] 정리 실패:', e);
        });
    }

    window.TuruSlotPhotoCache = {
        save: save,
        remove: remove,
        getAllForCar: getAllForCar,
        clearForCar: clearForCar
    };
})();

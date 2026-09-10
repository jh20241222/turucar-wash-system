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

    function save(carId, slotKey, file, capturedAt) {
        return withStore('readwrite', function (store) {
            store.put({
                key: makeKey(carId, slotKey),
                carId: carId,
                slotKey: slotKey,
                fileName: file.name || 'photo.jpg',
                fileType: file.type || 'image/jpeg',
                blob: file,
                // (2026-09-10) 이 값은 "이 사진이 몇 번째로 최신 촬영본인지" 구분하는
                // 용도로도 쓰인다(removeIfMatches 참고) — 호출한 쪽이 캡처 시각을
                // 명시적으로 넘기면 그대로 쓰고, 안 넘기면 지금 시각으로 대체한다.
                savedAt: (typeof capturedAt === 'number') ? capturedAt : Date.now()
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

    // (2026-09-10) "두 번째 사진을 찍었더니 사진이 없어졌다" 버그의 원인 중 하나 —
    // 오래된(이미 재촬영으로 대체된) 업로드 시도가 뒤늦게 성공 응답을 받고서 무조건
    // 캐시를 지워버리면, 그 사이 새로 찍혀서 캐시에 들어간 "더 최신" 사진의 유일한
    // 안전장치까지 함께 사라진다. 그래서 삭제 직전에 "지금 캐시에 있는 게 정말 내가
    // 방금 올린 그 사진이 맞는지"(savedAt 일치 여부)를 한 트랜잭션 안에서 확인하고,
    // 일치할 때만 지운다 — 이미 더 최신 사진으로 덮어써졌다면 그 사진은 그대로 둔다.
    function removeIfMatches(carId, slotKey, expectedSavedAt) {
        return withStore('readwrite', function (store) {
            return new Promise(function (resolve, reject) {
                var key = makeKey(carId, slotKey);
                var getReq = store.get(key);
                getReq.onsuccess = function () {
                    var rec = getReq.result;
                    if (!rec || rec.savedAt !== expectedSavedAt) {
                        resolve();
                        return;
                    }
                    var delReq = store.delete(key);
                    delReq.onsuccess = function () { resolve(); };
                    delReq.onerror = function () { reject(delReq.error); };
                };
                getReq.onerror = function () { reject(getReq.error); };
            });
        }).catch(function (e) {
            console.warn('[슬롯사진캐시] 조건부 삭제 실패:', e);
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
        removeIfMatches: removeIfMatches,
        getAllForCar: getAllForCar,
        clearForCar: clearForCar
    };
})();

# Turu Wash 데이터 영구 보존 설정

이 패치는 계정/지역, 업체관리, 세차오더, 완료현황을 모두 `DATA_DIR` 아래 SQLite 파일에만 저장하도록 보호합니다.

## 반드시 해야 하는 Railway 설정

1. Railway 서비스에 Volume을 추가합니다.
2. Volume Mount Path를 `/app/data` 로 설정합니다.
3. Variables에 아래 값을 추가합니다.

```text
DATA_DIR=/app/data
PERSISTENCE_STRICT=1
```

## 저장 위치

```text
/app/data/db.sqlite3      # 계정, 권한, 업체, 지역, 공지사항
/app/data/wash.db         # 세차 오더, 완료 현황
/app/data/uploads/        # 업로드 파일
/app/data/backups/        # 자동 백업
```

## 안전장치

Railway에서 `DATA_DIR` 또는 Volume 경로가 설정되지 않으면 앱이 일부러 시작되지 않습니다.
잘못된 임시 저장소로 실행되어 데이터가 유실되는 것을 막기 위한 동작입니다.

## 상태 확인

마스터 계정으로 로그인 후 아래 주소를 열면 현재 저장소 경로와 데이터 건수를 확인할 수 있습니다.

```text
/storage-status
```

## 절대 하지 말 것

- Railway Volume 삭제
- DATA_DIR 값 변경
- `/app/data` 내부 파일 수동 삭제
- `wash.db`, `db.sqlite3`, `uploads` 덮어쓰기
- `init_db.py` 수동 실행

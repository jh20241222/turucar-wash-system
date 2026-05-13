# TURU Wash native-like app update

이 패치는 Railway가 보고 있는 GitHub 폴더 `turucar_wash/`에 덮어씌우는 용도입니다.

반영 파일:

- `app.py`
- `requirements.txt`
- `Procfile`
- `templates/`
- `static/`

중요:

- `wash.db`는 포함하지 않았습니다. 운영 데이터 보호용입니다.
- `uploads/`, `.pem`, `.pub` 키 파일도 포함하지 않았습니다.
- 기존 GitHub 폴더의 `wash.db`, `차량소속별_밴드매칭.xlsx`는 지우지 말고 그대로 두세요.
- Railway에서 Volume을 쓴다면 `DATA_DIR=/app/data`와 Mount path `/app/data`를 권장합니다.

배포 흐름:

1. GitHub Desktop으로 repo를 clone합니다.
2. 이 ZIP 안의 `turucar_wash/` 내용을 기존 repo의 `turucar_wash/`에 복사해서 덮어씁니다.
3. GitHub Desktop에서 Commit 합니다.
4. Push 합니다.
5. Railway Deployments에서 자동 재배포 성공 여부를 확인합니다.

반영 확인 URL:

- `/static/manifest.webmanifest`
- `/service-worker.js`
- `/wash_list`

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV FLASK_APP=app.py
ENV FLASK_ENV=production

EXPOSE 8080

# --timeout 180: gunicorn의 기본 sync 워커 타임아웃(30초)이 세차완료 사진 업로드
# 요청(여러 장을 순차로 압축 -> R2 업로드)보다 짧아서, 사진이 많거나 현장 네트워크가
# 느릴 때 처리 도중 워커가 강제 종료되고(SIGABRT) 연결이 끊기는 문제가 있었다
# (2026-09-02 실사용 중 확인 — 브라우저에 ERR_HTTP2_PROTOCOL_ERROR로 표시됨).
# 넉넉하게 180초로 올려서 이 요청이 타임아웃보다 먼저 끝나도록 한다.
#
# --worker-class gthread --workers 3 --threads 4: 기존엔 sync 워커 2개뿐이라, 느린
# 사진 업로드 요청이 딱 2건만 동시에 몰려도(현장 네트워크 느린 작업자 2명이 동시에
# 사진을 올리는 등) 서버 전체가 사실상 응답 불가 상태가 됐다 — 훼손 제보 슬랙 알림이
# 이미지 URL을 슬랙이 즉시 못 가져가서 깨진 이미지로 뜨는 문제나, 다른 사용자의
# 페이지 로딩이 멈추는 문제(2026-09-08 확인)도 결국 이 워커 부족이 원인일 가능성이
# 높다. gthread로 바꾸고 워커당 스레드를 늘리면, 워커 프로세스 수(메모리 사용량)는
# 크게 늘리지 않으면서도 동시 처리 가능한 요청 수를 2 -> 최대 12(3워커 x 4스레드)로
# 늘릴 수 있다.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--worker-class", "gthread", "--workers", "3", "--threads", "4", "--timeout", "180", "app:app"]

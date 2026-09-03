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
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "180", "app:app"]

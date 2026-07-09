import os

# 하드코딩하지 않는다. 실제 시크릿 키는 환경변수 SECRET_KEY로 주입한다.
# (참고: 현재 app.py는 이 파일을 import하지 않고 자체적으로 SECRET_KEY를 관리합니다.
#  이 파일은 혹시 다른 곳에서 참조될 경우를 대비해 안전하게 맞춰둔 것입니다.)
SECRET_KEY = os.environ.get("SECRET_KEY", "")
UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"xlsx"}

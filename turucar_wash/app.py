import os
import sqlite3
from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, jsonify, send_file
)
from flask_login import (
    LoginManager, UserMixin, login_user,
    logout_user, login_required, current_user
)
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO

app = Flask(__name__)
app.secret_key = "turu_secret_key"

# =========================================================
# [중요] DB 및 경로 설정 (Vercel 절대 경로 대응)
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_DB_PATH = os.path.join(BASE_DIR, "db.sqlite3")
WASH_DB_PATH = os.path.join(BASE_DIR, "wash.db")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
BAND_MATCHING_PATH = os.path.join(BASE_DIR, "차량소속별_밴드매칭.xlsx")

def get_user_db():
    conn = sqlite3.connect(USER_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_wash_db():
    conn = sqlite3.connect(WASH_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# =========================================================
# 로그인 설정
# =========================================================
login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.init_app(app)

class User(UserMixin):
    def __init__(self, id, username, role, vendor):
        self.id = id
        self.username = username
        self.role = role
        self.vendor = vendor

@login_manager.user_loader
def load_user(user_id):
    conn = get_user_db()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM accounts WHERE id=?", (user_id,)).fetchone()
    conn.close()
    if row:
        return User(row["id"], row["username"], row["role"], row["vendor"])
    return None

# =========================================================
# 라우트: 로그인 / 로그아웃
# =========================================================
@app.route("/")
@login_required
def home():
    return redirect(url_for("dashboard"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        pw = request.form.get("password")

        conn = get_user_db()
        cur = conn.cursor()
        user = cur.execute("SELECT * FROM accounts WHERE username=?", (username,)).fetchone()
        conn.close()

        if user and check_password_hash(user["password"], pw):
            login_user(User(user["id"], user["username"], user["role"], user["vendor"]))
            return redirect(url_for("dashboard"))

        flash("❌ 아이디 또는 비밀번호가 잘못되었습니다.")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

# =========================================================
# 대시보드
# =========================================================
@app.route("/dashboard")
@login_required
def dashboard():
    conn = get_wash_db()
    cur = conn.cursor()
    
    # 관리자/업체 권한에 따른 통계 처리
    if current_user.role == "admin":
        total_count = cur.execute("SELECT COUNT(*) AS c FROM wash_list").fetchone()["c"]
        done_count = cur.execute("SELECT COUNT(*) AS c FROM wash_history").fetchone()["c"]
        vendor_counts = cur.execute("SELECT 업체, COUNT(*) AS c FROM wash_list GROUP BY 업체").fetchall()
    else:
        total_count = cur.execute("SELECT COUNT(*) AS c FROM wash_list WHERE 업체=?", (current_user.vendor,)).fetchone()["c"]
        done_count = cur.execute("SELECT COUNT(*) AS c FROM wash_history WHERE 업체=?", (current_user.vendor,)).fetchone()["c"]
        vendor_counts = []

    conn.close()
    return render_template("dashboard.html", total_count=total_count, done_count=done_count, vendor_counts=vendor_counts)

# =========================================================
# 세차 대상 업로드
# =========================================================
@app.route("/upload_wash_list", methods=["GET", "POST"])
@login_required
def upload_wash_list():
    if request.method == "POST":
        wash_date = request.form.get("wash_date")
        file = request.files.get("file")
        
        if not wash_date or not file:
            flash("❌ 날짜와 파일을 모두 선택해주세요.")
            return redirect(url_for("upload_wash_list"))

        os.makedirs(UPLOAD_DIR, exist_ok=True)
        filepath = os.path.join(UPLOAD_DIR, secure_filename(file.filename))
        file.save(filepath)

        df = pd.read_excel(filepath)
        
        # 밴드 매칭 데이터 로드
        band_dict = {}
        if os.path.exists(BAND_MATCHING_PATH):
            map_df = pd.read_excel(BAND_MATCHING_PATH)
            band_dict = dict(zip(map_df["차량소속"], map_df["밴드링크"]))

        conn = get_wash_db()
        cur = conn.cursor()
        for _, r in df.iterrows():
            band = band_dict.get(r["차량소속"], None)
            cur.execute("""
                INSERT INTO wash_list (차량번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 세차일, 업체, 밴드링크, 완료)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """, (r["차량번호"], r["차종명"], r["차량소속"], r["현재스팟명"], r["현재스팟주소"], r["지역(시/도)"], r["지역(구/군)"], wash_date, r["담당업체"], band))
        
        conn.commit()
        conn.close()
        flash("✔ 업로드 완료")
        return redirect(url_for("upload_wash_list"))

    return render_template("upload_wash_list.html")

# =========================================================
# 세차 리스트 (필터 포함)
# =========================================================
@app.route("/wash_list")
@login_required
def wash_list():
    today = datetime.today().strftime("%Y-%m-%d")
    selected_date = request.args.get("date", today)
    
    conn = get_wash_db()
    cur = conn.cursor()

    query = "SELECT * FROM wash_list WHERE 세차일 = ?"
    params = [selected_date]

    # 업체 계정은 본인 업체 데이터만 조회
    if current_user.role != "admin":
        query += " AND 업체 = ?"
        params.append(current_user.vendor)

    rows = cur.execute(query, params).fetchall()
    
    # 필터용 데이터 추출
    def get_distinct(col):
        return [r[0] for r in cur.execute(f"SELECT DISTINCT {col} FROM wash_list").fetchall() if r[0]]

    region1 = get_distinct("지역시도")
    vendor_list = get_distinct("업체")

    conn.close()
    return render_template("wash_list.html", rows=rows, selected_date=selected_date, region1=region1, vendor_list=vendor_list)

# =========================================================
# 세차 완료 처리
# =========================================================
@app.route("/car_detail/<int:id>")
@login_required
def car_detail(id):
    conn = get_wash_db()
    car = conn.execute("SELECT * FROM wash_list WHERE id=?", (id,)).fetchone()
    conn.close()
    return render_template("car_detail.html", car=car)

@app.route("/wash_complete/<int:id>", methods=["POST"])
@login_required
def wash_complete(id):
    conn = get_wash_db()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM wash_list WHERE id=?", (id,)).fetchone()

    if row:
        done_date = datetime.now().strftime("%Y-%m-%d")
        cur.execute("""
            INSERT INTO wash_history (차량번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 업체, 세차완료일, 주행거리, 훼손, 경고등, 특이사항, 작업자)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (row["차량번호"], row["차종명"], row["차량소속"], row["스팟"], row["주소"], row["지역시도"], row["지역구군"], row["업체"], done_date,
              request.form.get("distance"), request.form.get("damage"), request.form.get("warning"), request.form.get("etc"), current_user.username))
        cur.execute("DELETE FROM wash_list WHERE id=?", (id,))
        conn.commit()
    
    conn.close()
    return redirect(url_for("wash_status"))

# =========================================================
# 세차 현황 및 엑셀 다운로드
# =========================================================
@app.route("/wash_status")
@login_required
def wash_status():
    conn = get_wash_db()
    query = "SELECT * FROM wash_history WHERE 1=1"
    params = []
    if current_user.role != "admin":
        query += " AND 업체=?"
        params.append(current_user.vendor)
    
    rows = conn.execute(query + " ORDER BY id DESC", params).fetchall()
    conn.close()
    return render_template("wash_status.html", rows=rows)

@app.route("/wash_status_excel")
@login_required
def wash_status_excel():
    conn = get_wash_db()
    df = pd.read_sql_query("SELECT * FROM wash_history", conn)
    conn.close()
    
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name="wash_status.xlsx")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

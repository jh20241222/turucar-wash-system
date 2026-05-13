import os
import sqlite3
from datetime import datetime
from io import BytesIO

import pandas as pd
from flask import (
    Flask, flash, jsonify, redirect, render_template,
    request, send_file, url_for
)
from flask_login import (
    LoginManager, UserMixin, current_user,
    login_required, login_user, logout_user
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = "turu_secret_key"

# =========================================================
# Vercel 배포를 위한 경로 설정 (절대 경로 사용)
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_DB_PATH = os.path.join(BASE_DIR, "db.sqlite3")
WASH_DB_PATH = os.path.join(BASE_DIR, "wash.db")
BAND_MATCHING_PATH = os.path.join(BASE_DIR, "차량소속별_밴드매칭.xlsx")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")

# 업로드 폴더 생성
if not os.path.exists(UPLOAD_DIR):
    try:
        os.makedirs(UPLOAD_DIR, exist_ok=True)
    except:
        pass

def load_band_mapping():
    if not os.path.exists(BAND_MATCHING_PATH):
        return {}
    try:
        df = pd.read_excel(BAND_MATCHING_PATH)
        required_cols = {"차량소속", "밴드링크"}
        if not required_cols.issubset(df.columns):
            return {}
        clean_df = df[["차량소속", "밴드링크"]].copy()
        clean_df["차량소속"] = clean_df["차량소속"].astype(str).str.strip()
        clean_df["밴드링크"] = clean_df["밴드링크"].astype(str).str.strip()
        clean_df = clean_df[(clean_df["차량소속"] != "") & (clean_df["밴드링크"] != "") & (clean_df["밴드링크"].str.lower() != "nan")]
        return dict(zip(clean_df["차량소속"], clean_df["밴드링크"]))
    except:
        return {}

# =========================================================
# DB 연결 함수
# =========================================================
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
    def __init__(self, id, username, role, vendor=None, parent_id=None):
        self.id = id
        self.username = username
        self.role = role
        self.vendor = vendor
        self.parent_id = parent_id

    @property
    def is_master(self):
        return self.role == "master"
    @property
    def is_admin(self):
        return self.role in ("master", "admin")
    @property
    def is_staff(self):
        return self.role == "staff"

@login_manager.user_loader
def load_user(user_id):
    conn = get_user_db()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM accounts WHERE id=?", (user_id,)).fetchone()
    conn.close()
    if row:
        return User(row["id"], row["username"], row["role"], row["vendor"], row["parent_id"])
    return None

# =========================================================
# 권한 및 필터 함수
# =========================================================
def scoped_condition(table_name, user):
    if user.is_master:
        return "", []
    clauses = [f"{table_name}.업체 = ?"]
    params = [user.vendor]
    if user.is_staff:
        conn = get_user_db()
        cur = conn.cursor()
        regions = cur.execute(
            "SELECT city, district FROM account_region WHERE username=?", (user.username,)
        ).fetchall()
        conn.close()
        if not regions:
            return " AND 1=0", params
        region_clause = " OR ".join([f"({table_name}.지역시도 = ? AND {table_name}.지역구군 = ?)"] * len(regions))
        clauses.append(f"({region_clause})")
        for region in regions:
            params.extend([region["city"], region["district"]])
    return " AND " + " AND ".join(clauses), params

def filter_distinct_values(cur, table_name, column_name, base_query, base_params):
    query = f"SELECT DISTINCT {column_name} AS value FROM {table_name} WHERE 1=1{base_query} ORDER BY {column_name}"
    rows = cur.execute(query, base_params).fetchall()
    return [r["value"] for r in rows if r["value"] not in (None, "", "None")]

def can_manage_target(target_row):
    if current_user.is_master: return True
    return (current_user.role == "admin" and target_row["role"] == "staff" and 
            target_row["parent_id"] == current_user.id and target_row["vendor"] == current_user.vendor)

# =========================================================
# 라우트 (Route)
# =========================================================
@app.route("/")
def index():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        pw = request.form.get("password", "")
        conn = get_user_db()
        cur = conn.cursor()
        user = cur.execute("SELECT * FROM accounts WHERE username=?", (username,)).fetchone()
        conn.close()
        if user and check_password_hash(user["password"], pw):
            login_user(User(user["id"], user["username"], user["role"], user["vendor"], user["parent_id"]))
            return redirect(url_for("dashboard"))
        flash("❌ 아이디 또는 비밀번호가 잘못되었습니다.")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

@app.route("/dashboard")
@login_required
def dashboard():
    conn = get_wash_db()
    cur = conn.cursor()
    sql, params = scoped_condition("wash_list", current_user)
    total = cur.execute(f"SELECT COUNT(*) AS c FROM wash_list WHERE 1=1{sql}", params).fetchone()["c"]
    sql_hist, params_hist = scoped_condition("wash_history", current_user)
    done = cur.execute(f"SELECT COUNT(*) AS c FROM wash_history WHERE 1=1{sql_hist}", params_hist).fetchone()["c"]
    vendors = cur.execute(f"SELECT 업체, COUNT(*) AS c FROM wash_list WHERE 1=1{sql} GROUP BY 업체", params).fetchall()
    conn.close()
    return render_template("dashboard.html", total_count=total, done_count=done, vendor_counts=vendors)

@app.route("/wash_list")
@login_required
def wash_list():
    conn = get_wash_db()
    cur = conn.cursor()
    today = datetime.today().strftime("%Y-%m-%d")
    date = request.args.get("date", today)
    query = "SELECT * FROM wash_list WHERE 세차일 = ?"
    params = [date]
    sql, p = scoped_condition("wash_list", current_user)
    query += sql
    params += p
    rows = cur.execute(query + " ORDER BY id DESC", params).fetchall()
    conn.close()
    return render_template("wash_list.html", rows=rows, selected_date=date)

@app.route("/car_detail/<int:id>")
@login_required
def car_detail(id):
    conn = get_wash_db()
    cur = conn.cursor()
    car = cur.execute("SELECT * FROM wash_list WHERE id=?", (id,)).fetchone()
    conn.close()
    if not car: return "차량 없음", 404
    return render_template("car_detail.html", car=car)

@app.route("/wash_complete/<int:id>", methods=["POST"])
@login_required
def wash_complete(id):
    conn = get_wash_db()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM wash_list WHERE id=?", (id,)).fetchone()
    if row:
        cur.execute("INSERT INTO wash_history (차량번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 업체, 세차완료일, 작업자, 원본ID) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (row["차량번호"], row["차종명"], row["차량소속"], row["스팟"], row["주소"], row["지역시도"], row["지역구군"], row["업체"], datetime.now().strftime("%Y-%m-%d"), current_user.username, id))
        cur.execute("DELETE FROM wash_list WHERE id=?", (id,))
        conn.commit()
    conn.close()
    return redirect(url_for("wash_status"))

@app.route("/wash_status")
@login_required
def wash_status():
    conn = get_wash_db()
    cur = conn.cursor()
    sql, params = scoped_condition("wash_history", current_user)
    rows = cur.execute(f"SELECT * FROM wash_history WHERE 1=1{sql} ORDER BY id DESC", params).fetchall()
    conn.close()
    return render_template("wash_status.html", rows=rows)

# Vercel은 __main__을 실행하지 않으므로 app 객체를 밖으로 노출
app = app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

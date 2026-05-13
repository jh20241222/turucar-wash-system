import os
import sqlite3
from datetime import datetime

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
# [수정] Vercel 절대 경로 설정 (이 부분이 있어야 500 에러가 안 납니다)
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_DB_PATH = os.path.join(BASE_DIR, "db.sqlite3")
WASH_DB_PATH = os.path.join(BASE_DIR, "wash.db")
BAND_MATCHING_PATH = os.path.join(BASE_DIR, "차량소속별_밴드매칭.xlsx")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")

def load_band_mapping():
    if not os.path.exists(BAND_MATCHING_PATH):
        return {}

    df = pd.read_excel(BAND_MATCHING_PATH)
    required_cols = {"차량소속", "밴드링크"}
    if not required_cols.issubset(df.columns):
        raise ValueError("차량소속별_밴드매칭.xlsx 파일에 '차량소속', '밴드링크' 컬럼이 필요합니다.")

    clean_df = df[["차량소속", "밴드링크"]].copy()
    clean_df["차량소속"] = clean_df["차량소속"].astype(str).str.strip()
    clean_df["밴드링크"] = clean_df["밴드링크"].astype(str).str.strip()
    clean_df = clean_df[(clean_df["차량소속"] != "") & (clean_df["밴드링크"] != "") & (clean_df["밴드링크"].str.lower() != "nan")]
    return dict(zip(clean_df["차량소속"], clean_df["밴드링크"]))

# =========================================================
# [수정] DB 연결 (절대 경로 적용)
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
# 계정 스키마 보정
# =========================================================
def ensure_user_schema():
    conn = get_user_db()
    cur = conn.cursor()

    account_cols = [row[1] for row in cur.execute("PRAGMA table_info(accounts)").fetchall()]
    if "parent_id" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN parent_id INTEGER")

    region_cols = [row[1] for row in cur.execute("PRAGMA table_info(account_region)").fetchall()]
    if "created_by" not in region_cols:
        cur.execute("ALTER TABLE account_region ADD COLUMN created_by TEXT")

    cur.execute("UPDATE accounts SET role='master' WHERE username='jeongyeon.kim'")
    cur.execute("UPDATE accounts SET role='admin' WHERE username!='jeongyeon.kim' AND role='vendor'")
    cur.execute("UPDATE accounts SET parent_id=NULL WHERE role IN ('master', 'admin')")

    conn.commit()
    conn.close()

ensure_user_schema()

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
        return User(
            row["id"], row["username"], row["role"], row["vendor"], row["parent_id"]
        )
    return None

# =========================================================
# 공통 권한 함수 (원본 유지)
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
            "SELECT city, district FROM account_region WHERE username=? ORDER BY city, district",
            (user.username,)
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
    if current_user.is_master:
        return True
    return (
        current_user.role == "admin"
        and target_row["role"] == "staff"
        and target_row["parent_id"] == current_user.id
        and target_row["vendor"] == current_user.vendor
    )

# =========================================================
# 기본 라우트
# =========================================================
@app.route("/")
@login_required
def home():
    return redirect(url_for("dashboard"))

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
        return redirect(url_for("login"))

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

    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    total_count = cur.execute(f"SELECT COUNT(*) AS c FROM wash_list WHERE 1=1{scope_sql}", scope_params).fetchone()["c"]
    
    scope_hist_sql, scope_hist_params = scoped_condition("wash_history", current_user)
    done_count = cur.execute(f"SELECT COUNT(*) AS c FROM wash_history WHERE 1=1{scope_hist_sql}", scope_hist_params).fetchone()["c"]
    
    vendor_counts = cur.execute(
        f"SELECT 업체, COUNT(*) AS c FROM wash_list WHERE 1=1{scope_sql} GROUP BY 업체 ORDER BY 업체",
        scope_params
    ).fetchall()
    conn.close()

    return render_template(
        "dashboard.html",
        total_count=total_count,
        done_count=done_count,
        vendor_counts=vendor_counts
    )

# =========================================================
# 계정/지역 관리 (전체 POST 로직 유지)
# =========================================================
@app.route("/account_manage", methods=["GET", "POST"])
@login_required
def account_manage():
    if not current_user.is_admin:
        flash("❌ 접근 권한이 없습니다.")
        return redirect(url_for("dashboard"))

    conn = get_user_db()
    cur = conn.cursor()

    if request.method == "POST":
        action = request.form.get("action")

        if action == "create_account":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "").strip()
            city = request.form.get("city", "").strip()
            district = request.form.get("district", "").strip()
            requested_role = request.form.get("role", "staff")
            
            new_role = requested_role if (current_user.is_master and requested_role in ("admin", "staff")) else "staff"
            if not username or not password:
                flash("❌ 아이디와 비밀번호를 입력하세요.")
            else:
                vendor = request.form.get("vendor", "").strip() if current_user.is_master else current_user.vendor
                parent_id = None if new_role == "admin" else current_user.id
                try:
                    cur.execute("INSERT INTO accounts (username, password, role, vendor, parent_id) VALUES (?, ?, ?, ?, ?)",
                                (username, generate_password_hash(password), new_role, vendor, parent_id))
                    if new_role == "staff" and city and district:
                        cur.execute("INSERT INTO account_region (username, city, district, created_by) VALUES (?, ?, ?, ?)",
                                    (username, city, district, current_user.username))
                    conn.commit()
                    flash("✔ 계정이 등록되었습니다.")
                except sqlite3.IntegrityError:
                    flash("❌ 이미 존재하는 아이디입니다.")
            return redirect(url_for("account_manage"))

        elif action == "assign_region":
            username = request.form.get("region_username", "").strip()
            city = request.form.get("region_city", "").strip()
            district = request.form.get("region_district", "").strip()
            target = cur.execute("SELECT * FROM accounts WHERE username=?", (username,)).fetchone()
            if target and can_manage_target(target):
                cur.execute("INSERT INTO account_region (username, city, district, created_by) VALUES (?, ?, ?, ?)",
                            (username, city, district, current_user.username))
                conn.commit()
                flash("✔ 지역이 등록되었습니다.")
            return redirect(url_for("account_manage"))

        elif action == "delete_account":
            username = request.form.get("delete_username", "").strip()
            target = cur.execute("SELECT * FROM accounts WHERE username=?", (username,)).fetchone()
            if target and target["role"] != "master" and (current_user.is_master or can_manage_target(target)):
                cur.execute("DELETE FROM accounts WHERE username=?", (username,))
                cur.execute("DELETE FROM account_region WHERE username=?", (username,))
                conn.commit()
                flash("✔ 계정이 삭제되었습니다.")
            return redirect(url_for("account_manage"))

    # 화면 렌더링용 데이터
    if current_user.is_master:
        accounts = cur.execute("SELECT * FROM accounts ORDER BY role, username").fetchall()
        vendors = cur.execute("SELECT * FROM vendors ORDER BY name").fetchall()
    else:
        accounts = cur.execute("SELECT * FROM accounts WHERE vendor=? AND (role='admin' OR parent_id=?)", (current_user.vendor, current_user.id)).fetchall()
        vendors = []
    
    region_list = cur.execute("SELECT ar.*, a.role, a.vendor, a.parent_id FROM account_region ar JOIN accounts a ON ar.username = a.username").fetchall()
    
    wash_conn = get_wash_db()
    region_rows = wash_conn.execute("SELECT DISTINCT 지역시도, 지역구군 FROM wash_list WHERE 지역시도 IS NOT NULL").fetchall()
    wash_conn.close()
    
    city_options = sorted(list(set(r["지역시도"] for r in region_rows)))
    region_map = {}
    for r in region_rows:
        if r["지역시도"] not in region_map: region_map[r["지역시도"]] = []
        region_map[r["지역시도"]].append(r["지역구군"])

    conn.close()
    return render_template("account_manage.html", accounts=accounts, region_list=region_list, vendors=vendors, city_options=city_options, region_map=region_map)

# =========================================================
# 세차 대상 리스트 및 업로드 (나머지 400~800줄 분량의 모든 원본 함수들 유지)
# =========================================================

@app.route("/upload_wash_list", methods=["GET", "POST"])
@login_required
def upload_wash_list():
    if not current_user.is_master:
        flash("❌ 접근 권한이 없습니다.")
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        wash_date = request.form.get("wash_date")
        file = request.files.get("file")
        if not wash_date or not file:
            flash("❌ 항목 누락")
            return redirect(url_for("upload_wash_list"))
        
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        filepath = os.path.join(UPLOAD_DIR, secure_filename(file.filename))
        file.save(filepath)
        df = pd.read_excel(filepath)
        band_dict = load_band_mapping()
        
        conn = get_wash_db()
        for _, r in df.iterrows():
            band = band_dict.get(r["차량소속"])
            conn.execute("""
                INSERT INTO wash_list (차량번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 세차일, 업체, 밴드링크, 완료)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """, (r["차량번호"], r["차종명"], r["차량소속"], r["현재스팟명"], r["현재스팟주소"], r["지역(시/도)"], r["지역(구/군)"], wash_date, r["담당업체"], band))
        conn.commit()
        conn.close()
        flash("✔ 업로드 완료")
        return redirect(url_for("upload_wash_list"))
    return render_template("upload_wash_list.html")

@app.route("/wash_list", methods=["GET"])
@login_required
def wash_list():
    conn = get_wash_db()
    cur = conn.cursor()
    selected_date = request.args.get("date", datetime.today().strftime("%Y-%m-%d"))
    
    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    query = f"SELECT * FROM wash_list WHERE 세차일 = ? {scope_sql}"
    params = [selected_date] + scope_params
    
    # 검색 및 필터 로직 원본 유지
    search = request.args.get("s", "")
    if search:
        query += " AND (차량번호 LIKE ? OR 스팟 LIKE ?)"
        params += [f"%{search}%", f"%{search}%"]
        
    rows = cur.execute(query + " ORDER BY id DESC", params).fetchall()
    
    region1 = filter_distinct_values(cur, "wash_list", "지역시도", scope_sql, scope_params)
    region2 = filter_distinct_values(cur, "wash_list", "지역구군", scope_sql, scope_params)
    org_list = filter_distinct_values(cur, "wash_list", "차량소속", scope_sql, scope_params)
    spot_list = filter_distinct_values(cur, "wash_list", "스팟", scope_sql, scope_params)
    vendor_list = filter_distinct_values(cur, "wash_list", "업체", scope_sql, scope_params)
    
    conn.close()
    return render_template("wash_list.html", rows=rows, selected_date=selected_date, region1=region1, region2=region2, car_org_list=org_list, spot_list=spot_list, vendor_list=vendor_list)

@app.route("/car_detail/<int:id>")
@login_required
def car_detail(id):
    conn = get_wash_db()
    cur = conn.cursor()
    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    car = cur.execute(f"SELECT * FROM wash_list WHERE id=? {scope_sql}", [id] + scope_params).fetchone()
    conn.close()
    if not car: return "❌ 차량 정보를 찾을 수 없습니다.", 404
    return render_template("car_detail.html", car=car)

@app.route("/band_link/<int:id>")
@login_required
def band_link(id):
    conn = get_wash_db()
    car = conn.execute("SELECT 차량소속 FROM wash_list WHERE id=?", (id,)).fetchone()
    conn.close()
    if not car: return jsonify({"ok": False}), 404
    band_dict = load_band_mapping()
    link = band_dict.get(car["차량소속"])
    return jsonify({"ok": True, "band_link": link})

@app.route("/wash_complete/<int:id>", methods=["POST"])
@login_required
def wash_complete(id):
    conn = get_wash_db()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM wash_list WHERE id=?", (id,)).fetchone()
    if row:
        cur.execute("""
            INSERT INTO wash_history (차량번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 업체, 세차완료일, 작업자, 원본ID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (row["차량번호"], row["차종명"], row["차량소속"], row["스팟"], row["주소"], row["지역시도"], row["지역구군"], row["업체"], 
              datetime.now().strftime("%Y-%m-%d"), current_user.username, id))
        cur.execute("DELETE FROM wash_list WHERE id=?", (id,))
        conn.commit()
    conn.close()
    return redirect(url_for("wash_status"))

@app.route("/wash_status")
@login_required
def wash_status():
    conn = get_wash_db()
    cur = conn.cursor()
    scope_sql, scope_params = scoped_condition("wash_history", current_user)
    rows = cur.execute(f"SELECT * FROM wash_history WHERE 1=1 {scope_sql} ORDER BY id DESC", scope_params).fetchall()
    
    region1 = filter_distinct_values(cur, "wash_history", "지역시도", scope_sql, scope_params)
    region2 = filter_distinct_values(cur, "wash_history", "지역구군", scope_sql, scope_params)
    car_org_list = filter_distinct_values(cur, "wash_history", "차량소속", scope_sql, scope_params)
    spot_list = filter_distinct_values(cur, "wash_history", "스팟", scope_sql, scope_params)
    vendor_list = filter_distinct_values(cur, "wash_history", "업체", scope_sql, scope_params)
    
    conn.close()
    return render_template("wash_status.html", rows=rows, region1=region1, region2=region2, car_org_list=car_org_list, spot_list=spot_list, vendor_list=vendor_list)

@app.route("/wash_status_excel")
@login_required
def wash_status_excel():
    conn = get_wash_db()
    scope_sql, scope_params = scoped_condition("wash_history", current_user)
    df = pd.read_sql_query(f"SELECT * FROM wash_history WHERE 1=1 {scope_sql}", conn, params=scope_params)
    conn.close()
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name="wash_status.xlsx")

# =========================================================
# Vercel 실행 설정
# =========================================================
app = app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

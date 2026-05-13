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


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_DB_PATH = os.path.join("/data", "db.sqlite3")
WASH_DB_PATH = os.path.join("/data", "wash.db")
BAND_MATCHING_PATH = os.path.join("/data", "차량소속별_밴드매칭.xlsx")
UPLOAD_DIR = os.path.join("/data", "uploads")

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
# DB 연결
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
def init_db():
    conn = get_user_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'staff',
            vendor TEXT,
            parent_id INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS account_region (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            city TEXT,
            district TEXT,
            created_by TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vendors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    """)
    conn.commit()
    conn.close()

    conn = get_wash_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wash_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            차량번호 TEXT, 차종명 TEXT, 차량소속 TEXT,
            스팟 TEXT, 주소 TEXT, 지역시도 TEXT, 지역구군 TEXT,
            세차일 TEXT, 업체 TEXT, 밴드링크 TEXT, 작업자 TEXT, 완료 INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wash_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            차량번호 TEXT, 차종명 TEXT, 차량소속 TEXT,
            스팟 TEXT, 주소 TEXT, 지역시도 TEXT, 지역구군 TEXT,
            업체 TEXT, 세차완료일 TEXT, 주행거리 TEXT,
            훼손 TEXT, 경고등 TEXT, 특이사항 TEXT, 작업자 TEXT, 원본ID INTEGER
        )
    """)
    conn.commit()
    conn.close()

def init_db():
    conn = get_user_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'staff',
            vendor TEXT,
            parent_id INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS account_region (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            city TEXT,
            district TEXT,
            created_by TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vendors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    """)
    existing = cur.execute("SELECT 1 FROM accounts WHERE username='jeongyeon.kim'").fetchone()
    if not existing:
        cur.execute(
            "INSERT INTO accounts (username, password, role) VALUES (?, ?, ?)",
            ("jeongyeon.kim", generate_password_hash("1111"), "master")
        )
    conn.commit()
    conn.close()

    conn = get_wash_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wash_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            차량번호 TEXT, 차종명 TEXT, 차량소속 TEXT,
            스팟 TEXT, 주소 TEXT, 지역시도 TEXT, 지역구군 TEXT,
            세차일 TEXT, 업체 TEXT, 밴드링크 TEXT, 작업자 TEXT, 완료 INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wash_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            차량번호 TEXT, 차종명 TEXT, 차량소속 TEXT,
            스팟 TEXT, 주소 TEXT, 지역시도 TEXT, 지역구군 TEXT,
            업체 TEXT, 세차완료일 TEXT, 주행거리 TEXT,
            훼손 TEXT, 경고등 TEXT, 특이사항 TEXT, 작업자 TEXT, 원본ID INTEGER
        )
    """)
    conn.commit()
    conn.close()

init_db()

init_db()
def ensure_user_schema():
    conn = get_user_db()
    cur = conn.cursor()

    account_cols = [row[1] for row in cur.execute("PRAGMA table_info(accounts)").fetchall()]
    if "parent_id" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN parent_id INTEGER")

    region_cols = [row[1] for row in cur.execute("PRAGMA table_info(account_region)").fetchall()]
    if "created_by" not in region_cols:
        cur.execute("ALTER TABLE account_region ADD COLUMN created_by TEXT")

    # 기존 역할 정리
    cur.execute("UPDATE accounts SET role='master' WHERE username='jeongyeon.kim'")
    cur.execute("UPDATE accounts SET role='admin' WHERE username!='jeongyeon.kim' AND role='vendor'")

    # 기존 admin 계정은 최상위로 유지
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
            row["id"],
            row["username"],
            row["role"],
            row["vendor"],
            row["parent_id"]
        )
    return None


# =========================================================
# 공통 권한 함수
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


# =========================================================
# 로그인
# =========================================================
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


# =========================================================
# 로그아웃
# =========================================================
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

    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    total_count = cur.execute(f"SELECT COUNT(*) AS c FROM wash_list WHERE 1=1{scope_sql}", scope_params).fetchone()["c"]
    done_count = cur.execute("SELECT COUNT(*) AS c FROM wash_history WHERE 1=1" + scoped_condition("wash_history", current_user)[0], scoped_condition("wash_history", current_user)[1]).fetchone()["c"]
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
# 계정/지역 관리
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
            if current_user.is_master and requested_role in ("admin", "staff"):
                new_role = requested_role
            else:
                new_role = "staff"

            if not username or not password:
                flash("❌ 아이디와 비밀번호를 입력하세요.")
                return redirect(url_for("account_manage"))

            vendor = request.form.get("vendor", "").strip() if current_user.is_master else current_user.vendor
            if new_role != "master" and not vendor:
                flash("❌ 업체 정보가 필요합니다.")
                return redirect(url_for("account_manage"))

            parent_id = None if new_role == "admin" else current_user.id

            try:
                cur.execute(
                    "INSERT INTO accounts (username, password, role, vendor, parent_id) VALUES (?, ?, ?, ?, ?)",
                    (username, generate_password_hash(password), new_role, vendor, parent_id)
                )
                if new_role == "staff" and city and district:
                    cur.execute(
                        "INSERT INTO account_region (username, city, district, created_by) VALUES (?, ?, ?, ?)",
                        (username, city, district, current_user.username)
                    )
                conn.commit()
                flash("✔ 계정이 등록되었습니다.")
            except sqlite3.IntegrityError:
                flash("❌ 이미 존재하는 아이디입니다.")

            return redirect(url_for("account_manage"))

        if action == "assign_region":
            username = request.form.get("region_username", "").strip()
            city = request.form.get("region_city", "").strip()
            district = request.form.get("region_district", "").strip()

            target = cur.execute("SELECT * FROM accounts WHERE username=?", (username,)).fetchone()
            if not target or not can_manage_target(target):
                flash("❌ 해당 계정에 지역을 지정할 권한이 없습니다.")
                return redirect(url_for("account_manage"))

            if not city or not district:
                flash("❌ 시/도와 구/군을 모두 선택하세요.")
                return redirect(url_for("account_manage"))

            exists = cur.execute(
                "SELECT 1 FROM account_region WHERE username=? AND city=? AND district=?",
                (username, city, district)
            ).fetchone()
            if exists:
                flash("ℹ 이미 등록된 지역입니다.")
            else:
                cur.execute(
                    "INSERT INTO account_region (username, city, district, created_by) VALUES (?, ?, ?, ?)",
                    (username, city, district, current_user.username)
                )
                conn.commit()
                flash("✔ 지역이 등록되었습니다.")

            return redirect(url_for("account_manage"))

        if action == "delete_account":
            username = request.form.get("delete_username", "").strip()
            target = cur.execute("SELECT * FROM accounts WHERE username=?", (username,)).fetchone()

            if not target:
                flash("❌ 계정을 찾을 수 없습니다.")
                return redirect(url_for("account_manage"))
            if target["role"] == "master":
                flash("❌ 마스터 계정은 삭제할 수 없습니다.")
                return redirect(url_for("account_manage"))

            allowed = False
            if current_user.is_master:
                allowed = target["role"] in ("admin", "staff")
            else:
                allowed = can_manage_target(target)

            if not allowed:
                flash("❌ 해당 계정을 삭제할 권한이 없습니다.")
                return redirect(url_for("account_manage"))

            child_rows = cur.execute("SELECT username FROM accounts WHERE parent_id=?", (target["id"],)).fetchall()
            child_usernames = [r["username"] for r in child_rows]
            if child_usernames:
                placeholders = ",".join(["?"] * len(child_usernames))
                cur.execute(f"DELETE FROM account_region WHERE username IN ({placeholders})", child_usernames)
                cur.execute(f"DELETE FROM accounts WHERE username IN ({placeholders})", child_usernames)

            cur.execute("DELETE FROM account_region WHERE username=?", (username,))
            cur.execute("DELETE FROM accounts WHERE username=?", (username,))
            conn.commit()
            flash("✔ 계정이 삭제되었습니다.")
            return redirect(url_for("account_manage"))

        if action == "delete_region":
            region_id = request.form.get("region_id", "").strip()
            region_row = cur.execute(
                """
                SELECT ar.id, ar.username, ar.city, ar.district, a.vendor, a.role, a.parent_id
                FROM account_region ar
                JOIN accounts a ON a.username = ar.username
                WHERE ar.id = ?
                """,
                (region_id,)
            ).fetchone()

            if not region_row:
                flash("❌ 지역 정보를 찾을 수 없습니다.")
                return redirect(url_for("account_manage"))

            if not can_manage_target(region_row) and not current_user.is_master:
                flash("❌ 해당 지역을 삭제할 권한이 없습니다.")
                return redirect(url_for("account_manage"))

            cur.execute("DELETE FROM account_region WHERE id=?", (region_id,))
            conn.commit()
            flash("✔ 지역이 삭제되었습니다.")
            return redirect(url_for("account_manage"))

    if current_user.is_master:
        accounts = cur.execute(
            "SELECT * FROM accounts ORDER BY CASE role WHEN 'master' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END, username"
        ).fetchall()
        creatable_accounts = cur.execute(
            "SELECT * FROM accounts WHERE role IN ('admin', 'staff') ORDER BY CASE role WHEN 'admin' THEN 0 ELSE 1 END, username"
        ).fetchall()
        vendors = cur.execute("SELECT * FROM vendors ORDER BY name").fetchall()
    else:
        accounts = cur.execute(
            "SELECT * FROM accounts WHERE vendor=? AND (role='admin' OR parent_id=?) ORDER BY CASE role WHEN 'admin' THEN 0 ELSE 1 END, username",
            (current_user.vendor, current_user.id)
        ).fetchall()
        creatable_accounts = cur.execute(
            "SELECT * FROM accounts WHERE parent_id=? ORDER BY username",
            (current_user.id,)
        ).fetchall()
        vendors = []

    region_list = cur.execute(
        """
        SELECT ar.id, ar.username, ar.city, ar.district, a.vendor, a.role, a.parent_id
        FROM account_region ar
        JOIN accounts a ON a.username = ar.username
        {where_clause}
        ORDER BY ar.username, ar.city, ar.district
        """.format(
            where_clause=""
            if current_user.is_master
            else "WHERE a.parent_id = ?"
        ),
        () if current_user.is_master else (current_user.id,)
    ).fetchall()

    wash_conn = get_wash_db()
    wash_cur = wash_conn.cursor()
    region_rows = wash_cur.execute(
        "SELECT DISTINCT 지역시도, 지역구군 FROM wash_list WHERE 지역시도 IS NOT NULL AND 지역구군 IS NOT NULL ORDER BY 지역시도, 지역구군"
    ).fetchall()
    wash_conn.close()

    city_options = []
    region_map = {}
    for row in region_rows:
        city = str(row["지역시도"]).strip()
        district = str(row["지역구군"]).strip()
        if not city or city.lower() == "none" or not district or district.lower() == "none":
            continue
        if city not in region_map:
            region_map[city] = []
            city_options.append(city)
        if district not in region_map[city]:
            region_map[city].append(district)

    conn.close()

    return render_template(
        "account_manage.html",
        accounts=accounts,
        region_list=region_list,
        vendors=vendors,
        creatable_accounts=creatable_accounts,
        city_options=city_options,
        region_map=region_map
    )


# =========================================================
# 세차 대상 업로드
# =========================================================
@app.route("/upload_wash_list", methods=["GET", "POST"])
@login_required
def upload_wash_list():
    if not current_user.is_master:
        flash("❌ 접근 권한이 없습니다.")
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        wash_date = request.form.get("wash_date")
        if not wash_date:
            flash("❌ 세차일자를 선택하세요.")
            return redirect(url_for("upload_wash_list"))

        file = request.files.get("file")
        if not file:
            flash("❌ 업로드할 파일을 선택하세요.")
            return redirect(url_for("upload_wash_list"))

        filename = secure_filename(file.filename)
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        filepath = os.path.join(UPLOAD_DIR, filename)
        file.save(filepath)

        df = pd.read_excel(filepath)
        required = [
            "차량번호", "차종명", "차량소속", "현재스팟명",
            "현재스팟주소", "지역(시/도)", "지역(구/군)", "담당업체"
        ]
        for col in required:
            if col not in df.columns:
                flash(f"❌ '{col}' 컬럼이 없습니다.")
                return redirect(url_for("upload_wash_list"))

        try:
            band_dict = load_band_mapping()
        except Exception as e:
            flash(f"❌ 밴드매칭 파일 오류: {e}")
            return redirect(url_for("upload_wash_list"))

        conn = get_wash_db()
        cur = conn.cursor()
        for _, r in df.iterrows():
            band = band_dict.get(r["차량소속"], None)
            cur.execute(
                """
                INSERT INTO wash_list
                (차량번호, 차종명, 차량소속, 스팟, 주소,
                 지역시도, 지역구군, 세차일,
                 업체, 밴드링크, 작업자, 완료)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    r["차량번호"], r["차종명"], r["차량소속"], r["현재스팟명"],
                    r["현재스팟주소"], r["지역(시/도)"], r["지역(구/군)"],
                    wash_date, r["담당업체"], band, None
                )
            )
        conn.commit()
        conn.close()

        flash("✔ 업로드 완료")
        return redirect(url_for("upload_wash_list"))

    return render_template("upload_wash_list.html")


# =========================================================
# 세차 대상 리스트
# =========================================================
@app.route("/wash_list", methods=["GET"])
@login_required
def wash_list():
    conn = get_wash_db()
    cur = conn.cursor()

    today = datetime.today().strftime("%Y-%m-%d")
    selected_date = request.args.get("date", today)

    query = "SELECT * FROM wash_list WHERE 세차일 = ?"
    params = [selected_date]

    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    query += scope_sql
    params += scope_params

    search = request.args.get("s", "")
    r1 = request.args.get("r1", "")
    r2 = request.args.get("r2", "")
    org = request.args.get("org", "")
    spot = request.args.get("spot", "")
    vendor = request.args.get("vendor", "")

    if search:
        query += " AND (차량번호 LIKE ? OR 스팟 LIKE ?)"
        params += [f"%{search}%", f"%{search}%"]
    if r1:
        query += " AND 지역시도 = ?"
        params.append(r1)
    if r2:
        query += " AND 지역구군 = ?"
        params.append(r2)
    if org:
        query += " AND 차량소속 = ?"
        params.append(org)
    if spot:
        query += " AND 스팟 = ?"
        params.append(spot)
    if vendor and current_user.is_master:
        query += " AND 업체 = ?"
        params.append(vendor)

    query += " ORDER BY id DESC"
    rows = cur.execute(query, params).fetchall()

    filter_scope_sql, filter_scope_params = scoped_condition("wash_list", current_user)
    region1 = filter_distinct_values(cur, "wash_list", "지역시도", filter_scope_sql, filter_scope_params)
    region2 = filter_distinct_values(cur, "wash_list", "지역구군", filter_scope_sql, filter_scope_params)
    org_list = filter_distinct_values(cur, "wash_list", "차량소속", filter_scope_sql, filter_scope_params)
    spot_list = filter_distinct_values(cur, "wash_list", "스팟", filter_scope_sql, filter_scope_params)
    vendor_list = filter_distinct_values(cur, "wash_list", "업체", filter_scope_sql, filter_scope_params)

    conn.close()

    return render_template(
        "wash_list.html",
        rows=rows,
        selected_date=selected_date,
        search_input=search,
        region1=region1,
        region2=region2,
        car_org_list=org_list,
        spot_list=spot_list,
        vendor_list=vendor_list,
        selected_r1=r1,
        selected_r2=r2,
        selected_org=org,
        selected_spot=spot,
        selected_vendor=vendor
    )


# =========================================================
# 차량 상세 입력 페이지
# =========================================================
@app.route("/car_detail/<int:id>")
@login_required
def car_detail(id):
    conn = get_wash_db()
    cur = conn.cursor()

    query = "SELECT * FROM wash_list WHERE id=?"
    params = [id]
    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    query += scope_sql
    params += scope_params

    car = cur.execute(query, params).fetchone()
    conn.close()

    if not car:
        return "❌ 차량 정보를 찾을 수 없습니다.", 404

    return render_template("car_detail.html", car=car)


# =========================================================
# 밴드 링크 조회
# =========================================================
@app.route("/band_link/<int:id>", methods=["GET"])
@login_required
def band_link(id):
    conn = get_wash_db()
    cur = conn.cursor()

    query = "SELECT * FROM wash_list WHERE id=?"
    params = [id]
    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    query += scope_sql
    params += scope_params

    car = cur.execute(query, params).fetchone()
    conn.close()

    if not car:
        return jsonify({"ok": False, "message": "차량 정보를 찾을 수 없습니다."}), 404

    try:
        band_dict = load_band_mapping()
    except Exception as e:
        return jsonify({"ok": False, "message": f"밴드매칭 파일 오류: {e}"}), 500

    car_org = str(car["차량소속"]).strip()
    band = band_dict.get(car_org)
    if not band:
        return jsonify({"ok": False, "message": f"'{car_org}' 차량소속의 밴드 링크가 없습니다."}), 404

    return jsonify({"ok": True, "band_link": band, "car_org": car_org})


# =========================================================
# 세차 완료 처리
# =========================================================
@app.route("/wash_complete/<int:id>", methods=["POST"])
@login_required
def wash_complete(id):
    conn = get_wash_db()
    cur = conn.cursor()

    query = "SELECT * FROM wash_list WHERE id=?"
    params = [id]
    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    query += scope_sql
    params += scope_params
    row = cur.execute(query, params).fetchone()

    if not row:
        conn.close()
        return "데이터 없음"

    done_date = datetime.now().strftime("%Y-%m-%d")
    cur.execute(
        """
        INSERT INTO wash_history
        (차량번호, 차종명, 차량소속, 스팟, 주소,
         지역시도, 지역구군, 업체, 세차완료일,
         주행거리, 훼손, 경고등, 특이사항, 작업자, 원본ID)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["차량번호"], row["차종명"], row["차량소속"], row["스팟"], row["주소"],
            row["지역시도"], row["지역구군"], row["업체"], done_date,
            request.form.get("distance"), request.form.get("damage"),
            request.form.get("warning"), request.form.get("etc"),
            current_user.username, id
        )
    )
    cur.execute("DELETE FROM wash_list WHERE id=?", (id,))
    conn.commit()
    conn.close()

    return redirect(url_for("wash_status"))


# =========================================================
# 세차 현황
# =========================================================
@app.route("/wash_status")
@login_required
def wash_status():
    s = request.args.get("s", "")
    r1 = request.args.get("r1", "")
    r2 = request.args.get("r2", "")
    org = request.args.get("org", "")
    sp = request.args.get("spot", "")
    vendor = request.args.get("vendor", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")

    conn = get_wash_db()
    cur = conn.cursor()

    query = "SELECT * FROM wash_history WHERE 1=1"
    params = []
    scope_sql, scope_params = scoped_condition("wash_history", current_user)
    query += scope_sql
    params += scope_params

    if s:
        query += " AND (차량번호 LIKE ? OR 스팟 LIKE ?)"
        params += [f"%{s}%", f"%{s}%"]
    if r1:
        query += " AND 지역시도=?"
        params.append(r1)
    if r2:
        query += " AND 지역구군=?"
        params.append(r2)
    if org:
        query += " AND 차량소속=?"
        params.append(org)
    if sp:
        query += " AND 스팟=?"
        params.append(sp)
    if vendor and current_user.is_master:
        query += " AND 업체=?"
        params.append(vendor)
    if start and end:
        query += " AND 세차완료일 BETWEEN ? AND ?"
        params += [start, end]

    query += " ORDER BY id DESC"
    rows = cur.execute(query, params).fetchall()

    region1 = filter_distinct_values(cur, "wash_history", "지역시도", scope_sql, scope_params)
    region2 = filter_distinct_values(cur, "wash_history", "지역구군", scope_sql, scope_params)
    car_org_list = filter_distinct_values(cur, "wash_history", "차량소속", scope_sql, scope_params)
    spot_list = filter_distinct_values(cur, "wash_history", "스팟", scope_sql, scope_params)
    vendor_list = filter_distinct_values(cur, "wash_history", "업체", scope_sql, scope_params)

    conn.close()

    return render_template(
        "wash_status.html",
        rows=rows,
        region1=region1,
        region2=region2,
        car_org_list=car_org_list,
        spot_list=spot_list,
        vendor_list=vendor_list,
        search_input=s,
        selected_r1=r1,
        selected_r2=r2,
        selected_org=org,
        selected_spot=sp,
        selected_vendor=vendor,
        start=start,
        end=end
    )


# =========================================================
# 세차 현황 엑셀 다운로드
# =========================================================
@app.route("/wash_status_excel")
@login_required
def wash_status_excel():
    from io import BytesIO

    s = request.args.get("s", "")
    r1 = request.args.get("r1", "")
    r2 = request.args.get("r2", "")
    org = request.args.get("org", "")
    sp = request.args.get("spot", "")
    vendor = request.args.get("vendor", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")

    conn = get_wash_db()
    query = "SELECT * FROM wash_history WHERE 1=1"
    params = []
    scope_sql, scope_params = scoped_condition("wash_history", current_user)
    query += scope_sql
    params += scope_params

    if s:
        query += " AND (차량번호 LIKE ? OR 스팟 LIKE ?)"
        params += [f"%{s}%", f"%{s}%"]
    if r1:
        query += " AND 지역시도=?"
        params.append(r1)
    if r2:
        query += " AND 지역구군=?"
        params.append(r2)
    if org:
        query += " AND 차량소속=?"
        params.append(org)
    if sp:
        query += " AND 스팟=?"
        params.append(sp)
    if vendor and current_user.is_master:
        query += " AND 업체=?"
        params.append(vendor)
    if start and end:
        query += " AND 세차완료일 BETWEEN ? AND ?"
        params += [start, end]

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="wash_status.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

app = app  # Vercel이 이 이름을 찾아서 실행해요

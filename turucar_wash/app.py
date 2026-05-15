import os
import shutil
import sqlite3
from datetime import datetime

import pandas as pd
from flask import (
    Flask, flash, jsonify, redirect, render_template,
    request, send_file, send_from_directory, url_for
)
from flask_login import (
    LoginManager, UserMixin, current_user,
    login_required, login_user, logout_user
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-this-secret-key")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _truthy(value):
    return str(value or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _running_on_railway():
    return any(os.environ.get(name) for name in (
        "RAILWAY_ENVIRONMENT",
        "RAILWAY_PROJECT_ID",
        "RAILWAY_SERVICE_ID",
        "RAILWAY_DEPLOYMENT_ID",
    ))


def _resolve_data_dir():
    """Return the only directory where mutable operating data is allowed to live.

    계정/지역, 업체, 세차 오더, 완료 현황, 업로드 파일은 모두 DATA_DIR 아래에만 저장한다.
    Railway에서는 반드시 Volume Mount Path와 DATA_DIR을 같은 경로로 맞춰야 한다.
    """
    explicit = os.environ.get("DATA_DIR")
    if explicit:
        return explicit

    railway_volume_path = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
    if railway_volume_path:
        return railway_volume_path

    # 로컬 개발은 기존처럼 프로젝트 내부 data 폴더를 사용한다.
    # Railway 운영에서는 DATA_DIR을 명시하지 않으면 아래 fail-safe가 앱 실행을 막는다.
    return os.path.join(BASE_DIR, "data")


DATA_DIR = os.path.abspath(_resolve_data_dir())
USER_DB_PATH = os.path.join(DATA_DIR, "db.sqlite3")
WASH_DB_PATH = os.path.join(DATA_DIR, "wash.db")
BAND_MATCHING_PATH = os.path.join(DATA_DIR, "차량소속별_밴드매칭.xlsx")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
STORAGE_MARKER_PATH = os.path.join(DATA_DIR, ".turu_wash_persistent_storage")

# Railway에서는 기본적으로 fail-safe를 켠다. DATA_DIR/Volume 설정이 없으면 앱을 시작하지 않는다.
PERSISTENCE_STRICT = _truthy(os.environ.get("PERSISTENCE_STRICT", "1" if _running_on_railway() else "0"))


def _validate_persistent_storage_config():
    """Fail closed rather than run on ephemeral storage in production.

    이 검사는 데이터 유실을 막기 위한 안전장치다. Railway에서 DATA_DIR이 명시되지 않은 채
    실행되면 재배포/슬립 후 재시작 시 SQLite 파일이 사라질 수 있으므로 앱 시작을 중단한다.
    """
    if not (_running_on_railway() and PERSISTENCE_STRICT):
        return

    has_explicit_data_dir = bool(os.environ.get("DATA_DIR") or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH"))
    if not has_explicit_data_dir:
        raise RuntimeError(
            "Persistent storage is not configured. "
            "Create a Railway Volume and set DATA_DIR to the volume mount path, e.g. DATA_DIR=/app/data. "
            "This app refuses to start to protect accounts, wash orders, completion history, and vendor data."
        )


def _write_storage_marker():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(STORAGE_MARKER_PATH):
        with open(STORAGE_MARKER_PATH, "w", encoding="utf-8") as f:
            f.write(f"created_at={datetime.now().isoformat(timespec='seconds')}\n")
            f.write(f"data_dir={DATA_DIR}\n")


def _backup_sqlite_file(path, label, keep=30):
    """Create a lightweight timestamped backup of an existing SQLite DB in DATA_DIR/backups."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return

    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"{label}-{timestamp}.sqlite3")
    shutil.copy2(path, backup_path)

    backups = sorted(
        [os.path.join(BACKUP_DIR, name) for name in os.listdir(BACKUP_DIR) if name.startswith(f"{label}-")],
        key=os.path.getmtime,
        reverse=True,
    )
    for old_backup in backups[keep:]:
        try:
            os.remove(old_backup)
        except OSError:
            pass


def backup_databases(reason="startup"):
    """Backup both operating DBs. Safe to call on startup and before destructive imports."""
    _backup_sqlite_file(USER_DB_PATH, f"user-db-{reason}")
    _backup_sqlite_file(WASH_DB_PATH, f"wash-db-{reason}")


def bootstrap_storage():
    """Create durable app storage and migrate legacy files into DATA_DIR without overwriting."""
    _validate_persistent_storage_config()

    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    _write_storage_marker()

    legacy_files = [
        (os.path.join(BASE_DIR, "wash.db"), WASH_DB_PATH),
        (os.path.join(BASE_DIR, "차량소속별_밴드매칭.xlsx"), BAND_MATCHING_PATH),
        (os.path.join(BASE_DIR, "#Ucc28#Ub7c9#Uc18c#Uc18d#Ubcc4_#Ubc34#Ub4dc#Ub9e4#Uce6d.xlsx"), BAND_MATCHING_PATH),
    ]
    for source, target in legacy_files:
        # Never overwrite live data. Legacy files are copied only for first boot of an empty DATA_DIR.
        if os.path.exists(source) and not os.path.exists(target):
            shutil.copy2(source, target)

    backup_databases("startup")


bootstrap_storage()


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
# DB 초기화 (테이블 생성 + 마스터 계정 생성)
# =========================================================
def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(UPLOAD_DIR, exist_ok=True)

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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dashboard_notices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            body TEXT,
            author TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    # 마스터 계정 없으면 자동 생성
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
# PWA 앱 설치 / 오프라인 지원
# =========================================================
@app.route("/offline")
def offline():
    return render_template("offline.html")


@app.route("/service-worker.js")
def service_worker():
    response = send_from_directory(
        os.path.join(BASE_DIR, "static"),
        "sw.js",
        mimetype="text/javascript"
    )
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Service-Worker-Allowed"] = "/"
    return response


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
# 내정보 / 앱 설정
# =========================================================

@app.route("/storage-status")
@login_required
def storage_status():
    if not current_user.is_master:
        flash("❌ 마스터 계정만 저장소 상태를 확인할 수 있습니다.")
        return redirect(url_for("dashboard"))

    def safe_count(db_path, table):
        if not os.path.exists(db_path):
            return None
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            value = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            conn.close()
            return value
        except Exception:
            return None

    payload = {
        "data_dir": DATA_DIR,
        "strict_mode": PERSISTENCE_STRICT,
        "running_on_railway": _running_on_railway(),
        "storage_marker_exists": os.path.exists(STORAGE_MARKER_PATH),
        "user_db_path": USER_DB_PATH,
        "wash_db_path": WASH_DB_PATH,
        "upload_dir": UPLOAD_DIR,
        "backup_dir": BACKUP_DIR,
        "counts": {
            "accounts": safe_count(USER_DB_PATH, "accounts"),
            "account_region": safe_count(USER_DB_PATH, "account_region"),
            "vendors": safe_count(USER_DB_PATH, "vendors"),
            "wash_list": safe_count(WASH_DB_PATH, "wash_list"),
            "wash_history": safe_count(WASH_DB_PATH, "wash_history"),
        },
        "files_exist": {
            "db.sqlite3": os.path.exists(USER_DB_PATH),
            "wash.db": os.path.exists(WASH_DB_PATH),
            "uploads": os.path.isdir(UPLOAD_DIR),
        },
    }
    return jsonify(payload)


@app.route("/profile")
@login_required
def profile():
    conn = get_user_db()
    cur = conn.cursor()
    region_rows = cur.execute(
        """
        SELECT city, district
        FROM account_region
        WHERE username=?
        ORDER BY city, district
        """,
        (current_user.username,)
    ).fetchall()

    child_count = 0
    if current_user.is_admin:
        child_count = cur.execute(
            "SELECT COUNT(*) AS c FROM accounts WHERE parent_id=?",
            (current_user.id,)
        ).fetchone()["c"]
    conn.close()

    return render_template(
        "profile.html",
        region_rows=region_rows,
        child_count=child_count,
    )


# =========================================================
# 앱 설정 / 공지사항
# =========================================================
def get_app_setting(key, default=""):
    conn = get_user_db()
    cur = conn.cursor()
    row = cur.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row and row["value"] is not None else default


def set_app_setting(key, value):
    conn = get_user_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )
    conn.commit()
    conn.close()



def create_dashboard_notice(title, body, author):
    conn = get_user_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dashboard_notices (title, body, author, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            title,
            body,
            author,
            datetime.now().strftime("%Y-%m-%d %H:%M")
        )
    )
    conn.commit()
    conn.close()


def get_dashboard_notices(page=1, per_page=10):
    page = max(int(page or 1), 1)
    per_page = max(int(per_page or 10), 1)
    offset = (page - 1) * per_page

    conn = get_user_db()
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) AS c FROM dashboard_notices").fetchone()["c"]
    rows = cur.execute(
        """
        SELECT id, title, body, author, created_at
        FROM dashboard_notices
        ORDER BY id DESC
        LIMIT ? OFFSET ?
        """,
        (per_page, offset)
    ).fetchall()
    conn.close()

    total_pages = max((total + per_page - 1) // per_page, 1)
    if page > total_pages:
        page = total_pages

    return rows, total, page, total_pages



def get_dashboard_notice_by_id(notice_id):
    conn = get_user_db()
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT id, title, body, author, created_at
        FROM dashboard_notices
        WHERE id=?
        """,
        (notice_id,)
    ).fetchone()
    conn.close()
    return row


def update_dashboard_notice_item(notice_id, title, body, author):
    conn = get_user_db()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE dashboard_notices
        SET title=?, body=?, author=?
        WHERE id=?
        """,
        (title, body, author, notice_id)
    )
    conn.commit()
    conn.close()


def delete_dashboard_notice_item(notice_id):
    conn = get_user_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM dashboard_notices WHERE id=?", (notice_id,))
    conn.commit()
    conn.close()


@app.route("/dashboard/notice", methods=["POST"])
@login_required
def update_dashboard_notice():
    if not current_user.is_master:
        flash("❌ 마스터 계정만 공지사항을 수정할 수 있습니다.")
        return redirect(url_for("dashboard"))

    notice_title = request.form.get("notice_title", "").strip() or "공지사항"
    notice_body = request.form.get("notice_body", "").strip() or "공지사항 내용을 입력해주세요."
    notice_author = request.form.get("notice_author", "").strip() or "투루카 담당자"

    set_app_setting("dashboard_notice_title", notice_title)
    set_app_setting("dashboard_notice_body", notice_body)
    create_dashboard_notice(notice_title, notice_body, notice_author)

    flash("공지사항이 저장되었습니다.")
    return redirect(url_for("dashboard"))



@app.route("/dashboard/notice/<int:notice_id>/edit", methods=["POST"])
@login_required
def edit_dashboard_notice(notice_id):
    if not current_user.is_master:
        flash("❌ 마스터 계정만 공지사항을 수정할 수 있습니다.")
        return redirect(url_for("dashboard"))

    notice_title = request.form.get("notice_title", "").strip() or "공지사항"
    notice_body = request.form.get("notice_body", "").strip() or "공지사항 내용을 입력해주세요."
    notice_author = request.form.get("notice_author", "").strip() or "투루카 담당자"

    update_dashboard_notice_item(notice_id, notice_title, notice_body, notice_author)
    flash("공지사항이 수정되었습니다.")
    page = request.form.get("notice_page", 1)
    return redirect((url_for("notices", notice_page=page) if request.form.get("return_to") == "notices" else url_for("dashboard") + "#notice-list"))


@app.route("/dashboard/notice/<int:notice_id>/delete", methods=["POST"])
@login_required
def delete_dashboard_notice(notice_id):
    if not current_user.is_master:
        flash("❌ 마스터 계정만 공지사항을 삭제할 수 있습니다.")
        return redirect(url_for("dashboard"))

    delete_dashboard_notice_item(notice_id)
    flash("공지사항이 삭제되었습니다.")
    page = request.form.get("notice_page", 1)
    return redirect((url_for("notices", notice_page=page) if request.form.get("return_to") == "notices" else url_for("dashboard") + "#notice-list"))




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

    notice_title = get_app_setting("dashboard_notice_title", "오늘의 세차관리")
    notice_body = get_app_setting(
        "dashboard_notice_body",
        f"{current_user.username} 계정으로 접속 중입니다. 오더 확인, 완료 처리까지 앱처럼 빠르게 확인하세요."
    )

    notice_rows, notice_total, _, _ = get_dashboard_notices(1, 3)

    return render_template(
        "dashboard.html",
        total_count=total_count,
        done_count=done_count,
        vendor_counts=vendor_counts,
        notice_title=notice_title,
        notice_body=notice_body,
        notice_rows=notice_rows,
        notice_total=notice_total,
        notice_page=1,
        notice_total_pages=1,
    )



@app.route("/notices")
@login_required
def notices():
    notice_page = request.args.get("notice_page", 1, type=int)
    notice_rows, notice_total, notice_page, notice_total_pages = get_dashboard_notices(notice_page, 10)
    return render_template(
        "notices.html",
        notice_rows=notice_rows,
        notice_total=notice_total,
        notice_page=notice_page,
        notice_total_pages=notice_total_pages,
    )


# =========================================================
# 업체 관리 (마스터 전용)
# =========================================================
@app.route("/vendor_manage", methods=["GET", "POST"])
@login_required
def vendor_manage():
    if not current_user.is_master:
        flash("❌ 접근 권한이 없습니다.")
        return redirect(url_for("dashboard"))

    conn = get_user_db()
    cur = conn.cursor()

    if request.method == "POST":
        action = request.form.get("action")

        if action == "create_vendor":
            name = request.form.get("name", "").strip()
            if not name:
                flash("❌ 업체명을 입력하세요.")
                return redirect(url_for("vendor_manage"))
            try:
                cur.execute("INSERT INTO vendors (name) VALUES (?)", (name,))
                conn.commit()
                flash("✔ 업체가 등록되었습니다.")
            except sqlite3.IntegrityError:
                flash("❌ 이미 존재하는 업체명입니다.")
            return redirect(url_for("vendor_manage"))

        if action == "delete_vendor":
            vendor_id = request.form.get("vendor_id", "").strip()
            cur.execute("DELETE FROM vendors WHERE id=?", (vendor_id,))
            conn.commit()
            flash("✔ 업체가 삭제되었습니다.")
            return redirect(url_for("vendor_manage"))

    vendors = cur.execute("SELECT * FROM vendors ORDER BY name").fetchall()
    conn.close()

    return render_template("vendor_manage.html", vendors=vendors)


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

app = app

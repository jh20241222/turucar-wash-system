import os
import shutil
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

def now_kst():
    """현재 KST 시각 반환."""
    return datetime.now(KST)

def today_kst():
    """오늘 KST 날짜 문자열 반환 (YYYY-MM-DD)."""
    return datetime.now(KST).strftime("%Y-%m-%d")

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
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
        [os.path.join(BACKUP_DIR, name) for name in os.listdir(BACKUP_DIR) if name.startswith(f"{label}-") and os.path.exists(os.path.join(BACKUP_DIR, name))],
        key=lambda p: os.path.getmtime(p) if os.path.exists(p) else 0,
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

print(f"[TuruWash] DATA_DIR = {DATA_DIR}")
print(f"[TuruWash] WASH_DB  = {WASH_DB_PATH}")


def load_band_mapping():
    """차량소속별_밴드매칭.xlsx를 읽어 (차량소속, 담당업체) 복합키 딕셔너리로 반환."""
    if not os.path.exists(BAND_MATCHING_PATH):
        return {}

    df = pd.read_excel(BAND_MATCHING_PATH)
    if "차량소속" not in df.columns or "밴드링크" not in df.columns:
        raise ValueError("차량소속별_밴드매칭.xlsx 파일에 '차량소속', '밴드링크' 컬럼이 필요합니다.")

    has_vendor_col = "담당업체" in df.columns
    df["차량소속"] = df["차량소속"].astype(str).str.strip()
    df["밴드링크"] = df["밴드링크"].astype(str).str.strip()
    if has_vendor_col:
        df["담당업체"] = df["담당업체"].astype(str).str.strip().replace("nan", "")
    else:
        df["담당업체"] = ""

    df = df[(df["차량소속"] != "") & (df["밴드링크"] != "") & (df["밴드링크"].str.lower() != "nan")]

    mapping = {}
    for _, row in df.iterrows():
        vendor = str(row["담당업체"]).strip() if str(row["담당업체"]).strip().lower() not in ("nan", "") else ""
        mapping[(row["차량소속"], vendor)] = row["밴드링크"]
    return mapping


def find_band_link(band_dict, car_org, vendor=""):
    """복합키(차량소속+담당업체) 우선, 없으면 차량소속 단독으로 폴백."""
    car_org = str(car_org).strip() if car_org and not isinstance(car_org, float) else ""
    vendor = str(vendor).strip() if vendor and not isinstance(vendor, float) else ""
    # 1순위: 차량소속 + 담당업체 정확히 일치
    link = band_dict.get((car_org, vendor))
    if link:
        return link
    # 2순위: 담당업체 없는 단순 키
    link = band_dict.get((car_org, ""))
    if link:
        return link
    # 3순위: 차량소속만 일치하는 첫 번째 항목
    for (org, _), url in band_dict.items():
        if org == car_org:
            return url
    return None


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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS support_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            car_number TEXT NOT NULL,
            message TEXT NOT NULL,
            requester TEXT NOT NULL,
            requester_role TEXT,
            vendor TEXT,
            status TEXT NOT NULL DEFAULT '접수',
            admin_reply TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id INTEGER NOT NULL,
            sender TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(ticket_id) REFERENCES support_tickets(id)
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
            세차일 TEXT, 업체 TEXT, 밴드링크 TEXT, 작업자 TEXT, 완료 INTEGER DEFAULT 0,
            등록일 TEXT, 이월횟수 INTEGER DEFAULT 0, 세차경과일 INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wash_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            차량번호 TEXT, 차종명 TEXT, 차량소속 TEXT,
            스팟 TEXT, 주소 TEXT, 지역시도 TEXT, 지역구군 TEXT,
            업체 TEXT, 세차완료일 TEXT, 주행거리 TEXT,
            훼손 TEXT, 경고등 TEXT, 특이사항 TEXT, 작업자 TEXT, 원본ID INTEGER,
            상태 TEXT DEFAULT '완료'
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            차량번호 TEXT UNIQUE NOT NULL,
            차대번호 TEXT,
            차종명 TEXT,
            차량소속 TEXT,
            스팟 TEXT,
            주소 TEXT,
            지역시도 TEXT,
            지역구군 TEXT,
            담당업체 TEXT,
            최근세차일 TEXT,
            세차경과일 INTEGER DEFAULT 0,
            updated_at TEXT
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
# 세차 오더 스키마 보정
# =========================================================
def ensure_wash_schema():
    conn = get_wash_db()
    cur = conn.cursor()
    try:
        wash_cols = [row[1] for row in cur.execute("PRAGMA table_info(wash_list)").fetchall()]
        if "등록일" not in wash_cols:
            cur.execute("ALTER TABLE wash_list ADD COLUMN 등록일 TEXT")
            cur.execute("UPDATE wash_list SET 등록일 = 세차일 WHERE 등록일 IS NULL")
            print("[TuruWash] wash_list.등록일 컬럼 추가됨")
        if "이월횟수" not in wash_cols:
            cur.execute("ALTER TABLE wash_list ADD COLUMN 이월횟수 INTEGER DEFAULT 0")
            cur.execute("UPDATE wash_list SET 이월횟수 = 0 WHERE 이월횟수 IS NULL")
            print("[TuruWash] wash_list.이월횟수 컬럼 추가됨")
        if "세차경과일" not in wash_cols:
            cur.execute("ALTER TABLE wash_list ADD COLUMN 세차경과일 INTEGER DEFAULT 0")
            cur.execute("UPDATE wash_list SET 세차경과일 = 0 WHERE 세차경과일 IS NULL")
            print("[TuruWash] wash_list.세차경과일 컬럼 추가됨")

        hist_cols = [row[1] for row in cur.execute("PRAGMA table_info(wash_history)").fetchall()]
        if "상태" not in hist_cols:
            cur.execute("ALTER TABLE wash_history ADD COLUMN 상태 TEXT DEFAULT '완료'")
            print("[TuruWash] wash_history.상태 컬럼 추가됨")
        if "원본ID" not in hist_cols:
            cur.execute("ALTER TABLE wash_history ADD COLUMN 원본ID INTEGER")
            print("[TuruWash] wash_history.원본ID 컬럼 추가됨")

        conn.commit()
        print("[TuruWash] ensure_wash_schema 완료")
    except Exception as e:
        print(f"[TuruWash] ensure_wash_schema 오류: {e}")
        conn.rollback()
    finally:
        conn.close()


ensure_wash_schema()


# =========================================================
# 미완료 오더 이월 처리 (월~금: 세차일 < 오늘 → 오늘로 이월)
# =========================================================
def rollover_wash_orders():
    """세차일이 오늘보다 과거인 미완료 오더를 오늘 날짜로 이월. 토요일은 이월 없음(리셋에서 처리)."""
    today = now_kst()
    if today.weekday() == 5:
        return
    today_str = today.strftime("%Y-%m-%d")
    conn = get_wash_db()
    cur = conn.cursor()
    try:
        # 이월횟수 컬럼 존재 여부 확인 후 분기
        wash_cols = [row[1] for row in cur.execute("PRAGMA table_info(wash_list)").fetchall()]
        if "이월횟수" in wash_cols:
            cur.execute("""
                UPDATE wash_list
                SET 세차일 = ?,
                    이월횟수 = COALESCE(이월횟수, 0) + 1
                WHERE 세차일 < ? AND 완료 = 0
            """, (today_str, today_str))
        else:
            cur.execute("""
                UPDATE wash_list SET 세차일 = ?
                WHERE 세차일 < ? AND 완료 = 0
            """, (today_str, today_str))
        affected = cur.rowcount
        conn.commit()
        print(f"[TuruWash] 이월 완료 — {affected}건 → {today_str}")
    except Exception as e:
        print(f"[TuruWash] rollover 오류: {e}")
        conn.rollback()
    finally:
        conn.close()


# =========================================================
# 토요일 자정 리셋 (미완료 오더 전체 삭제)
# =========================================================
def saturday_reset():
    """토요일에 앱 시작 시 실행. 세차일이 오늘(토) 이전인 미완료 오더를 전부 삭제한다."""
    today = now_kst()
    if today.weekday() != 5:
        return
    today_str = today.strftime("%Y-%m-%d")
    conn = get_wash_db()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM wash_list WHERE 세차일 < ? AND 완료 = 0", (today_str,))
        affected = cur.rowcount
        conn.commit()
        print(f"[TuruWash] 토요일 리셋 완료 — 미완료 오더 {affected}건 삭제됨")
    except Exception as e:
        print(f"[TuruWash] saturday_reset 오류: {e}")
        conn.rollback()
    finally:
        conn.close()


def run_daily_once():
    """앱 시작 시 오늘 날짜 기준으로 이월/리셋을 딱 한 번만 실행."""
    today_str = today_kst()
    last_run = get_app_setting("last_rollover_date", "")
    if last_run == today_str:
        print(f"[TuruWash] 오늘({today_str}) 이월/리셋 이미 실행됨 — 스킵")
        return
    saturday_reset()
    rollover_wash_orders()
    set_app_setting("last_rollover_date", today_str)
    print(f"[TuruWash] 이월/리셋 실행 완료 — {today_str}")


# =========================================================
# APScheduler: 자정 자동 이월 / 토요일 리셋
# =========================================================
def scheduled_daily_job():
    """매일 00:00 KST에 실행. 토요일이면 리셋, 나머지 요일이면 이월."""
    saturday_reset()
    rollover_wash_orders()
    set_app_setting("last_rollover_date", today_kst())
    print(f"[TuruWash] 스케줄러 실행 완료 — {now_kst().strftime('%Y-%m-%d %H:%M:%S')}")


_scheduler = BackgroundScheduler(timezone="Asia/Seoul")
_scheduler.add_job(scheduled_daily_job, "cron", hour=0, minute=0)
_scheduler.start()
print("[TuruWash] APScheduler 시작 — 매일 00:00 KST 이월/리셋 자동 실행")


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


def can_manage_support(user):
    return bool(user and (getattr(user, 'is_master', False) or getattr(user, 'username', '') == 'jeongyeon.kim'))


def get_support_ticket_total_count():
    if not current_user.is_authenticated or not can_manage_support(current_user):
        return 0
    conn = get_user_db()
    try:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM support_tickets").fetchone()
        return int(row["cnt"] if row else 0)
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


@app.context_processor
def inject_support_badge_count():
    try:
        count = get_support_ticket_total_count()
    except Exception:
        count = 0
    return {"support_badge_count": count}


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

    # 비밀번호 초기화 대상 계정 (admin: 본인 소속 staff, master: 모든 계정)
    reset_targets = []
    if current_user.is_master:
        reset_targets = cur.execute(
            "SELECT username, role, vendor FROM accounts WHERE username != ? ORDER BY role, username",
            (current_user.username,)
        ).fetchall()
    elif current_user.is_admin:
        reset_targets = cur.execute(
            "SELECT username, role, vendor FROM accounts WHERE parent_id=? ORDER BY username",
            (current_user.id,)
        ).fetchall()

    conn.close()

    # 담당 지역 기준 차량 리스트 (staff/admin 모두)
    assigned_vehicles = []
    if region_rows and not current_user.is_master:
        wash_conn = get_wash_db()
        wash_cur = wash_conn.cursor()
        region_clauses = " OR ".join(
            ["(지역시도 = ? AND 지역구군 = ?)"] * len(region_rows)
        )
        region_params = []
        for r in region_rows:
            region_params.extend([r["city"], r["district"]])
        vendor_param = [current_user.vendor] if current_user.vendor else []
        vendor_clause = " AND 업체 = ?" if current_user.vendor else ""
        query = f"""
            SELECT 차량번호, 차종명, 차량소속, 스팟, 지역시도, 지역구군, 업체, 세차일, 세차경과일
            FROM wash_list
            WHERE ({region_clauses}){vendor_clause}
            GROUP BY 차량번호
            ORDER BY 세차경과일 DESC, 차량번호
        """
        assigned_vehicles = wash_cur.execute(query, region_params + vendor_param).fetchall()
        wash_conn.close()

    return render_template(
        "profile.html",
        region_rows=region_rows,
        child_count=child_count,
        reset_targets=reset_targets,
        assigned_vehicles=assigned_vehicles,
    )


# =========================================================
# 내 담당 차량 현황
# =========================================================
@app.route("/my_vehicles")
@login_required
def my_vehicles():
    if current_user.is_master:
        flash("❌ 담당자/관리자 계정만 접근할 수 있습니다.")
        return redirect(url_for("dashboard"))

    conn = get_user_db()
    cur = conn.cursor()
    region_rows = cur.execute(
        "SELECT city, district FROM account_region WHERE username=? ORDER BY city, district",
        (current_user.username,)
    ).fetchall()
    conn.close()

    vehicles = []
    region_stats = []

    if region_rows:
        wash_conn = get_wash_db()
        wash_cur = wash_conn.cursor()

        region_clauses = " OR ".join(["(지역시도 = ? AND 지역구군 = ?)"] * len(region_rows))
        region_params = []
        for r in region_rows:
            region_params.extend([r["city"], r["district"]])

        vendor_clause = " AND 담당업체 = ?" if current_user.vendor else ""
        vendor_param = [current_user.vendor] if current_user.vendor else []

        vehicles = wash_cur.execute(f"""
            SELECT 차량번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 담당업체, 최근세차일, 세차경과일
            FROM vehicle_master
            WHERE ({region_clauses}){vendor_clause}
            ORDER BY 세차경과일 DESC, 차량번호
        """, region_params + vendor_param).fetchall()

        for r in region_rows:
            rows = [v for v in vehicles if v["지역시도"] == r["city"] and v["지역구군"] == r["district"]]
            urgent = [v for v in rows if (v["세차경과일"] or 0) >= 14]
            region_stats.append({
                "city": r["city"],
                "district": r["district"],
                "total": len(rows),
                "urgent": len(urgent),
                "regular": len(rows) - len(urgent),
            })

        wash_conn.close()

    total = len(vehicles)
    urgent_count = sum(1 for v in vehicles if (v["세차경과일"] or 0) >= 14)
    regular_count = total - urgent_count

    vehicles_list = [dict(v) for v in vehicles]

    return render_template(
        "my_vehicles.html",
        region_rows=region_rows,
        vehicles=vehicles_list,
        region_stats=region_stats,
        total=total,
        urgent_count=urgent_count,
        regular_count=regular_count,
    )


# =========================================================
# 본인 비밀번호 변경
# =========================================================
@app.route("/profile/change_password", methods=["POST"])
@login_required
def change_password():
    current_pw = request.form.get("current_password", "")
    new_pw = request.form.get("new_password", "").strip()
    confirm_pw = request.form.get("confirm_password", "").strip()

    conn = get_user_db()
    cur = conn.cursor()
    user = cur.execute("SELECT * FROM accounts WHERE id=?", (current_user.id,)).fetchone()

    if not check_password_hash(user["password"], current_pw):
        flash("❌ 현재 비밀번호가 일치하지 않습니다.")
        conn.close()
        return redirect(url_for("profile"))

    if not new_pw:
        flash("❌ 새 비밀번호를 입력하세요.")
        conn.close()
        return redirect(url_for("profile"))

    if new_pw != confirm_pw:
        flash("❌ 새 비밀번호가 일치하지 않습니다.")
        conn.close()
        return redirect(url_for("profile"))

    cur.execute("UPDATE accounts SET password=? WHERE id=?", (generate_password_hash(new_pw), current_user.id))
    conn.commit()
    conn.close()
    flash("✔ 비밀번호가 변경되었습니다.")
    return redirect(url_for("profile"))


# =========================================================
# 계정 비밀번호 초기화 (admin: 소속 staff, master: 모든 계정)
# =========================================================
@app.route("/profile/reset_password", methods=["POST"])
@login_required
def reset_password():
    if not current_user.is_admin:
        flash("❌ 접근 권한이 없습니다.")
        return redirect(url_for("profile"))

    target_username = request.form.get("target_username", "").strip()
    if not target_username:
        flash("❌ 초기화할 계정을 선택하세요.")
        return redirect(url_for("profile"))

    RESET_PW = "0325"

    conn = get_user_db()
    cur = conn.cursor()
    target = cur.execute("SELECT * FROM accounts WHERE username=?", (target_username,)).fetchone()

    if not target:
        flash("❌ 계정을 찾을 수 없습니다.")
        conn.close()
        return redirect(url_for("profile"))

    # master는 모든 계정 초기화 가능, admin은 본인 소속 staff만
    if not current_user.is_master:
        if target["role"] != "staff" or target["parent_id"] != current_user.id:
            flash("❌ 해당 계정의 비밀번호를 초기화할 권한이 없습니다.")
            conn.close()
            return redirect(url_for("profile"))

    if target["role"] == "master":
        flash("❌ 마스터 계정은 초기화할 수 없습니다.")
        conn.close()
        return redirect(url_for("profile"))

    cur.execute("UPDATE accounts SET password=? WHERE username=?", (generate_password_hash(RESET_PW), target_username))
    conn.commit()
    conn.close()
    flash(f"✔ {target_username} 비밀번호가 {RESET_PW}(으)로 초기화되었습니다.")
    return redirect(url_for("profile"))


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


# get_app_setting/set_app_setting 정의 이후 실행 — 순서 중요
run_daily_once()



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
    if not can_manage_support(current_user):
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
    if not can_manage_support(current_user):
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
    if not can_manage_support(current_user):
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
    today = today_kst()
    conn = get_wash_db()
    cur = conn.cursor()

    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    total_count = cur.execute(
        f"SELECT COUNT(*) AS c FROM wash_list WHERE 세차일 = ? AND 완료 = 0{scope_sql}",
        [today] + scope_params
    ).fetchone()["c"]
    done_count = cur.execute(
        "SELECT COUNT(*) AS c FROM wash_history WHERE 세차완료일 = ?" + scoped_condition("wash_history", current_user)[0],
        [today] + scoped_condition("wash_history", current_user)[1]
    ).fetchone()["c"]
    vendor_counts = cur.execute(
        f"SELECT 업체, COUNT(*) AS c FROM wash_list WHERE 세차일 = ? AND 완료 = 0{scope_sql} GROUP BY 업체 ORDER BY 업체",
        [today] + scope_params
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

    # 전국 고정 시/도 + 구/군 데이터 (세차 오더 업로드 없이도 지역 배정 가능)
    KOREA_REGIONS = {
        "서울특별시": [
            "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구",
            "노원구","도봉구","동대문구","동작구","마포구","서대문구","서초구",
            "성동구","성북구","송파구","양천구","영등포구","용산구","은평구",
            "종로구","중구","중랑구"
        ],
        "부산광역시": [
            "강서구","금정구","기장군","남구","동구","동래구","부산진구","북구",
            "사상구","사하구","서구","수영구","연제구","영도구","중구","해운대구"
        ],
        "대구광역시": [
            "군위군","남구","달서구","달성군","동구","북구","서구","수성구","중구"
        ],
        "인천광역시": [
            "강화군","계양구","남동구","동구","미추홀구","부평구","서구","연수구",
            "옹진군","중구"
        ],
        "광주광역시": ["광산구","남구","동구","북구","서구"],
        "대전광역시": ["대덕구","동구","서구","유성구","중구"],
        "울산광역시": ["남구","동구","북구","울주군","중구"],
        "세종특별자치시": ["세종시"],
        "경기도": [
            "가평군","고양시","과천시","광명시","광주시","구리시","군포시","김포시",
            "남양주시","동두천시","부천시","성남시","수원시","시흥시","안산시",
            "안성시","안양시","양주시","양평군","여주시","연천군","오산시","용인시",
            "의왕시","의정부시","이천시","파주시","평택시","포천시","하남시","화성시"
        ],
        "강원도": [
            "강릉시","고성군","동해시","삼척시","속초시","양구군","양양군",
            "영월군","원주시","인제군","정선군","철원군","춘천시","태백시",
            "평창군","홍천군","화천군","횡성군"
        ],
        "충청북도": [
            "괴산군","단양군","보은군","영동군","옥천군","음성군","제천시",
            "증평군","진천군","청주시","충주시"
        ],
        "충청남도": [
            "계룡시","공주시","금산군","논산시","당진시","보령시","부여군",
            "서산시","서천군","아산시","예산군","천안시",
            "청양군","태안군","홍성군"
        ],
        "전라북도": [
            "고창군","군산시","김제시","남원시","무주군","부안군","순창군",
            "완주군","익산시","임실군","장수군","전주시 덕진구","전주시 완산구",
            "정읍시","진안군"
        ],
        "전라남도": [
            "강진군","고흥군","곡성군","광양시","구례군","나주시","담양군",
            "목포시","무안군","보성군","순천시","신안군","여수시","영광군",
            "영암군","완도군","장성군","장흥군","진도군","함평군","해남군","화순군"
        ],
        "경상북도": [
            "경산시","경주시","고령군","구미시","김천시","문경시","봉화군",
            "상주시","성주군","안동시","영덕군","영양군","영주시","영천시",
            "예천군","울릉군","울진군","의성군","청도군","청송군","칠곡군","포항시"
        ],
        "경상남도": [
            "거제시","거창군","고성군","김해시","남해군","밀양시","사천시",
            "산청군","양산시","의령군","진주시","창녕군","창원시",
            "통영시","하동군","함안군","함양군","합천군"
        ],
        "제주특별자치도": ["서귀포시","제주시"],
    }

    city_options = list(KOREA_REGIONS.keys())
    region_map = KOREA_REGIONS

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
# 차량 마스터 업로드 (마스터 전용)
# =========================================================
@app.route("/upload_vehicle_master", methods=["POST"])
@login_required
def upload_vehicle_master():
    if not current_user.is_master:
        flash("❌ 마스터 계정만 업로드할 수 있습니다.")
        return redirect(url_for("upload_wash_list"))

    file = request.files.get("vehicle_file")
    if not file or not file.filename.endswith(".xlsx"):
        flash("❌ .xlsx 파일을 선택하세요.")
        return redirect(url_for("upload_wash_list"))

    try:
        df = pd.read_excel(file)
        df.columns = df.columns.str.strip()

        required = ["차량번호", "차종명", "차량소속"]
        for col in required:
            if col not in df.columns:
                flash(f"❌ '{col}' 컬럼이 없습니다.")
                return redirect(url_for("upload_wash_list"))

        today_str = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        conn = get_wash_db()
        cur = conn.cursor()

        inserted = 0
        updated = 0
        for _, r in df.iterrows():
            차량번호 = str(r["차량번호"]).strip()
            if not 차량번호 or 차량번호.lower() == "nan":
                continue
            # 스팟/지역 없는 행 스킵
            스팟체크 = str(r.get("현재스팟명", "")).strip()
            if not 스팟체크 or 스팟체크.lower() == "nan":
                continue

            차대번호 = str(r.get("차대번호", "")).strip() or None
            차종명 = str(r.get("차종명", "")).strip() or None
            차량소속 = str(r.get("차량소속", "")).strip() or None
            스팟 = str(r.get("현재스팟명", "")).strip() or None
            주소 = str(r.get("현재스팟주소", "")).strip() or None
            지역시도 = str(r.get("지역(시/도)", "")).strip() or None
            지역구군 = str(r.get("지역(구/군)", "")).strip() or None
            담당업체_raw = r.get("담당업체", None)
            담당업체 = str(담당업체_raw).strip() if 담당업체_raw and str(담당업체_raw).strip().lower() not in ("nan", "") else None

            최근세차일_raw = r.get("최근세차일", None) or r.get("세차일", None)
            최근세차일 = None
            if 최근세차일_raw and str(최근세차일_raw).strip().lower() not in ("nan", ""):
                최근세차일 = str(최근세차일_raw).strip()

            세차경과일_raw = r.get("세차경과일", 0)
            try:
                세차경과일 = int(float(세차경과일_raw)) if 세차경과일_raw and str(세차경과일_raw).lower() != "nan" else 0
            except:
                세차경과일 = 0

            existing = cur.execute("SELECT id FROM vehicle_master WHERE 차량번호=?", (차량번호,)).fetchone()
            if existing:
                cur.execute("""
                    UPDATE vehicle_master
                    SET 차대번호=?, 차종명=?, 차량소속=?, 스팟=?, 주소=?,
                        지역시도=?, 지역구군=?, 담당업체=?, 최근세차일=?, 세차경과일=?, updated_at=?
                    WHERE 차량번호=?
                """, (차대번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 담당업체, 최근세차일, 세차경과일, today_str, 차량번호))
                updated += 1
            else:
                cur.execute("""
                    INSERT INTO vehicle_master
                    (차량번호, 차대번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 담당업체, 최근세차일, 세차경과일, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (차량번호, 차대번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 담당업체, 최근세차일, 세차경과일, today_str))
                inserted += 1

        conn.commit()
        conn.close()
        flash(f"✔ 차량 마스터 업데이트 완료 — 신규 {inserted}대 / 업데이트 {updated}대")
    except Exception as e:
        flash(f"❌ 업로드 실패: {e}")

    return redirect(url_for("upload_wash_list"))


# =========================================================
# 밴드매칭 파일 업로드 (마스터 전용)
# =========================================================
@app.route("/upload_band_matching", methods=["POST"])
@login_required
def upload_band_matching():
    if not current_user.is_master:
        return jsonify({"ok": False, "message": "마스터 계정만 업로드할 수 있습니다."}), 403

    file = request.files.get("file")
    if not file or not file.filename.endswith(".xlsx"):
        flash("❌ .xlsx 파일을 선택하세요.")
        return redirect(url_for("upload_wash_list"))

    try:
        df = pd.read_excel(file)
        if "차량소속" not in df.columns or "밴드링크" not in df.columns:
            flash("❌ '차량소속', '밴드링크' 컬럼이 필요합니다.")
            return redirect(url_for("upload_wash_list"))
        os.makedirs(DATA_DIR, exist_ok=True)
        file.seek(0)
        file.save(BAND_MATCHING_PATH)
        flash(f"✔ 밴드매칭 파일이 업데이트되었습니다. ({len(df)}개 항목)")
    except Exception as e:
        flash(f"❌ 업로드 실패: {e}")

    return redirect(url_for("upload_wash_list"))


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

        # 밴드링크: 엑셀 컬럼 우선, 없으면 밴드매칭 파일에서 조회
        has_band_col = "밴드링크" in df.columns
        band_dict = {}
        if not has_band_col:
            try:
                band_dict = load_band_mapping()
            except Exception as e:
                flash(f"❌ 밴드매칭 파일 오류: {e}")
                return redirect(url_for("upload_wash_list"))

        has_elapsed_col = "세차경과일" in df.columns
        today_str = today_kst()
        conn = get_wash_db()
        cur = conn.cursor()
        inserted = 0
        skipped = 0
        for _, r in df.iterrows():
            # 밴드링크 결정
            if has_band_col:
                band_val = str(r["밴드링크"]).strip()
                band = band_val if band_val and band_val.lower() not in ("nan", "") else None
            else:
                band = find_band_link(band_dict, r["차량소속"], r.get("담당업체", ""))

            # 세차경과일 저장
            if has_elapsed_col:
                try:
                    elapsed_days = int(r["세차경과일"])
                except (ValueError, TypeError):
                    elapsed_days = 0
            else:
                elapsed_days = 0

            차량번호 = str(r["차량번호"]).strip()

            # 같은 날짜에 같은 차량번호가 미완료로 이미 있으면 정보 업데이트 (이월된 오더 포함)
            existing = cur.execute(
                "SELECT id FROM wash_list WHERE 차량번호=? AND 세차일=? AND 완료=0",
                (차량번호, wash_date)
            ).fetchone()
            if existing:
                cur.execute(
                    """
                    UPDATE wash_list
                    SET 차종명=?, 차량소속=?, 스팟=?, 주소=?,
                        지역시도=?, 지역구군=?, 업체=?, 밴드링크=?, 세차경과일=?
                    WHERE 차량번호=? AND 세차일=?
                    """,
                    (
                        r["차종명"], r["차량소속"], r["현재스팟명"],
                        r["현재스팟주소"], r["지역(시/도)"], r["지역(구/군)"],
                        r["담당업체"], band, elapsed_days,
                        차량번호, wash_date
                    )
                )
                skipped += 1
            else:
                cur.execute(
                    """
                    INSERT INTO wash_list
                    (차량번호, 차종명, 차량소속, 스팟, 주소,
                     지역시도, 지역구군, 세차일,
                     업체, 밴드링크, 작업자, 완료, 등록일, 이월횟수, 세차경과일)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, ?)
                    """,
                    (
                        차량번호, r["차종명"], r["차량소속"], r["현재스팟명"],
                        r["현재스팟주소"], r["지역(시/도)"], r["지역(구/군)"],
                        wash_date, r["담당업체"], band, None, today_str, elapsed_days
                    )
                )
                inserted += 1
        conn.commit()
        conn.close()

        if skipped:
            flash(f"✔ 업로드 완료 — {inserted}건 신규등록, {skipped}건 정보 업데이트")
        else:
            flash(f"✔ 업로드 완료 — {inserted}건 등록")
        return redirect(url_for("upload_wash_list"))

    # 날짜 목록 조회 (삭제 UI용)
    conn = get_wash_db()
    date_list = conn.execute(
        "SELECT 세차일, COUNT(*) AS cnt FROM wash_list WHERE 완료=0 GROUP BY 세차일 ORDER BY 세차일 DESC"
    ).fetchall()
    total_count = conn.execute("SELECT COUNT(*) AS c FROM wash_list WHERE 완료=0").fetchone()["c"]
    conn.close()

    return render_template("upload_wash_list.html", date_list=date_list, total_count=total_count)


# =========================================================
# 기존 오더 중복 제거 (마스터 전용)
# =========================================================
@app.route("/wash_deduplicate", methods=["POST"])
@login_required
def wash_deduplicate():
    if not current_user.is_master:
        flash("❌ 마스터 계정만 실행할 수 있습니다.")
        return redirect(url_for("upload_wash_list"))

    conn = get_wash_db()
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM wash_list
        WHERE 완료 = 0
        AND id NOT IN (
            SELECT MIN(id)
            FROM wash_list
            WHERE 완료 = 0
            GROUP BY 차량번호, 세차일
        )
    """)
    deleted = cur.rowcount
    conn.commit()
    conn.close()

    flash(f"✔ 중복 오더 {deleted}건 삭제 완료")
    return redirect(url_for("upload_wash_list"))


@app.route("/wash_force_rollover", methods=["POST"])
@login_required
def wash_force_rollover():
    """과거 날짜로 밀린 미완료 오더를 오늘로 강제 이월 (마스터 전용)."""
    if not current_user.is_master:
        flash("❌ 마스터 계정만 실행할 수 있습니다.")
        return redirect(url_for("upload_wash_list"))

    today_str = today_kst()
    conn = get_wash_db()
    cur = conn.cursor()
    try:
        wash_cols = [row[1] for row in cur.execute("PRAGMA table_info(wash_list)").fetchall()]
        if "이월횟수" in wash_cols:
            cur.execute("""
                UPDATE wash_list SET 세차일=?, 이월횟수=COALESCE(이월횟수,0)+1
                WHERE 세차일 < ? AND 완료=0
            """, (today_str, today_str))
        else:
            cur.execute("UPDATE wash_list SET 세차일=? WHERE 세차일 < ? AND 완료=0", (today_str, today_str))
        affected = cur.rowcount
        conn.commit()
        flash(f"✔ 밀린 오더 {affected}건을 오늘({today_str})로 이월했습니다.")
    except Exception as e:
        conn.rollback()
        flash(f"❌ 이월 오류: {e}")
    finally:
        conn.close()

    return redirect(url_for("upload_wash_list"))


# =========================================================
# 세차 스케줄 삭제 (날짜별 or 전체)
# =========================================================
@app.route("/wash_schedule_delete", methods=["POST"])
@login_required
def wash_schedule_delete():
    if not current_user.is_master:
        flash("❌ 마스터 계정만 삭제할 수 있습니다.")
        return redirect(url_for("upload_wash_list"))

    delete_type = request.form.get("delete_type")
    conn = get_wash_db()

    if delete_type == "all":
        conn.execute("DELETE FROM wash_list")
        conn.commit()
        conn.close()
        flash("✔ 전체 세차 오더가 삭제되었습니다.")
    elif delete_type == "date":
        target_date = request.form.get("target_date", "").strip()
        if not target_date:
            flash("❌ 삭제할 날짜를 선택하세요.")
            conn.close()
            return redirect(url_for("upload_wash_list"))
        conn.execute("DELETE FROM wash_list WHERE 세차일 = ?", (target_date,))
        conn.commit()
        conn.close()
        flash(f"✔ {target_date} 오더가 삭제되었습니다.")
    else:
        conn.close()
        flash("❌ 올바른 삭제 방식을 선택하세요.")

    return redirect(url_for("upload_wash_list"))


# =========================================================
# 세차 대상 리스트
# =========================================================
@app.route("/wash_list", methods=["GET"])
@login_required
def wash_list():
    conn = get_wash_db()
    cur = conn.cursor()

    today = today_kst()
    selected_date = request.args.get("date", today)

    query = "SELECT * FROM wash_list WHERE 세차일 = ? AND 완료 = 0"
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
        query += " AND (차량번호 LIKE ? OR 스팟 LIKE ? OR 차량소속 LIKE ?)"
        params += [f"%{search}%", f"%{search}%", f"%{search}%"]
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

    query += " ORDER BY 세차경과일 DESC, 이월횟수 DESC, id DESC"
    rows = cur.execute(query, params).fetchall()

    # 세차경과일 컬럼 기준으로 장기/정기 분류
    LONG_WASH_DAYS = 14

    rows_with_days = []
    for r in rows:
        elapsed = r["세차경과일"] or 0
        rows_with_days.append({"row": r, "elapsed": elapsed})

    long_wash_rows = [x for x in rows_with_days if x["elapsed"] >= LONG_WASH_DAYS]
    regular_rows = [x for x in rows_with_days if x["elapsed"] < LONG_WASH_DAYS]

    filter_scope_sql, filter_scope_params = scoped_condition("wash_list", current_user)
    region1 = filter_distinct_values(cur, "wash_list", "지역시도", filter_scope_sql, filter_scope_params)
    region2 = filter_distinct_values(cur, "wash_list", "지역구군", filter_scope_sql, filter_scope_params)
    org_list = filter_distinct_values(cur, "wash_list", "차량소속", filter_scope_sql, filter_scope_params)
    spot_list = filter_distinct_values(cur, "wash_list", "스팟", filter_scope_sql, filter_scope_params)
    vendor_list = filter_distinct_values(cur, "wash_list", "업체", filter_scope_sql, filter_scope_params)

    order_count = len(rows)
    history_scope_sql, history_scope_params = scoped_condition("wash_history", current_user)
    completed_count = cur.execute(
        "SELECT COUNT(*) AS c FROM wash_history WHERE 세차완료일 = ?" + history_scope_sql,
        [selected_date] + history_scope_params
    ).fetchone()["c"]
    total_target_count = order_count + completed_count

    conn.close()

    KOREA_REGIONS = {
        "서울특별시": ["강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구","동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구","영등포구","용산구","은평구","종로구","중구","중랑구"],
        "부산광역시": ["강서구","금정구","기장군","남구","동구","동래구","부산진구","북구","사상구","사하구","서구","수영구","연제구","영도구","중구","해운대구"],
        "대구광역시": ["군위군","남구","달서구","달성군","동구","북구","서구","수성구","중구"],
        "인천광역시": ["강화군","계양구","남동구","동구","미추홀구","부평구","서구","연수구","옹진군","중구"],
        "광주광역시": ["광산구","남구","동구","북구","서구"],
        "대전광역시": ["대덕구","동구","서구","유성구","중구"],
        "울산광역시": ["남구","동구","북구","울주군","중구"],
        "세종특별자치시": ["세종시"],
        "경기도": ["가평군","고양시","과천시","광명시","광주시","구리시","군포시","김포시","남양주시","동두천시","부천시","성남시","수원시","시흥시","안산시","안성시","안양시","양주시","양평군","여주시","연천군","오산시","용인시","의왕시","의정부시","이천시","파주시","평택시","포천시","하남시","화성시"],
        "강원도": ["강릉시","고성군","동해시","삼척시","속초시","양구군","양양군","영월군","원주시","인제군","정선군","철원군","춘천시","태백시","평창군","홍천군","화천군","횡성군"],
        "충청북도": ["괴산군","단양군","보은군","영동군","옥천군","음성군","제천시","증평군","진천군","청주시","충주시"],
        "충청남도": ["계룡시","공주시","금산군","논산시","당진시","보령시","부여군","서산시","서천군","아산시","예산군","천안시","청양군","태안군","홍성군"],
        "전라북도": ["고창군","군산시","김제시","남원시","무주군","부안군","순창군","완주군","익산시","임실군","장수군","전주시","정읍시","진안군"],
        "전라남도": ["강진군","고흥군","곡성군","광양시","구례군","나주시","담양군","목포시","무안군","보성군","순천시","신안군","여수시","영광군","영암군","완도군","장성군","장흥군","진도군","함평군","해남군","화순군"],
        "경상북도": ["경산시","경주시","고령군","구미시","김천시","문경시","봉화군","상주시","성주군","안동시","영덕군","영양군","영주시","영천시","예천군","울릉군","울진군","의성군","청도군","청송군","칠곡군","포항시"],
        "경상남도": ["거제시","거창군","고성군","김해시","남해군","밀양시","사천시","산청군","양산시","의령군","진주시","창녕군","창원시","통영시","하동군","함안군","함양군","합천군"],
        "제주특별자치도": ["서귀포시","제주시"],
    }

    return render_template(
        "wash_list.html",
        rows=rows,
        long_wash_rows=long_wash_rows,
        regular_rows=regular_rows,
        long_wash_count=len(long_wash_rows),
        regular_count=len(regular_rows),
        selected_date=selected_date,
        search_input=search,
        region1=region1,
        region2=region2,
        region_map=KOREA_REGIONS,
        car_org_list=org_list,
        spot_list=spot_list,
        vendor_list=vendor_list,
        selected_r1=r1,
        selected_r2=r2,
        selected_org=org,
        selected_spot=spot,
        selected_vendor=vendor,
        order_count=order_count,
        completed_count=completed_count,
        total_target_count=total_target_count
    )



# =========================================================
# 세차 오더 엑셀 다운로드
# =========================================================
@app.route("/wash_list_excel")
@login_required
def wash_list_excel():
    from io import BytesIO

    today = today_kst()
    selected_date = request.args.get("date", today)
    search = request.args.get("s", "")
    r1 = request.args.get("r1", "")
    r2 = request.args.get("r2", "")
    org = request.args.get("org", "")
    spot = request.args.get("spot", "")
    vendor = request.args.get("vendor", "")

    conn = get_wash_db()
    query = "SELECT * FROM wash_list WHERE 세차일 = ? AND 완료 = 0"
    params = [selected_date]

    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    query += scope_sql
    params += scope_params

    if search:
        query += " AND (차량번호 LIKE ? OR 스팟 LIKE ? OR 차량소속 LIKE ?)"
        params += [f"%{search}%", f"%{search}%", f"%{search}%"]
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

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    preferred_cols = [
        "id", "차량번호", "차종명", "차량소속", "스팟", "주소",
        "지역시도", "지역구군", "업체", "세차일"
    ]
    existing_cols = [col for col in preferred_cols if col in df.columns]
    extra_cols = [col for col in df.columns if col not in existing_cols]
    if existing_cols:
        df = df[existing_cols + extra_cols]

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="세차오더")
        worksheet = writer.sheets["세차오더"]
        for column_cells in worksheet.columns:
            max_length = 10
            column_letter = column_cells[0].column_letter
            for cell in column_cells:
                value = "" if cell.value is None else str(cell.value)
                max_length = max(max_length, min(len(value) + 2, 40))
            worksheet.column_dimensions[column_letter].width = max_length

    output.seek(0)
    filename = f"wash_orders_{selected_date}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
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

    elapsed = car["세차경과일"] or 0
    is_long_wash = elapsed >= 14

    return render_template("car_detail.html", car=car, elapsed=elapsed, is_long_wash=is_long_wash)


# =========================================================
# 밴드 링크 조회
# =========================================================
@app.route("/car_history")
@login_required
def car_history():
    """차량번호로 세차 수행 기록(wash_history) 조회 — JSON 반환"""
    from flask import jsonify
    car_num = request.args.get("car_num", "").strip()
    if not car_num:
        return jsonify({"rows": []})
    conn = get_wash_db()
    rows = conn.execute(
        "SELECT 세차완료일, 주행거리, 훼손, 경고등, 특이사항, 작업자 FROM wash_history WHERE 차량번호=? ORDER BY 세차완료일 DESC LIMIT 50",
        (car_num,)
    ).fetchall()
    conn.close()
    return jsonify({"rows": [dict(r) for r in rows]})


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
    vendor = str(car["업체"] or "").strip()
    band = find_band_link(band_dict, car_org, vendor)
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

    query = "SELECT * FROM wash_list WHERE id=? AND 완료=0"
    params = [id]
    scope_sql, scope_params = scoped_condition("wash_list", current_user)
    query += scope_sql
    params += scope_params
    row = cur.execute(query, params).fetchone()

    if not row:
        conn.close()
        flash("❌ 이미 완료 처리됐거나 존재하지 않는 오더입니다.")
        return redirect(url_for("wash_list"))

    done_date = today_kst()
    try:
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
        cur.execute("DELETE FROM wash_list WHERE id=? AND 완료=0", (id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f"❌ 완료 처리 오류: {e}")
        return redirect(url_for("wash_list"))

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
    today_str = today_kst()
    selected_date = request.args.get("date", today_str)

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
    else:
        # 날짜 네비게이터 기준 단일 날짜 필터
        query += " AND 세차완료일=?"
        params.append(selected_date)

    query += " ORDER BY id DESC"
    rows = cur.execute(query, params).fetchall()

    region1 = filter_distinct_values(cur, "wash_history", "지역시도", scope_sql, scope_params)
    region2 = filter_distinct_values(cur, "wash_history", "지역구군", scope_sql, scope_params)
    car_org_list = filter_distinct_values(cur, "wash_history", "차량소속", scope_sql, scope_params)
    spot_list = filter_distinct_values(cur, "wash_history", "스팟", scope_sql, scope_params)
    vendor_list = filter_distinct_values(cur, "wash_history", "업체", scope_sql, scope_params)

    today_completed_count = cur.execute(
        "SELECT COUNT(*) AS c FROM wash_history WHERE 세차완료일 = ?" + scope_sql,
        [today_str] + scope_params
    ).fetchone()["c"]
    selected_date_count = cur.execute(
        "SELECT COUNT(*) AS c FROM wash_history WHERE 세차완료일 = ?" + scope_sql,
        [selected_date] + scope_params
    ).fetchone()["c"]
    total_completed_count = cur.execute(
        "SELECT COUNT(*) AS c FROM wash_history WHERE 1=1" + scope_sql,
        scope_params
    ).fetchone()["c"]
    filtered_count = len(rows)

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
        end=end,
        today=today_str,
        selected_date=selected_date,
        today_completed_count=today_completed_count,
        selected_date_count=selected_date_count,
        total_completed_count=total_completed_count,
        filtered_count=filtered_count
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




# =========================================================
# Master delete actions
# =========================================================
@app.route("/wash_list/delete", methods=["POST"])
@login_required
def wash_list_delete():
    if not current_user.is_master:
        flash("❌ 마스터 계정만 세차 오더를 삭제할 수 있습니다.")
        return redirect(url_for("wash_list"))

    ids = request.form.getlist("ids")
    return_query = request.form.get("return_query", "")

    if not ids:
        flash("삭제할 세차 오더를 선택해주세요.")
        return redirect(url_for("wash_list") + (f"?{return_query}" if return_query else ""))

    placeholders = ",".join(["?"] * len(ids))
    conn = get_wash_db()
    conn.execute(f"DELETE FROM wash_list WHERE id IN ({placeholders})", ids)
    conn.commit()
    conn.close()

    flash(f"세차 오더 {len(ids)}건이 삭제되었습니다.")
    return redirect(url_for("wash_list") + (f"?{return_query}" if return_query else ""))


@app.route("/wash_status/delete", methods=["POST"])
@login_required
def wash_status_delete():
    if not current_user.is_master:
        flash("❌ 마스터 계정만 완료 이력을 삭제할 수 있습니다.")
        return redirect(url_for("wash_status"))

    ids = request.form.getlist("ids")
    return_query = request.form.get("return_query", "")

    if not ids:
        flash("삭제할 완료 이력을 선택해주세요.")
        return redirect(url_for("wash_status") + (f"?{return_query}" if return_query else ""))

    placeholders = ",".join(["?"] * len(ids))
    conn = get_wash_db()
    conn.execute(f"DELETE FROM wash_history WHERE id IN ({placeholders})", ids)
    conn.commit()
    conn.close()

    flash(f"완료 이력 {len(ids)}건이 삭제되었습니다.")
    return redirect(url_for("wash_status") + (f"?{return_query}" if return_query else ""))


@app.route("/support-manage/<int:ticket_id>/delete", methods=["POST"])
@login_required
def support_delete(ticket_id):
    if not current_user.is_master:
        flash("❌ 마스터 계정만 문의를 삭제할 수 있습니다.")
        return redirect(url_for("support_manage"))

    conn = get_user_db()
    conn.execute("DELETE FROM support_messages WHERE ticket_id=?", (ticket_id,))
    conn.execute("DELETE FROM support_tickets WHERE id=?", (ticket_id,))
    conn.commit()
    conn.close()

    flash("문의 내역이 삭제되었습니다.")
    return redirect(url_for("support_manage"))



# =========================================================
# 문의봇 / 문의 관리
# =========================================================
@app.route("/support-chat")
@login_required
def support_chat():
    conn = get_user_db()
    rows = conn.execute(
        """
        SELECT *
        FROM support_tickets
        WHERE requester=?
        ORDER BY id DESC
        LIMIT 10
        """,
        (current_user.username,)
    ).fetchall()
    conn.close()
    return render_template("support_chat.html", tickets=rows)


@app.route("/support-chat/submit", methods=["POST"])
@login_required
def support_chat_submit():
    data = request.get_json(silent=True) or request.form

    category = (data.get("category") or "").strip()
    message = (data.get("message") or "").strip()
    ticket_id = data.get("ticket_id")

    if not category or not message:
        return jsonify({"ok": False, "message": "문의유형과 메시지를 입력해주세요."}), 400

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    conn = get_user_db()
    cur = conn.cursor()

    if ticket_id:
        ticket = cur.execute(
            "SELECT * FROM support_tickets WHERE id=? AND requester=?",
            (ticket_id, current_user.username)
        ).fetchone()

        if not ticket:
            conn.close()
            return jsonify({"ok": False, "message": "문의 내역을 찾을 수 없습니다."}), 404

        existing_message = ticket["message"] or ""
        updated_message = (existing_message + "\n\n" if existing_message else "") + f"[작업자] {message}"
        cur.execute(
            """
            UPDATE support_tickets
            SET message=?, status=CASE WHEN status='완료' THEN '접수' ELSE status END, updated_at=?
            WHERE id=?
            """,
            (updated_message, now, ticket_id)
        )
        cur.execute(
            """
            INSERT INTO support_messages (ticket_id, sender, message, created_at)
            VALUES (?, 'worker', ?, ?)
            """,
            (ticket_id, message, now)
        )
        conn.commit()
        conn.close()

        return jsonify({
            "ok": True,
            "ticket_id": int(ticket_id),
            "message": "메시지가 전달되었습니다."
        })

    # First free-chat message creates ticket.
    car_number = (data.get("car_number") or "").strip()
    if not car_number:
        # Free chat mode: car number is optional; keep a visible placeholder for manager.
        car_number = "미입력"

    cur.execute(
        """
        INSERT INTO support_tickets
            (category, car_number, message, requester, requester_role, vendor, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, '접수', ?, ?)
        """,
        (
            category,
            car_number,
            f"[작업자] {message}",
            current_user.username,
            getattr(current_user, "role", ""),
            getattr(current_user, "vendor", ""),
            now,
            now
        )
    )
    new_ticket_id = cur.lastrowid
    cur.execute(
        """
        INSERT INTO support_messages (ticket_id, sender, message, created_at)
        VALUES (?, 'worker', ?, ?)
        """,
        (new_ticket_id, message, now)
    )
    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "ticket_id": new_ticket_id,
        "message": "문의가 접수되었습니다. 담당자가 확인 후 답변드리겠습니다."
    })






@app.route("/support-alerts/poll")
@login_required
def support_alerts_poll():
    """Return newly registered support ticket count for master screen.

    This is intentionally lightweight polling for the case where the web/PWA app
    is already open. It does not send OS-level push notifications.
    """
    if not can_manage_support(current_user):
        return jsonify({"ok": False, "message": "forbidden"}), 403

    try:
        since_id = int(request.args.get("since_id", 0) or 0)
    except (TypeError, ValueError):
        since_id = 0

    conn = get_user_db()
    params = []
    where = "WHERE 1=1"


    latest_row = conn.execute(
        f"SELECT COALESCE(MAX(id), 0) AS max_id FROM support_tickets {where}",
        params
    ).fetchone()
    max_id = int(latest_row["max_id"] if latest_row else 0)

    total_row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM support_tickets {where}",
        params
    ).fetchone()
    total_count = int(total_row["cnt"] if total_row else 0)

    count_params = list(params) + [since_id]
    count_row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM support_tickets {where} AND id > ?",
        count_params
    ).fetchone()
    new_count = int(count_row["cnt"] if count_row else 0)

    latest_ticket = None
    if new_count:
        ticket = conn.execute(
            f"""
            SELECT id, category, requester, car_number, created_at
            FROM support_tickets
            {where} AND id > ?
            ORDER BY id DESC
            LIMIT 1
            """,
            count_params
        ).fetchone()
        if ticket:
            latest_ticket = {
                "id": ticket["id"],
                "category": ticket["category"],
                "requester": ticket["requester"],
                "car_number": ticket["car_number"],
                "created_at": ticket["created_at"],
            }

    conn.close()
    return jsonify({
        "ok": True,
        "max_id": max_id,
        "total_count": total_count,
        "new_count": new_count,
        "latest_ticket": latest_ticket,
        "manage_url": url_for("support_manage"),
    })


@app.route("/support-manage")
@login_required
def support_manage():
    if not can_manage_support(current_user):
        flash("❌ 문의 관리는 마스터 계정만 볼 수 있습니다.")
        return redirect(url_for("dashboard"))

    status = request.args.get("status", "")
    query = "SELECT * FROM support_tickets WHERE 1=1"
    params = []

    if status:
        query += " AND status=?"
        params.append(status)


    query += " ORDER BY id DESC"

    conn = get_user_db()
    rows = conn.execute(query, params).fetchall()
    conn.close()

    return render_template("support_manage.html", rows=rows, selected_status=status)


@app.route("/support-manage/<int:ticket_id>/reply", methods=["POST"])
@login_required
def support_reply(ticket_id):
    if not can_manage_support(current_user):
        flash("❌ 문의 답변은 마스터 계정만 가능합니다.")
        return redirect(url_for("dashboard"))

    status = request.form.get("status", "확인중").strip() or "확인중"
    admin_reply = request.form.get("admin_reply", "").strip()

    conn = get_user_db()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE support_tickets
        SET status=?, admin_reply=?, updated_at=?
        WHERE id=?
        """,
        (status, admin_reply, datetime.now().strftime("%Y-%m-%d %H:%M"), ticket_id)
    )

    if admin_reply:
        cur.execute(
            """
            INSERT INTO support_messages (ticket_id, sender, message, created_at)
            VALUES (?, 'admin', ?, ?)
            """,
            (ticket_id, admin_reply, datetime.now().strftime("%Y-%m-%d %H:%M"))
        )

    conn.commit()
    conn.close()

    flash("문의 답변이 저장되었습니다.")
    return redirect(url_for("support_manage"))



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

app = app

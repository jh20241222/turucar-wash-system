import fcntl
import json
import os
import re
import shutil
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
KST = ZoneInfo("Asia/Seoul")
def now_kst():
    """현재 KST 시각 반환."""
    return datetime.now(KST)
def today_kst():
    """오늘 KST 날짜 문자열 반환 (YYYY-MM-DD)."""
    return datetime.now(KST).strftime("%Y-%m-%d")
import io
import requests as _requests
import pandas as pd
try:
    from PIL import Image as _PILImage, ExifTags as _ExifTags, ImageOps as _ImageOps
    _PIL_AVAILABLE = True
    try:
        # 아이폰이 "고효율성(HEIC)" 카메라 설정일 때 올라오는 .heic/.heif 사진 지원.
        # 이 플러그인이 없으면 Pillow가 HEIC를 열지 못해 아래 압축 로직이 통째로 실패하고
        # 원본 HEIC 바이트가 그대로 저장되는데, 저장/응답 시 Content-Type은 항상
        # image/jpeg로 고정돼 있어(_store_wash_photos 등) 브라우저가 이를 JPEG로 잘못
        # 해석하면서 사진이 깨지거나 색이 이상하게(예: 파랗게) 보이는 원인이 된다
        # (2026-09-01, "사진이 파랗다"/"화질이 별로다" 제보로 확인).
        import pillow_heif as _pillow_heif
        _pillow_heif.register_heif_opener()
    except ImportError:
        pass
except ImportError:
    _PIL_AVAILABLE = False
from apscheduler.schedulers.background import BackgroundScheduler
from flask import (
    Flask, Response, flash, jsonify, redirect, render_template, render_template_string,
    request, send_file, send_from_directory, url_for
)
from flask_login import (
    LoginManager, UserMixin, current_user,
    login_required, login_user, logout_user
)
from werkzeug.datastructures import FileStorage
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename
app = Flask(__name__)
def _configure_secret_key():
    """SECRET_KEY를 하드코딩하지 않는다. 운영(Railway)에서는 환경변수 필수,
    로컬 개발에서만 매 실행마다 임시 랜덤 키를 생성한다(재시작 시 기존 세션 만료)."""
    import secrets
    env_key = os.environ.get("SECRET_KEY")
    if env_key:
        return env_key
    is_railway = any(os.environ.get(name) for name in (
        "RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID",
        "RAILWAY_SERVICE_ID", "RAILWAY_DEPLOYMENT_ID",
    ))
    if is_railway:
        raise RuntimeError(
            "SECRET_KEY 환경변수가 설정되지 않았습니다. Railway 환경변수에 SECRET_KEY를 추가한 뒤 "
            "다시 배포하세요. 이 값이 없으면 로그인 세션이 위조될 수 있어 앱 시작을 중단합니다."
        )
    print("[TuruWash] 경고: SECRET_KEY 환경변수가 없어 임시 랜덤 키로 실행합니다. "
          "서버 재시작 시 기존 로그인 세션은 모두 만료됩니다. 운영 환경에서는 SECRET_KEY를 반드시 설정하세요.")
    return secrets.token_hex(32)
app.secret_key = _configure_secret_key()
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
DAMAGE_UPLOAD_DIR = os.path.join(DATA_DIR, "damage_photos")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
NOTICE_IMG_DIR = os.path.join(DATA_DIR, "notice_images")
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
    os.makedirs(NOTICE_IMG_DIR, exist_ok=True)
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
def _connect_sqlite(path):
    """WAL 모드 + busy_timeout 적용된 SQLite 연결.
    여러 gunicorn 워커/스케줄러가 동시에 같은 DB 파일에 접근할 때
    'database is locked' 즉시 실패 대신 잠깐 대기 후 재시도하도록 한다."""
    conn = sqlite3.connect(path, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn
def get_user_db():
    return _connect_sqlite(USER_DB_PATH)
def get_wash_db():
    return _connect_sqlite(WASH_DB_PATH)
# =========================================================
# 전국 고정 시/도 + 구/군 데이터 (지역 배정 / 필터 드롭다운 공용)
# =========================================================
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
        "강화군","검단구","계양구","남동구","미추홀구","부평구","서해구",
        "연수구","영종구","옹진군","제물포구"
    ],
    "대전광역시": ["대덕구","동구","서구","유성구","중구"],
    "울산광역시": ["남구","동구","북구","울주군","중구"],
    "세종특별자치시": ["세종시"],
    "경기도": [
        "가평군","고양시","과천시","광명시","광주시","구리시","군포시","김포시",
        "남양주시","동두천시","부천시","성남시","수원시","시흥시","안산시",
        "안성시","안양시","양주시","양평군","여주시","연천군","오산시","용인시",
        "의왕시","의정부시","이천시","파주시","평택시","포천시","하남시","화성시"
    ],
    "강원특별자치도": [
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
    "전북특별자치도": [
        "고창군","군산시","김제시","남원시","무주군","부안군","순창군",
        "완주군","익산시","임실군","장수군","전주시",
        "정읍시","진안군"
    ],
    "전남광주통합특별시": [
        "강진군","고흥군","곡성군","광산구","광양시","구례군","나주시","남구",
        "담양군","동구","목포시","무안군","보성군","북구","서구","순천시",
        "신안군","여수시","영광군","영암군","완도군","장성군","장흥군","진도군",
        "함평군","해남군","화순군"
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
            created_at TEXT NOT NULL,
            image_path TEXT
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS damage_reports (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            car_number    TEXT NOT NULL,
            wash_date     TEXT NOT NULL,
            damage_location TEXT NOT NULL,
            description   TEXT,
            photo_front   TEXT,
            photo_damage1 TEXT,
            photo_damage2 TEXT,
            photo_damage3 TEXT,
            photo_damage4 TEXT,
            photo_damage5 TEXT,
            slack_ts      TEXT,
            reporter      TEXT NOT NULL,
            vendor        TEXT,
            status        TEXT NOT NULL DEFAULT '접수',
            admin_reply   TEXT,
            created_at    TEXT NOT NULL,
            updated_at    TEXT
        )
    """)
    # damage_reports 컬럼 마이그레이션 (기존 DB 호환)
    _dr_cols = [row[1] for row in cur.execute("PRAGMA table_info(damage_reports)").fetchall()]
    for _col in ("photo_damage3", "photo_damage4", "photo_damage5", "slack_ts"):
        if _col not in _dr_cols:
            cur.execute(f"ALTER TABLE damage_reports ADD COLUMN {_col} TEXT")
    # dashboard_notices 테이블에 image_path 컬럼 마이그레이션 (기존 DB 호환)
    existing_cols = [row[1] for row in cur.execute("PRAGMA table_info(dashboard_notices)").fetchall()]
    if "image_path" not in existing_cols:
        cur.execute("ALTER TABLE dashboard_notices ADD COLUMN image_path TEXT")
    # 차량 청결 VOC (슬랙 동기화)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voc_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            slack_ts TEXT NOT NULL UNIQUE,
            author TEXT,
            text TEXT,
            permalink TEXT,
            synced_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '신규',
            city TEXT,
            district TEXT,
            note TEXT,
            requested_by TEXT,
            requested_at TEXT,
            photos TEXT
        )
    """)
    # voc_items.photos 컬럼 마이그레이션 (기존 DB 호환)
    _voc_cols = [row[1] for row in cur.execute("PRAGMA table_info(voc_items)").fetchall()]
    if "photos" not in _voc_cols:
        cur.execute("ALTER TABLE voc_items ADD COLUMN photos TEXT")
    # 긴급세차 / VOC 요청 건 — 지역 담당 작업자에게 전달되는 작업 큐
    cur.execute("""
        CREATE TABLE IF NOT EXISTS field_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL DEFAULT 'urgent',
            car_number TEXT,
            city TEXT,
            district TEXT,
            vendor TEXT,
            note TEXT,
            voc_item_id INTEGER,
            status TEXT NOT NULL DEFAULT '대기',
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            completed_by TEXT,
            completed_at TEXT,
            scheduled_date TEXT,
            accepted_by TEXT,
            accepted_at TEXT
        )
    """)
    # field_requests 작업조치예정일 컬럼 마이그레이션 (기존 DB 호환)
    _fr_cols = [row[1] for row in cur.execute("PRAGMA table_info(field_requests)").fetchall()]
    for _fr_col in ("scheduled_date", "accepted_by", "accepted_at", "complete_note", "cancelled_by", "cancelled_at"):
        if _fr_col not in _fr_cols:
            cur.execute(f"ALTER TABLE field_requests ADD COLUMN {_fr_col} TEXT")
    # 웹 푸시 구독 정보 (브라우저/PWA 알림용)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS push_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            endpoint TEXT NOT NULL UNIQUE,
            p256dh TEXT NOT NULL,
            auth TEXT NOT NULL,
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
            updated_at TEXT,
            BM구분 TEXT
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
    if "failed_attempts" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN failed_attempts INTEGER DEFAULT 0")
    if "locked_until" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN locked_until TEXT")
    region_cols = [row[1] for row in cur.execute("PRAGMA table_info(account_region)").fetchall()]
    if "created_by" not in region_cols:
        cur.execute("ALTER TABLE account_region ADD COLUMN created_by TEXT")
    # (2026-09-03) 차량소속(피플카/휴맥스 같은 차량 운영사) 담당자 계정용 — account_region과
    # 같은 패턴이지만 시/도+구/군이 아니라 차량소속으로 범위를 지정한다. 이 계정은 업체(청소업체)
    # 소속이 아닐 수도 있어서(차량 운영사 직원이지 청소업체 직원이 아님) accounts.vendor는
    # NULL이어도 되고, scoped_condition()이 이 배정이 있으면 업체+지역 대신 차량소속으로 범위를
    # 제한한다.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS account_fleet (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            차량소속 TEXT NOT NULL,
            created_by TEXT
        )
    """)
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
        if "세차일" not in hist_cols:
            # 이월된 오더는 세차일(원래 예정일)과 세차완료일(실제 완료일)이 다를 수 있는데,
            # 사진은 완료 처리 시점에 세차일 기준으로 저장되므로 완료 후 조회 시에도
            # 세차일로 찾아야 정확하다. 기존 행은 값이 없으니 세차완료일로 채워 최소한
            # 이월 안 된 과거 기록은 계속 조회되게 해둔다.
            cur.execute("ALTER TABLE wash_history ADD COLUMN 세차일 TEXT")
            cur.execute("UPDATE wash_history SET 세차일 = 세차완료일 WHERE 세차일 IS NULL")
            print("[TuruWash] wash_history.세차일 컬럼 추가됨")
        # (2026-09-04) 차량별 훼손관리 대시보드용 — 세차완료 시 작업자가 입력한 훼손/경고등
        # 메모가 있는 건을 관리자가 "확인"했는지 추적한다. 체크 안 된 건은 대시보드 상단에
        # "확인 필요"로 노출되고, 관리자가 확인 처리하면 이 플래그가 세워져 더 이상 상단에
        # 뜨지 않는다.
        if "damage_checked" not in hist_cols:
            cur.execute("ALTER TABLE wash_history ADD COLUMN damage_checked INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE wash_history ADD COLUMN damage_checked_by TEXT")
            cur.execute("ALTER TABLE wash_history ADD COLUMN damage_checked_at TEXT")
            print("[TuruWash] wash_history.damage_checked 컬럼 추가됨")
        vm_cols = [row[1] for row in cur.execute("PRAGMA table_info(vehicle_master)").fetchall()]
        if "BM구분" not in vm_cols:
            cur.execute("ALTER TABLE vehicle_master ADD COLUMN BM구분 TEXT")
            print("[TuruWash] vehicle_master.BM구분 컬럼 추가됨")
        # 세차 현장 사진 (현재는 차량소속 '카일이삼제스퍼' 전용) — Cloudflare R2에 실제 파일을
        # 저장하고, 이 테이블에는 R2 오브젝트 키만 남긴다. 차량번호+세차일로 묶어서
        # wash_list(진행중)든 wash_history(완료됨)든 상관없이 조회할 수 있게 한다.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wash_photos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                차량번호 TEXT NOT NULL,
                세차일 TEXT NOT NULL,
                r2_key TEXT NOT NULL,
                original_name TEXT,
                shot_label TEXT,
                uploaded_by TEXT,
                uploaded_at TEXT NOT NULL
            )
        """)
        # shot_label 컬럼 마이그레이션 (기존 DB 호환 — 촬영 슬롯별 라벨, 예: '전범퍼 정면')
        _wp_cols = [row[1] for row in cur.execute("PRAGMA table_info(wash_photos)").fetchall()]
        if "shot_label" not in _wp_cols:
            cur.execute("ALTER TABLE wash_photos ADD COLUMN shot_label TEXT")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_wash_photos_car_date ON wash_photos(차량번호, 세차일)")
        # (2026-09-03) AI 훼손 판독 학습용 라벨링 데이터 — 관리자가 완료현황 사진을 보면서
        # "정상"/"훼손의심"으로 태깅한 결과를 쌓아두는 테이블. 이걸로 나중에 가벼운 이미지
        # 분류 모델을 학습시킨다(사진마다 API를 호출하는 대신, 학습된 모델을 서버에 올려
        # 반복 비용 없이 자체 판독하는 게 목표). photo_id는 UNIQUE라 같은 사진을 다시
        # 태깅하면 라벨만 갱신된다.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS damage_ai_labels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                photo_id INTEGER UNIQUE NOT NULL,
                차량번호 TEXT,
                shot_label TEXT,
                label TEXT NOT NULL,
                labeled_by TEXT,
                labeled_at TEXT NOT NULL
            )
        """)
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
# gunicorn 등 다중 워커 프로세스 환경에서 워커마다 스케줄러가 따로 뜨면
# 같은 SQLite DB에 동시에 쓰기 작업을 해서 "database is locked" 에러와
# VOC 슬랙 동기화 중복 삽입(UNIQUE constraint) 문제가 발생한다.
# 파일 락으로 전체 워커 중 딱 하나만 스케줄러를 실제로 구동하도록 막는다.
_scheduler_lock_file = None
def _acquire_scheduler_lock():
    global _scheduler_lock_file
    try:
        lock_path = os.path.join(DATA_DIR, ".scheduler.lock")
        f = open(lock_path, "w")
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        _scheduler_lock_file = f  # 프로세스 살아있는 동안 열어둬야 락 유지됨(GC/close 되면 해제됨)
        return True
    except (IOError, OSError):
        return False
_scheduler = BackgroundScheduler(timezone="Asia/Seoul")
if _acquire_scheduler_lock():
    _scheduler.add_job(scheduled_daily_job, "cron", hour=0, minute=0)
    _scheduler.start()
    print("[TuruWash] APScheduler 시작 — 매일 00:00 KST 이월/리셋 자동 실행 (이 워커가 스케줄러 담당)")
else:
    print("[TuruWash] APScheduler 스킵 — 다른 워커가 이미 스케줄러 담당 중")
# =========================================================
# 로그인 설정
# =========================================================
login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.init_app(app)
# "자동 로그인" 체크 시 브라우저를 닫아도 30일간 로그인이 유지되는 쿠키(remember token) 설정
app.config["REMEMBER_COOKIE_DURATION"] = timedelta(days=30)
app.config["REMEMBER_COOKIE_HTTPONLY"] = True
app.config["REMEMBER_COOKIE_SAMESITE"] = "Lax"
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
    @property
    def is_contact_center(self):
        return bool(self.username) and "컨택센터" in self.username
    @property
    def fleets(self):
        """이 계정에 배정된 차량소속 목록(없으면 빈 리스트) — 사이드바 '차량 관리' 메뉴
        노출 여부 및 템플릿에서의 표시에 쓴다. 요청당 한 번만 조회하도록 캐싱한다."""
        if not hasattr(self, "_fleets_cache"):
            self._fleets_cache = _user_fleets(self.username) if self.username else []
        return self._fleets_cache
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
def _user_fleets(username):
    """이 계정에 배정된 차량소속(account_fleet) 목록. 비어있으면 차량소속 스코프 계정이 아님."""
    conn = get_user_db()
    rows = conn.execute(
        "SELECT DISTINCT 차량소속 FROM account_fleet WHERE username=? ORDER BY 차량소속",
        (username,)
    ).fetchall()
    conn.close()
    return [r["차량소속"] for r in rows]
def scoped_condition(table_name, user):
    if user.is_master or getattr(user, "is_contact_center", False):
        # 컨택센터 계정은 특정 업체/지역에 소속되지 않은 전역 조회 계정이므로 마스터처럼 범위 제한 없이 조회한다.
        return "", []
    # (2026-09-03) 차량소속(피플카/휴맥스 같은 차량 운영사) 담당자 계정은 업체(청소업체)+지역이
    # 아니라 차량소속 기준으로 범위를 제한한다 — 청소업체 직원이 아니라 차량 운영사 직원이라
    # 업체/지역 개념 자체가 안 맞기 때문. account_fleet에 배정이 있으면 아래 업체/지역 로직은
    # 타지 않고 차량소속으로만 필터링한다.
    fleets = _user_fleets(user.username)
    if fleets:
        fleet_clause = " OR ".join([f"{table_name}.차량소속 = ?"] * len(fleets))
        return f" AND ({fleet_clause})", list(fleets)
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
            # 담당 지역이 하나도 없으면 아무 것도 매칭되지 않게 하되, "1=0"에는 바인딩할 파라미터가 없어야 한다.
            # (여기서 params를 그대로 반환하면 자리표시자 수와 파라미터 수가 안 맞아 SQL 오류가 났었음)
            return " AND 1=0", []
        region_clause = " OR ".join([f"({table_name}.지역시도 = ? AND {table_name}.지역구군 = ?)"] * len(regions))
        clauses.append(f"({region_clause})")
        for region in regions:
            params.extend([region["city"], region["district"]])
    return " AND " + " AND ".join(clauses), params
def filter_distinct_values(cur, table_name, column_name, base_query, base_params):
    query = f"SELECT DISTINCT {column_name} AS value FROM {table_name} WHERE 1=1{base_query} ORDER BY {column_name}"
    rows = cur.execute(query, base_params).fetchall()
    return [r["value"] for r in rows if r["value"] not in (None, "", "None")]
# =========================================================
# 공용 페이지네이션 (리스트가 있는 관리 화면에서 공통으로 사용)
# =========================================================
def paginate_list(items, page, per_page=10):
    """리스트를 페이지 단위로 잘라서 (해당 페이지 항목, 현재 페이지, 전체 페이지 수)를 반환."""
    total = len(items)
    total_pages = max(1, -(-total // per_page))  # ceil division
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    return items[start:start + per_page], page, total_pages
def pagination_window(current_page, total_pages, radius=2):
    """1 2 3 ... 형태로 표시할 페이지 번호 목록. 생략된 구간은 '…' 문자열로 표시."""
    if total_pages <= 1:
        return []
    pages = {1, total_pages, current_page}
    for i in range(current_page - radius, current_page + radius + 1):
        if 1 <= i <= total_pages:
            pages.add(i)
    ordered = sorted(pages)
    result = []
    prev = None
    for p in ordered:
        if prev is not None and p - prev > 1:
            result.append("…")
        result.append(p)
        prev = p
    return result
def pagination_url(param_name, page_num):
    """현재 쿼리스트링을 유지한 채 param_name 값만 page_num으로 바꾼 URL을 만든다.
    (한 화면에 목록이 여러 개 있을 때 acct_page/region_page 처럼 서로 다른 파라미터명을 쓸 수 있게 함)"""
    args = request.args.to_dict()
    args[param_name] = page_num
    return request.path + "?" + urlencode(args)
app.jinja_env.globals["pagination_window"] = pagination_window
app.jinja_env.globals["pagination_url"] = pagination_url
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
# 컨택센터 전용 계정 접근 제한
# (이름/아이디에 "컨택센터"가 들어간 계정은 홈(공지사항)과 세차 대상 리스트만
#  볼 수 있고, 그 외 메뉴는 전부 접근 불가 처리한다)
# =========================================================
CONTACT_CENTER_ALLOWED_ENDPOINTS = {
    "dashboard", "home", "wash_target_list", "logout", "static",
    "service_worker", "offline",
}
@app.before_request
def restrict_contact_center_access():
    if not current_user.is_authenticated:
        return None
    if not getattr(current_user, "is_contact_center", False):
        return None
    endpoint = request.endpoint
    if endpoint in CONTACT_CENTER_ALLOWED_ENDPOINTS:
        return None
    # 상단 플래시 배너 대신 홈 화면 가운데에 모달로 안내한다 (dashboard.html의 denied 처리 참고)
    return redirect(url_for("dashboard", denied=1))
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
# 로그인 실패 잠금 (브루트포스 방지)
# =========================================================
LOGIN_MAX_ATTEMPTS = 5
LOGIN_LOCKOUT_MINUTES = 15
def _account_lock_status(user_row):
    """계정 잠금 여부와 남은 시간(분)을 반환한다."""
    locked_until_str = user_row["locked_until"] if user_row else None
    if not locked_until_str:
        return False, None
    try:
        until_dt = datetime.fromisoformat(locked_until_str)
    except (ValueError, TypeError):
        return False, None
    now = now_kst()
    if now < until_dt:
        remaining = max(1, int((until_dt - now).total_seconds() // 60) + 1)
        return True, remaining
    return False, None
def _register_login_failure(username):
    conn = get_user_db()
    cur = conn.cursor()
    row = cur.execute("SELECT failed_attempts FROM accounts WHERE username=?", (username,)).fetchone()
    if row:
        attempts = (row["failed_attempts"] or 0) + 1
        if attempts >= LOGIN_MAX_ATTEMPTS:
            locked_until = (now_kst() + timedelta(minutes=LOGIN_LOCKOUT_MINUTES)).isoformat()
            cur.execute(
                "UPDATE accounts SET failed_attempts=0, locked_until=? WHERE username=?",
                (locked_until, username)
            )
        else:
            cur.execute("UPDATE accounts SET failed_attempts=? WHERE username=?", (attempts, username))
        conn.commit()
    conn.close()
def _register_login_success(username):
    conn = get_user_db()
    cur = conn.cursor()
    cur.execute("UPDATE accounts SET failed_attempts=0, locked_until=NULL WHERE username=?", (username,))
    conn.commit()
    conn.close()
# =========================================================
# 로그인
# =========================================================
SAVED_ID_COOKIE = "saved_id"
AUTO_LOGIN_COOKIE = "auto_login_pref"
COOKIE_MAX_AGE = 60 * 60 * 24 * 365  # 1년
@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        pw = request.form.get("password", "")
        id_save = request.form.get("id_save") == "on"
        auto_login = request.form.get("auto_login") == "on"
        conn = get_user_db()
        cur = conn.cursor()
        user = cur.execute("SELECT * FROM accounts WHERE username=?", (username,)).fetchone()
        conn.close()
        if user:
            locked, remaining = _account_lock_status(user)
            if locked:
                flash(f"❌ 로그인 시도가 너무 많아 계정이 잠겼습니다. {remaining}분 후 다시 시도하세요.")
                return redirect(url_for("login"))
        if user and check_password_hash(user["password"], pw):
            _register_login_success(username)
            login_user(
                User(user["id"], user["username"], user["role"], user["vendor"], user["parent_id"]),
                remember=auto_login
            )
            resp = redirect(url_for("dashboard"))
            if id_save:
                resp.set_cookie(SAVED_ID_COOKIE, username, max_age=COOKIE_MAX_AGE, samesite="Lax")
            else:
                resp.delete_cookie(SAVED_ID_COOKIE)
            if auto_login:
                resp.set_cookie(AUTO_LOGIN_COOKIE, "1", max_age=COOKIE_MAX_AGE, samesite="Lax")
            else:
                resp.delete_cookie(AUTO_LOGIN_COOKIE)
            return resp
        if user:
            _register_login_failure(username)
        flash("❌ 아이디 또는 비밀번호가 잘못되었습니다.")
        return redirect(url_for("login"))
    saved_id = request.cookies.get(SAVED_ID_COOKIE, "")
    auto_login_checked = request.cookies.get(AUTO_LOGIN_COOKIE) == "1"
    return render_template("login.html", saved_id=saved_id, auto_login_checked=auto_login_checked)
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
            WHERE ({region_clauses}){vendor_clause} AND (BM구분 IS NULL OR TRIM(BM구분) NOT IN ('혼용'))
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
def create_dashboard_notice(title, body, author, image_path=None):
    conn = get_user_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dashboard_notices (title, body, author, created_at, image_path)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            title,
            body,
            author,
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            image_path,
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
        SELECT id, title, body, author, created_at, image_path
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
        SELECT id, title, body, author, created_at, image_path
        FROM dashboard_notices
        WHERE id=?
        """,
        (notice_id,)
    ).fetchone()
    conn.close()
    return row
def update_dashboard_notice_item(notice_id, title, body, author, image_path=None, clear_image=False):
    conn = get_user_db()
    cur = conn.cursor()
    if clear_image:
        cur.execute(
            "UPDATE dashboard_notices SET title=?, body=?, author=?, image_path=NULL WHERE id=?",
            (title, body, author, notice_id)
        )
    elif image_path is not None:
        cur.execute(
            "UPDATE dashboard_notices SET title=?, body=?, author=?, image_path=? WHERE id=?",
            (title, body, author, image_path, notice_id)
        )
    else:
        cur.execute(
            "UPDATE dashboard_notices SET title=?, body=?, author=? WHERE id=?",
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
NOTICE_ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp", "pdf"}
def _notice_allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in NOTICE_ALLOWED_EXTENSIONS
def _save_notice_file(file_obj):
    """파일을 NOTICE_IMG_DIR에 저장하고 파일명을 반환한다."""
    os.makedirs(NOTICE_IMG_DIR, exist_ok=True)
    ext = file_obj.filename.rsplit(".", 1)[1].lower()
    unique_name = f"{uuid.uuid4().hex}.{ext}"
    file_obj.save(os.path.join(NOTICE_IMG_DIR, unique_name))
    return unique_name
def _delete_notice_file(filename):
    """NOTICE_IMG_DIR에서 파일을 삭제한다."""
    if filename:
        path = os.path.join(NOTICE_IMG_DIR, filename)
        try:
            if os.path.isfile(path):
                os.remove(path)
        except OSError:
            pass
@app.route("/notice_file/<path:filename>")
@login_required
def notice_file(filename):
    """공지사항 첨부파일 서빙"""
    return send_from_directory(NOTICE_IMG_DIR, filename)
@app.route("/dashboard/notice", methods=["POST"])
@login_required
def update_dashboard_notice():
    if not can_manage_support(current_user):
        flash("❌ 마스터 계정만 공지사항을 수정할 수 있습니다.")
        return redirect(url_for("dashboard"))
    notice_title = request.form.get("notice_title", "").strip() or "공지사항"
    notice_body = request.form.get("notice_body", "").strip() or "공지사항 내용을 입력해주세요."
    notice_author = request.form.get("notice_author", "").strip() or "투루카 담당자"
    image_path = None
    file = request.files.get("notice_image")
    if file and file.filename and _notice_allowed_file(file.filename):
        image_path = _save_notice_file(file)
    set_app_setting("dashboard_notice_title", notice_title)
    set_app_setting("dashboard_notice_body", notice_body)
    create_dashboard_notice(notice_title, notice_body, notice_author, image_path)
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
    clear_image = request.form.get("notice_clear_image") == "1"
    existing = get_dashboard_notice_by_id(notice_id)
    old_image = existing["image_path"] if existing else None
    new_image_path = None
    file = request.files.get("notice_image")
    if file and file.filename and _notice_allowed_file(file.filename):
        new_image_path = _save_notice_file(file)
        if old_image:
            _delete_notice_file(old_image)
    elif clear_image and old_image:
        _delete_notice_file(old_image)
    update_dashboard_notice_item(
        notice_id, notice_title, notice_body, notice_author,
        image_path=new_image_path,
        clear_image=clear_image and not new_image_path,
    )
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
@app.route("/wash_target_list")
@login_required
def wash_target_list():
    """세차 대상 리스트: 업로드된 차량 마스터 전체를 검색할 수 있게 보여준다.
    지역/업체로 범위를 좁히지 않고 전체 차량을 대상으로 하며,
    전체 대수 + 지역별/차량소속별 요약을 간단한 대시보드 형태로 보여준다.
    (컨택센터 계정 전용 — 세차 관리 메뉴에서 접근)"""
    if not (current_user.is_contact_center or current_user.is_master):
        flash("❌ 접근 권한이 없습니다.")
        return redirect(url_for("dashboard"))
    conn = get_wash_db()
    cur = conn.cursor()
    vehicles = cur.execute("""
        SELECT 차량번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군,
               담당업체, 최근세차일, 세차경과일, BM구분
        FROM vehicle_master
        ORDER BY 세차경과일 DESC, 차량번호
    """).fetchall()
    conn.close()
    vehicles_list = [dict(v) for v in vehicles]
    total_all = len(vehicles_list)

    # 지역별(시/도 + 구/군) 집계
    region_counts = {}
    for v in vehicles_list:
        key = (v["지역시도"] or "지역 미지정", v["지역구군"] or "")
        region_counts[key] = region_counts.get(key, 0) + 1
    region_stats = [
        {"city": city, "district": district, "total": cnt}
        for (city, district), cnt in sorted(region_counts.items(), key=lambda x: -x[1])
    ]

    # 차량소속별 집계
    org_counts = {}
    for v in vehicles_list:
        key = v["차량소속"] or "소속 미지정"
        org_counts[key] = org_counts.get(key, 0) + 1
    org_stats = [
        {"name": name, "total": cnt}
        for name, cnt in sorted(org_counts.items(), key=lambda x: -x[1])
    ]

    # BM구분별 집계
    bm_counts = {}
    for v in vehicles_list:
        key = v["BM구분"] or "BM 미지정"
        bm_counts[key] = bm_counts.get(key, 0) + 1
    bm_stats = [
        {"name": name, "total": cnt}
        for name, cnt in sorted(bm_counts.items(), key=lambda x: -x[1])
    ]

    return render_template(
        "contact_center_home.html",
        vehicles=vehicles_list,
        total_all=total_all,
        region_stats=region_stats,
        org_stats=org_stats,
        bm_stats=bm_stats,
    )
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
    notice_rows, notice_total, _, _ = get_dashboard_notices(1, 5)
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
            # (2026-09-03) 차량소속(피플카/휴맥스 같은 차량 운영사) 담당자는 청소업체 소속이
            # 아니므로 업체 없이 계정을 만들 수 있어야 한다 — 대신 차량소속을 하나 이상 지정하면
            # 업체 필수 조건을 대체한다. 차량소속 지정은 마스터만 할 수 있다(업체 지정과 동일한 제약).
            fleet_values = request.form.getlist("fleet") if current_user.is_master else []
            fleet_values = [f.strip() for f in fleet_values if f.strip()]
            if new_role != "master" and not vendor and not fleet_values:
                flash("❌ 업체 또는 차량소속 정보가 필요합니다.")
                return redirect(url_for("account_manage"))
            parent_id = None if new_role == "admin" else current_user.id
            try:
                cur.execute(
                    "INSERT INTO accounts (username, password, role, vendor, parent_id) VALUES (?, ?, ?, ?, ?)",
                    (username, generate_password_hash(password), new_role, vendor or None, parent_id)
                )
                if new_role == "staff" and city and district:
                    cur.execute(
                        "INSERT INTO account_region (username, city, district, created_by) VALUES (?, ?, ?, ?)",
                        (username, city, district, current_user.username)
                    )
                for fleet in fleet_values:
                    cur.execute(
                        "INSERT INTO account_fleet (username, 차량소속, created_by) VALUES (?, ?, ?)",
                        (username, fleet, current_user.username)
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
        if action == "assign_fleet":
            # 차량소속 배정/삭제는 업체 하위 구조와 무관한 별도 축이라, 지역과 달리
            # can_manage_target(업체+parent_id 기준)을 그대로 쓸 수 없다 — 마스터만 배정한다.
            if not current_user.is_master:
                flash("❌ 차량소속 배정 권한이 없습니다.")
                return redirect(url_for("account_manage"))
            username = request.form.get("fleet_username", "").strip()
            fleet = request.form.get("fleet_value", "").strip()
            target = cur.execute("SELECT * FROM accounts WHERE username=?", (username,)).fetchone()
            if not target:
                flash("❌ 해당 계정을 찾을 수 없습니다.")
                return redirect(url_for("account_manage"))
            if not fleet:
                flash("❌ 차량소속을 선택하세요.")
                return redirect(url_for("account_manage"))
            exists = cur.execute(
                "SELECT 1 FROM account_fleet WHERE username=? AND 차량소속=?",
                (username, fleet)
            ).fetchone()
            if exists:
                flash("ℹ 이미 등록된 차량소속입니다.")
            else:
                cur.execute(
                    "INSERT INTO account_fleet (username, 차량소속, created_by) VALUES (?, ?, ?)",
                    (username, fleet, current_user.username)
                )
                conn.commit()
                flash("✔ 차량소속이 등록되었습니다.")
            return redirect(url_for("account_manage"))
        if action == "delete_fleet":
            if not current_user.is_master:
                flash("❌ 차량소속 배정 권한이 없습니다.")
                return redirect(url_for("account_manage"))
            fleet_id = request.form.get("fleet_id", "").strip()
            cur.execute("DELETE FROM account_fleet WHERE id=?", (fleet_id,))
            conn.commit()
            flash("✔ 차량소속이 삭제되었습니다.")
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
                cur.execute(f"DELETE FROM account_fleet WHERE username IN ({placeholders})", child_usernames)
                cur.execute(f"DELETE FROM accounts WHERE username IN ({placeholders})", child_usernames)
            cur.execute("DELETE FROM account_region WHERE username=?", (username,))
            cur.execute("DELETE FROM account_fleet WHERE username=?", (username,))
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
    # 차량소속 배정 목록 (2026-09-03 추가) — 지역 권한과 같은 방식으로, 어느 계정이
    # 어느 차량소속으로 스코프되어 있는지 보여주고 배정/해제할 수 있게 한다.
    fleet_list = cur.execute(
        """
        SELECT af.id, af.username, af.차량소속, a.vendor, a.role, a.parent_id
        FROM account_fleet af
        JOIN accounts a ON a.username = af.username
        {where_clause}
        ORDER BY af.username, af.차량소속
        """.format(
            where_clause=""
            if current_user.is_master
            else "WHERE a.parent_id = ?"
        ),
        () if current_user.is_master else (current_user.id,)
    ).fetchall()
    # 전국 고정 시/도 + 구/군 데이터 (세차 오더 업로드 없이도 지역 배정 가능)
    # KOREA_REGIONS는 모듈 상단에 정의된 공용 상수를 사용한다.
    city_options = list(KOREA_REGIONS.keys())
    region_map = KOREA_REGIONS
    conn.close()
    # 차량소속 옵션은 차량마스터(WASH_DB)에 실제 등록된 값을 그대로 쓴다 — 임의 문자열을
    # 입력받으면 오탈자로 필터링이 안 맞는 계정이 생길 수 있어서 드롭다운으로 강제한다.
    wconn = get_wash_db()
    fleet_options = [
        r["차량소속"] for r in wconn.execute(
            "SELECT DISTINCT 차량소속 FROM vehicle_master WHERE 차량소속 IS NOT NULL AND TRIM(차량소속) != '' ORDER BY 차량소속"
        ).fetchall()
    ]
    wconn.close()

    # 계정/지역 검색 (2026-09-01 추가) — 계정이 60개+로 늘어나면서 페이지를 여러 장 넘겨야
    # 원하는 계정을 찾을 수 있었다. 계정명·업체명(계정 목록), 계정명·업체명·시/도·구/군
    # (지역 권한)으로 부분일치 검색해서 페이지네이션 이전에 걸러낸다 — 그래서 검색은
    # 현재 페이지에 보이는 것만이 아니라 전체를 대상으로 동작한다.
    acct_q = request.args.get("acct_q", "").strip()
    region_q = request.args.get("region_q", "").strip()
    fleet_q = request.args.get("fleet_q", "").strip()
    active_tab = request.args.get("tab", "acct")
    if active_tab not in ("acct", "region", "fleet"):
        active_tab = "acct"

    if acct_q:
        _q = acct_q.lower()
        accounts_filtered = [
            a for a in accounts
            if _q in (a["username"] or "").lower() or _q in (a["vendor"] or "").lower()
        ]
    else:
        accounts_filtered = accounts

    if region_q:
        _q = region_q.lower()
        region_list_filtered = [
            r for r in region_list
            if _q in (r["username"] or "").lower() or _q in (r["vendor"] or "").lower()
            or _q in (r["city"] or "").lower() or _q in (r["district"] or "").lower()
        ]
    else:
        region_list_filtered = region_list

    if fleet_q:
        _q = fleet_q.lower()
        fleet_list_filtered = [
            f for f in fleet_list
            if _q in (f["username"] or "").lower() or _q in (f["차량소속"] or "").lower()
        ]
    else:
        fleet_list_filtered = fleet_list

    acct_page = request.args.get("acct_page", 1, type=int)
    region_page = request.args.get("region_page", 1, type=int)
    fleet_page = request.args.get("fleet_page", 1, type=int)
    accounts_page, acct_current_page, acct_total_pages = paginate_list(accounts_filtered, acct_page, per_page=10)
    region_list_page, region_current_page, region_total_pages = paginate_list(region_list_filtered, region_page, per_page=10)
    fleet_list_page, fleet_current_page, fleet_total_pages = paginate_list(fleet_list_filtered, fleet_page, per_page=10)
    return render_template(
        "account_manage.html",
        accounts=accounts,
        accounts_filtered=accounts_filtered,
        accounts_page=accounts_page,
        acct_current_page=acct_current_page,
        acct_total_pages=acct_total_pages,
        region_list=region_list,
        region_list_filtered=region_list_filtered,
        region_list_page=region_list_page,
        region_current_page=region_current_page,
        region_total_pages=region_total_pages,
        fleet_list=fleet_list,
        fleet_list_filtered=fleet_list_filtered,
        fleet_list_page=fleet_list_page,
        fleet_current_page=fleet_current_page,
        fleet_total_pages=fleet_total_pages,
        fleet_options=fleet_options,
        vendors=vendors,
        creatable_accounts=creatable_accounts,
        city_options=city_options,
        region_map=region_map,
        acct_q=acct_q,
        region_q=region_q,
        fleet_q=fleet_q,
        active_tab=active_tab
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
        # 공백/줄바꿈이 섞여 있어도 컬럼명이 안전하게 매칭되도록 정리
        df.columns = df.columns.str.replace(" ", "").str.replace("\n", "").str.strip()
        # 예전 파일에 "BM구분" 대신 "용도구분"으로 남아있는 경우 자동으로 인식
        if "BM구분" not in df.columns and "용도구분" in df.columns:
            df = df.rename(columns={"용도구분": "BM구분"})
        required = ["차량번호", "차종명", "차량소속"]
        for col in required:
            if col not in df.columns:
                flash(f"❌ '{col}' 컬럼이 없습니다.")
                return redirect(url_for("upload_wash_list"))
        today_str = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        conn = get_wash_db()
        cur = conn.cursor()
        # 차량마스터는 "왕복(고정 스팟) 차량 목록" 그 자체이므로, 매번 업로드한 파일 내용으로 완전히
        # 교체한다. (기존 upsert 방식은 새 파일에서 빠진 차량이 계속 남아있어 왕복/혼용 판정이
        # 갱신되지 않는 문제가 있었음)
        cur.execute("DELETE FROM vehicle_master")
        inserted = 0
        skipped = 0
        for _, r in df.iterrows():
            차량번호 = str(r["차량번호"]).strip()
            if not 차량번호 or 차량번호.lower() == "nan":
                continue
            # 스팟/지역 없는 행 스킵
            스팟체크 = str(r.get("현재스팟명", "")).strip()
            if not 스팟체크 or 스팟체크.lower() == "nan":
                skipped += 1
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
            BM구분_raw = r.get("BM구분", None)
            BM구분 = str(BM구분_raw).strip() if BM구분_raw and str(BM구분_raw).strip().lower() not in ("nan", "") else None
            cur.execute("""
                INSERT INTO vehicle_master
                (차량번호, 차대번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 담당업체, 최근세차일, 세차경과일, updated_at, BM구분)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (차량번호, 차대번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 담당업체, 최근세차일, 세차경과일, today_str, BM구분))
            inserted += 1
        conn.commit()
        conn.close()
        flash(f"✔ 차량 마스터 전체 교체 완료 — 등록 {inserted}대 (스팟 없어 제외 {skipped}대)")
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
        # 예전 파일에 "BM구분" 대신 "용도구분"으로 남아있는 경우 자동으로 인식
        if "BM구분" not in df.columns and "용도구분" in df.columns:
            df = df.rename(columns={"용도구분": "BM구분"})
        has_bm_col = "BM구분" in df.columns
        today_str = today_kst()
        conn = get_wash_db()
        cur = conn.cursor()
        inserted = 0
        skipped = 0
        excluded_mixed = 0
        for _, r in df.iterrows():
            # BM구분이 "혼용"인 차량은 오더에 올리지 않는다.
            # (혼용 차량은 스팟이 수시로 바뀌어 오더가 있어도 실제로 그 자리에 없을 수 있음)
            if has_bm_col:
                bm_val = str(r.get("BM구분", "")).strip()
                if bm_val == "혼용":
                    excluded_mixed += 1
                    continue
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
        msg = f"✔ 업로드 완료 — {inserted}건 신규등록" if skipped == 0 else f"✔ 업로드 완료 — {inserted}건 신규등록, {skipped}건 정보 업데이트"
        if excluded_mixed:
            msg += f" (BM구분 '혼용' {excluded_mixed}건 제외)"
        flash(msg)
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
# 혼용 차량 바로 등록 (오더 없이 완료처리)
# =========================================================
@app.route("/mixed_car_register", methods=["POST"])
@login_required
def mixed_car_register():
    """혼용(BM구분='혼용') 차량은 스팟이 수시로 바뀌어 정기 오더 생성 대상에서 제외되지만,
    작업자가 현장에서 우연히 마주쳐 세차하는 경우가 있다. 그런 경우 정식 오더 없이도
    차량번호만으로 wash_list에 오늘자 임시 오더를 하나 만들어 곧장 car_detail(완료 입력
    화면)으로 보내준다. 완료 처리(wash_complete)는 기존 로직을 그대로 타므로 사진 업로드,
    작업자 기록(완료 현황 계정별 스코프 포함) 등이 정상 오더와 완전히 동일하게 동작한다."""
    plate_input = request.form.get("plate", "").strip()
    if not plate_input:
        flash("❌ 차량번호를 입력해주세요.")
        return redirect(url_for("wash_list"))
    norm_target = _norm_plate(plate_input)
    conn = get_wash_db()
    cur = conn.cursor()
    vm_rows = cur.execute("SELECT * FROM vehicle_master WHERE TRIM(BM구분)='혼용'").fetchall()
    vm_row = next((r for r in vm_rows if _norm_plate(r["차량번호"]) == norm_target), None)
    if not vm_row:
        conn.close()
        flash(f"❌ 혼용 차량 목록에서 '{plate_input}' 차량을 찾을 수 없습니다. 차량마스터를 확인해주세요.")
        return redirect(url_for("wash_list"))
    # 스코프 확인 — scoped_condition()과 동일한 규칙(마스터/컨택센터: 무제한, 업체 관리자: 업체
    # 일치, 개별 작업자: 업체+담당 지역까지 일치)을 vehicle_master 조회 결과에 대해 그대로 적용해,
    # 다른 업체/지역 차량을 임의로 등록하는 걸 막는다.
    if not (current_user.is_master or getattr(current_user, "is_contact_center", False)):
        if (vm_row["담당업체"] or "") != (current_user.vendor or ""):
            conn.close()
            flash("❌ 담당 업체가 달라 등록할 수 없는 차량입니다.")
            return redirect(url_for("wash_list"))
        if current_user.is_staff:
            regions = _account_regions(current_user.username)
            if (vm_row["지역시도"], vm_row["지역구군"]) not in regions:
                conn.close()
                flash("❌ 담당 지역이 아니어서 등록할 수 없는 차량입니다.")
                return redirect(url_for("wash_list"))
    today_str = today_kst()
    # 오늘 이미 등록된(미완료) 오더가 있으면 중복 생성하지 않고 그 화면으로 이동
    existing = cur.execute(
        "SELECT id FROM wash_list WHERE 차량번호=? AND 완료=0 AND 세차일=?",
        (vm_row["차량번호"], today_str)
    ).fetchone()
    if existing:
        conn.close()
        flash("ℹ 오늘 이미 등록된 오더가 있어 해당 화면으로 이동합니다.")
        return redirect(url_for("car_detail", id=existing["id"]))
    cur.execute(
        """INSERT INTO wash_list
        (차량번호, 차종명, 차량소속, 스팟, 주소, 지역시도, 지역구군, 세차일, 업체, 밴드링크, 작업자, 완료, 등록일, 이월횟수, 세차경과일)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,0,?,0,0)""",
        (vm_row["차량번호"], vm_row["차종명"], vm_row["차량소속"], vm_row["스팟"], vm_row["주소"],
         vm_row["지역시도"], vm_row["지역구군"], today_str, vm_row["담당업체"], "",
         current_user.username, today_str)
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    flash(f"✔ 혼용 차량 '{vm_row['차량번호']}' 등록 완료 — 세차 기록을 입력해주세요.")
    return redirect(url_for("car_detail", id=new_id))
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
    # KOREA_REGIONS는 모듈 상단에 정의된 공용 상수를 사용한다.
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
    if not car:
        # 이 화면(오더 진행중 목록)에서만 사라졌을 뿐, 이미 완료 처리(wash_complete)돼
        # wash_history로 넘어간 것일 수 있다 — 특히 사진 업로드가 오래 걸려 화면이 멈춘
        #것처럼 보이다가 다시 이 페이지로 돌아왔을 때 "정보를 찾을 수 없다"는 식으로만
        # 뜨면, 실제로는 정상 처리된 건지 진짜 유실된 건지 알 수가 없어서 혼란스러웠다.
        # wash_history.원본ID로 이미 완료된 기록이 있는지 확인해서, 있으면 그 결과 화면
        # 링크를 바로 보여준다.
        hist_scope_sql, hist_scope_params = scoped_condition("wash_history", current_user)
        completed = cur.execute(
            "SELECT id FROM wash_history WHERE 원본ID=?" + hist_scope_sql + " ORDER BY id DESC LIMIT 1",
            [id] + hist_scope_params
        ).fetchone()
        conn.close()
        if completed:
            return render_template_string("""
{% extends "base.html" %}{% block content %}
<div style="max-width:420px;margin:60px auto;text-align:center;padding:0 20px;font-family:'Pretendard',sans-serif;">
    <div style="font-size:40px;margin-bottom:10px;">✅</div>
    <h2 style="margin:0 0 8px;">이미 처리된 오더입니다</h2>
    <p style="color:#6b7280;font-size:14px;line-height:1.6;margin:0 0 22px;">
        이 오더는 세차 완료 처리가 정상적으로 접수되어 이미 완료 기록으로 옮겨졌습니다.<br>
        사진이나 내역이 궁금하면 아래에서 바로 확인하세요.
    </p>
    <a href="{{ url_for('wash_record', id=completed_id) }}" style="display:inline-block;background:#212121;color:#fff;font-weight:700;padding:12px 22px;border-radius:10px;text-decoration:none;margin-bottom:10px;">완료된 내역 보기</a><br>
    <a href="{{ url_for('wash_list') }}" style="display:inline-block;color:#6b7280;font-size:13px;text-decoration:underline;margin-top:6px;">세차 오더 목록으로</a>
</div>
{% endblock %}
""", completed_id=completed["id"])
        return render_template_string("""
{% extends "base.html" %}{% block content %}
<div style="max-width:420px;margin:60px auto;text-align:center;padding:0 20px;font-family:'Pretendard',sans-serif;">
    <div style="font-size:40px;margin-bottom:10px;">❌</div>
    <h2 style="margin:0 0 8px;">차량 정보를 찾을 수 없습니다</h2>
    <p style="color:#6b7280;font-size:14px;line-height:1.6;margin:0 0 22px;">
        이미 삭제되었거나, 담당 범위 밖의 오더일 수 있습니다.<br>
        완료 처리 중이었다면 완료 현황에서 반영 여부를 확인해보세요.
    </p>
    <a href="{{ url_for('wash_status') }}" style="display:inline-block;background:#212121;color:#fff;font-weight:700;padding:12px 22px;border-radius:10px;text-decoration:none;margin-bottom:10px;">완료 현황에서 확인</a><br>
    <a href="{{ url_for('wash_list') }}" style="display:inline-block;color:#6b7280;font-size:13px;text-decoration:underline;margin-top:6px;">세차 오더 목록으로</a>
</div>
{% endblock %}
"""), 404
    conn.close()
    from datetime import date as _date
    try:
        reg_date = (car["등록일"] or car["세차일"] or today_kst())[:10]
        elapsed = max((_date.fromisoformat(today_kst()) - _date.fromisoformat(reg_date)).days, 0)
    except Exception:
        elapsed = 0
    is_long_wash = elapsed >= 14
    # 차량소속이 지정된 목록(현재 '카일이삼제스퍼')인 경우에만 세차 현장 사진
    # 업로드/관리 섹션을 노출한다.
    show_photo_section = (car["차량소속"] or "").strip() in PHOTO_UPLOAD_ORGS
    car_photos = _get_wash_photos(car["차량번호"], car["세차일"]) if show_photo_section else []
    return render_template(
        "car_detail.html", car=car, elapsed=elapsed, is_long_wash=is_long_wash,
        show_photo_section=show_photo_section, car_photos=car_photos,
        r2_configured=bool(_get_r2_client()),
        photo_slot_groups=PHOTO_SLOT_GROUPS
    )
# =========================================================
# 세차 현장 사진 업로드 / 조회 / 삭제 (차량소속 '카일이삼제스퍼' 전용, R2 저장)
# =========================================================
@app.route("/car_photo_upload/<int:id>", methods=["POST"])
@login_required
def car_photo_upload(id):
    target = _lookup_wash_car_for_photo(id, current_user)
    if not target:
        flash("❌ 차량 정보를 찾을 수 없습니다.")
        return redirect(url_for("wash_list"))
    if (target["차량소속"] or "").strip() not in PHOTO_UPLOAD_ORGS:
        flash("❌ 이 차량소속은 사진 업로드 대상이 아닙니다.")
        return redirect(url_for("car_detail", id=id))
    client = _get_r2_client()
    if not client:
        flash("❌ 사진 저장소가 아직 설정되지 않았습니다. 관리자에게 R2 환경변수 설정을 요청하세요.")
        return redirect(url_for("car_detail", id=id))
    files = request.files.getlist("photos")
    if not files or not any(f and f.filename for f in files):
        flash("❌ 업로드할 사진을 선택하세요.")
        return redirect(url_for("car_detail", id=id))
    차량번호 = target["차량번호"]
    세차일 = target["세차일"]
    conn = get_wash_db()
    uploaded, failed = _store_wash_photos(conn, client, files, 차량번호, 세차일, current_user.username)
    conn.commit()
    conn.close()
    if uploaded and not failed:
        flash(f"✔ 사진 {uploaded}장 업로드 완료")
    elif uploaded:
        flash(f"✔ 사진 {uploaded}장 업로드 완료 ({failed}장 실패 — 이미지 파일만 업로드 가능)")
    else:
        flash("❌ 업로드된 사진이 없습니다.")
    return redirect(url_for("car_detail", id=id))
# =========================================================
# 세차 사진 슬롯 즉시 업로드 (2026-09-03)
# =========================================================
# 세차완료(내역업로드) 버튼을 누른 "순간"에 슬롯 최대 22장을 한꺼번에 업로드하면
# 현장 LTE 환경에서 체감 대기시간이 길고, 사진이 많을 때는 서버 처리시간이 gunicorn
# 워커 타임아웃에 걸려 요청 자체가 실패하는 사고(ERR_HTTP2_PROTOCOL_ERROR)까지
# 있었다. 그래서 사진을 "찍는 즉시" 이 API로 백그라운드 업로드해두고, 완료처리
# 버튼을 누르는 시점에는 이미 R2에 올라간 사진들을 그대로 인정만 하도록 구조를
# 바꿨다(wash_complete() 쪽 처리는 아래 참고). 같은 슬롯을 다시 찍으면(재촬영)
# 이전에 올라간 사진은 지우고 새 걸로 교체한다.
@app.route("/car_slot_photo_upload/<int:id>", methods=["POST"])
@login_required
def car_slot_photo_upload(id):
    target = _lookup_wash_car_for_photo(id, current_user)
    if not target:
        return jsonify({"ok": False, "message": "차량 정보를 찾을 수 없습니다."}), 404
    if (target["차량소속"] or "").strip() not in PHOTO_UPLOAD_ORGS:
        return jsonify({"ok": False, "message": "사진 업로드 대상이 아닙니다."}), 403
    # 라벨은 클라이언트가 보낸 값을 그대로 믿지 않고, 서버가 알고 있는 슬롯 정의에서
    # slot_key로 조회한 값만 사용한다.
    slot_key = (request.form.get("slot_key") or "").strip()
    label = _SLOT_LABEL_BY_KEY.get(slot_key)
    if not label:
        return jsonify({"ok": False, "message": "알 수 없는 슬롯입니다."}), 400
    f = request.files.get("photo")
    if not f or not f.filename:
        return jsonify({"ok": False, "message": "사진이 없습니다."}), 400
    ext = os.path.splitext(secure_filename(f.filename))[1].lower()
    if ext not in ALLOWED_IMAGE_EXTS:
        return jsonify({"ok": False, "message": "이미지 파일만 올릴 수 있습니다."}), 400
    client = _get_r2_client()
    if not client:
        return jsonify({"ok": False, "message": "사진 저장소가 아직 설정되지 않았습니다."}), 503
    img_bytes = _compress_image_bytes(f)
    if not img_bytes:
        return jsonify({"ok": False, "message": "이미지 처리에 실패했습니다."}), 500
    차량번호, 세차일 = target["차량번호"], target["세차일"]
    key = f"wash_photos/{secure_filename(차량번호)}/{세차일}/{uuid.uuid4().hex}.jpg"
    try:
        client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=img_bytes, ContentType="image/jpeg")
    except Exception as e:
        print(f"[R2] 슬롯 즉시업로드 실패: {e}")
        return jsonify({"ok": False, "message": "업로드에 실패했습니다."}), 502
    conn = get_wash_db()
    # 같은 슬롯(차량번호+세차일+shot_label)에 이미 올라간 사진이 있으면 재촬영으로 보고 교체
    old_rows = conn.execute(
        "SELECT id, r2_key FROM wash_photos WHERE 차량번호=? AND 세차일=? AND shot_label=?",
        (차량번호, 세차일, label)
    ).fetchall()
    uploaded_at = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute(
        """INSERT INTO wash_photos (차량번호, 세차일, r2_key, original_name, shot_label, uploaded_by, uploaded_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (차량번호, 세차일, key, secure_filename(f.filename), label, current_user.username, uploaded_at)
    )
    new_id = cur.lastrowid
    for old in old_rows:
        conn.execute("DELETE FROM wash_photos WHERE id=?", (old["id"],))
    conn.commit()
    conn.close()
    for old in old_rows:
        try:
            client.delete_object(Bucket=R2_BUCKET_NAME, Key=old["r2_key"])
        except Exception as e:
            print(f"[R2] 재촬영으로 교체된 이전 사진 삭제 실패: {e}")
    return jsonify({"ok": True, "photo_id": new_id})

@app.route("/car_slot_photo_clear/<int:id>", methods=["POST"])
@login_required
def car_slot_photo_clear(id):
    """사용자가 완료처리 전에 슬롯 사진을 지우기(✕)만 하고 다시 찍지 않은 경우,
    이미 백그라운드로 올라가 있던 사진을 R2/DB에서 함께 정리한다."""
    target = _lookup_wash_car_for_photo(id, current_user)
    if not target:
        return jsonify({"ok": False}), 404
    slot_key = (request.form.get("slot_key") or "").strip()
    label = _SLOT_LABEL_BY_KEY.get(slot_key)
    if not label:
        return jsonify({"ok": False}), 400
    차량번호, 세차일 = target["차량번호"], target["세차일"]
    conn = get_wash_db()
    old_rows = conn.execute(
        "SELECT id, r2_key FROM wash_photos WHERE 차량번호=? AND 세차일=? AND shot_label=?",
        (차량번호, 세차일, label)
    ).fetchall()
    for old in old_rows:
        conn.execute("DELETE FROM wash_photos WHERE id=?", (old["id"],))
    conn.commit()
    conn.close()
    client = _get_r2_client()
    if client:
        for old in old_rows:
            try:
                client.delete_object(Bucket=R2_BUCKET_NAME, Key=old["r2_key"])
            except Exception as e:
                print(f"[R2] 슬롯 사진 삭제 실패: {e}")
    return jsonify({"ok": True})
@app.route("/car_photo/<int:photo_id>")
@login_required
def car_photo_view(photo_id):
    """R2에 저장된 사진을 짧은 유효기간(1시간)의 서명된 URL로 리다이렉트한다.
    R2 자격증명을 브라우저에 노출하지 않기 위한 방식."""
    conn = get_wash_db()
    row = conn.execute("SELECT r2_key FROM wash_photos WHERE id=?", (photo_id,)).fetchone()
    conn.close()
    if not row:
        return "Not found", 404
    client = _get_r2_client()
    if not client:
        return "사진 저장소가 설정되지 않았습니다.", 500
    try:
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": R2_BUCKET_NAME, "Key": row["r2_key"]},
            ExpiresIn=3600,
        )
    except Exception as e:
        print(f"[R2] presign 실패: {e}")
        return "사진을 불러오지 못했습니다.", 502
    return redirect(url)
@app.route("/car_photo_delete/<int:photo_id>", methods=["POST"])
@login_required
def car_photo_delete(photo_id):
    if not current_user.is_master:
        return "Forbidden", 403
    conn = get_wash_db()
    row = conn.execute("SELECT * FROM wash_photos WHERE id=?", (photo_id,)).fetchone()
    if not row:
        conn.close()
        flash("❌ 사진을 찾을 수 없습니다.")
        return redirect(request.referrer or url_for("wash_list"))
    client = _get_r2_client()
    if client:
        try:
            client.delete_object(Bucket=R2_BUCKET_NAME, Key=row["r2_key"])
        except Exception as e:
            print(f"[R2] 삭제 실패: {e}")
    conn.execute("DELETE FROM wash_photos WHERE id=?", (photo_id,))
    conn.commit()
    conn.close()
    flash("✔ 사진이 삭제되었습니다.")
    return redirect(request.referrer or url_for("wash_list"))
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
# =========================================================
# 촬영 좌표 -> 한글 주소 변환 (카카오 로컬 API, 사진 워터마크용)
# =========================================================
KAKAO_REST_API_KEY = os.environ.get("KAKAO_REST_API_KEY", "")

def _kakao_reverse_geocode(lat, lon):
    """좌표를 카카오 로컬 API로 한글 지번 주소로 변환한다.
    반환 형식은 워터마크에 위→아래로 찍힐 순서(상세 -> 구/군 -> 시/도)의
    문자열 리스트. 키가 없거나 API 실패 시 None (호출부에서 좌표로 대체한다)."""
    if not KAKAO_REST_API_KEY:
        return None
    try:
        resp = _requests.get(
            "https://dapi.kakao.com/v2/local/geo/coord2address.json",
            params={"x": lon, "y": lat},
            headers={"Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"},
            timeout=4,
        )
        if resp.status_code != 200:
            print(f"[카카오 역지오코딩] HTTP {resp.status_code}: {resp.text[:200]}")
            return None
        docs = (resp.json() or {}).get("documents") or []
        if not docs:
            return None
        addr = docs[0].get("address") or {}
        region1 = (addr.get("region_1depth_name") or "").strip()  # 시/도
        region2 = (addr.get("region_2depth_name") or "").strip()  # 구/군
        region3 = (addr.get("region_3depth_name") or "").strip()  # 동/읍/면
        main_no = (addr.get("main_address_no") or "").strip()
        sub_no = (addr.get("sub_address_no") or "").strip()
        if main_no:
            detail = f"{main_no}-{sub_no} {region3}" if sub_no else f"{main_no} {region3}"
        else:
            detail = region3
        lines = [l.strip() for l in [detail, region2, region1] if l and l.strip()]
        return lines or None
    except Exception as e:
        print(f"[카카오 역지오코딩] 실패: {e}")
        return None

@app.route("/api/reverse_geocode")
@login_required
def api_reverse_geocode():
    """세차 사진 촬영 화면에서 GPS 좌표를 한글 주소로 바꿔 워터마크에 쓰기 위한 엔드포인트.
    REST API 키가 노출되면 안 되므로 브라우저가 카카오에 직접 호출하지 않고 이 서버를 거친다."""
    try:
        lat = float(request.args.get("lat", ""))
        lon = float(request.args.get("lon", ""))
    except (TypeError, ValueError):
        return jsonify({"ok": False}), 400
    lines = _kakao_reverse_geocode(lat, lon)
    if not lines:
        return jsonify({"ok": False})
    return jsonify({"ok": True, "lines": lines})
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
    # 차량소속이 사진 업로드 대상(현재 '카일이삼제스퍼')이면, 이 화면엔 별도의
    # 세차 기록 입력 폼/사진 업로드 버튼 없이 "내역업로드" 버튼 하나만 있다.
    # 선택된 사진을 R2에 올리는 것과 완료 처리(wash_history 이관)를 한 번에 처리한다.
    is_photo_org = (row["차량소속"] or "").strip() in PHOTO_UPLOAD_ORGS
    photo_uploaded, photo_failed = 0, 0
    damage_report_created = False
    if is_photo_org:
        # 슬롯별 사진 수집 (외부10+내부5+특이사항7+무인훼손제보5 = 27컷, 전부 선택사항, 1장씩).
        # 무인훼손 제보 슬롯(damage_1~5)도 이제 다른 슬롯과 동일하게 한 장씩 찍는 개별 슬롯이라
        # 같은 루프에서 함께 수집하고, 그중 damage_* 키에 해당하는 파일만 따로 모아
        # 기존 훼손제보(damage_reports)/슬랙 연동에 사용한다.
        #
        # (2026-09-03) 슬롯 사진은 이제 찍는 즉시 /car_slot_photo_upload로 미리 R2에
        # 올라가 있을 수 있다(현장 업로드 체감속도 개선 — 완료 버튼 누르는 순간엔 이미
        # 다 올라가 있어서 그때 가서 새로 올릴 게 없게 하는 게 목적). 그런 슬롯은 여기서
        # 다시 올리지 않고 이미 올라간 걸 그대로 인정한다. 이번 요청에 원본 파일이 직접
        # 첨부돼 있으면(즉시업로드가 실패했거나, 막판에 재촬영해서 아직 확인 전인 경우)
        # 그 파일을 우선 처리하고 — 기존에 올라가 있던 스테이징 사진이 있었다면 새 걸로
        # 교체한다.
        existing_rows = conn.execute(
            "SELECT shot_label, r2_key FROM wash_photos WHERE 차량번호=? AND 세차일=?",
            (row["차량번호"], row["세차일"])
        ).fetchall()
        existing_key_by_label = {r["shot_label"]: r["r2_key"] for r in existing_rows if r["shot_label"]}

        slot_files, slot_labels, damage_files = [], [], []
        staged_labels = []       # 이미 업로드되어 있어 다시 올릴 필요 없는 라벨들
        needs_client = False     # 훼손 슬롯이 스테이징돼 있으면 R2에서 다시 읽어와야 함
        for group in PHOTO_SLOT_GROUPS:
            for item in group["items"]:
                label = item["label"]
                f = request.files.get(f"slot_{item['key']}")
                if f and f.filename:
                    slot_files.append(f)
                    slot_labels.append(label)
                    if item["key"] in DAMAGE_SLOT_KEYS:
                        damage_files.append(f)
                elif label in existing_key_by_label:
                    staged_labels.append(label)
                    if item["key"] in DAMAGE_SLOT_KEYS:
                        needs_client = True

        all_files = slot_files
        all_labels = slot_labels
        client = None
        if all_files or needs_client:
            client = _get_r2_client()
            if not client:
                conn.close()
                flash("❌ 사진 저장소가 아직 설정되지 않았습니다. 관리자에게 R2 환경변수 설정을 요청하세요.")
                return redirect(url_for("car_detail", id=id))
        # 이번 요청에서 원본 파일로 새로 처리하는 슬롯 중, 이미 스테이징된 사진이 있던
        # 라벨은 재촬영으로 보고 기존 걸 먼저 정리한다 (그대로 두면 같은 슬롯 사진이
        # 두 장 남는다).
        labels_to_replace = [l for l in slot_labels if l in existing_key_by_label]
        if labels_to_replace:
            for label in labels_to_replace:
                conn.execute(
                    "DELETE FROM wash_photos WHERE 차량번호=? AND 세차일=? AND shot_label=?",
                    (row["차량번호"], row["세차일"], label)
                )
            conn.commit()
            if client:
                for label in labels_to_replace:
                    try:
                        client.delete_object(Bucket=R2_BUCKET_NAME, Key=existing_key_by_label[label])
                    except Exception as e:
                        print(f"[R2] 재촬영 교체 삭제 실패: {e}")
        if all_files:
            photo_uploaded, photo_failed = _store_wash_photos(
                conn, client, all_files, row["차량번호"], row["세차일"], current_user.username,
                labels=all_labels
            )
        photo_uploaded += len(staged_labels)  # 이미 올라가 있던 사진들도 등록 수량에 포함

        # 이미 업로드돼 있던 무인훼손 슬롯은 damage_reports/슬랙 연동을 위해 R2에서
        # 바이트를 다시 받아와 기존 로직(FileStorage 기반 _save_damage_photo)에 그대로
        # 넘길 수 있도록 감싼다.
        if needs_client:
            for item in PHOTO_SLOT_GROUPS[-1]["items"]:
                key = existing_key_by_label.get(item["label"])
                if not key or item["label"] in labels_to_replace:
                    continue
                try:
                    obj = client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
                    body = obj["Body"].read()
                    damage_files.append(FileStorage(
                        stream=io.BytesIO(body), filename=f"{item['key']}.jpg", content_type="image/jpeg"
                    ))
                except Exception as e:
                    print(f"[훼손제보] 기업로드 사진 재조회 실패: {e}")

        # 무인훼손 제보 슬롯에 사진이 있으면, 기존 훼손제보(damage_reports)/슬랙 알림
        # 파이프라인에도 함께 등록한다 (damage_submit()과 동일한 로직 재사용).
        if damage_files:
            try:
                damage_location = (request.form.get("damage") or "").strip() or "위치 미기재 (세차 중 무인훼손 사진 첨부)"
                description = (request.form.get("etc") or "").strip()
                saved_names = []
                for f in damage_files:
                    try:
                        f.stream.seek(0)
                    except Exception:
                        pass
                    fname = _save_damage_photo(f)
                    if fname:
                        saved_names.append(fname)
                dmg_fields = ["photo_front", "photo_damage1", "photo_damage2", "photo_damage3", "photo_damage4", "photo_damage5"]
                dmg_values = {field: None for field in dmg_fields}
                for field, fname in zip(dmg_fields[1:], saved_names):  # photo_front는 비워두고 damage1~5부터 채움
                    dmg_values[field] = fname
                created_at = now_kst().strftime("%Y-%m-%d %H:%M")
                # damage_reports 테이블은 wash.db가 아니라 계정 DB(USER_DB_PATH)에 있으므로
                # 별도 커넥션을 연다 (damage_submit()과 동일한 방식).
                udb = get_user_db()
                dcur = udb.execute(
                    """INSERT INTO damage_reports
                       (car_number, wash_date, damage_location, description,
                        photo_front, photo_damage1, photo_damage2,
                        photo_damage3, photo_damage4, photo_damage5,
                        reporter, vendor, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (row["차량번호"], row["세차일"], damage_location, description,
                     dmg_values["photo_front"], dmg_values["photo_damage1"], dmg_values["photo_damage2"],
                     dmg_values["photo_damage3"], dmg_values["photo_damage4"], dmg_values["photo_damage5"],
                     current_user.username, getattr(current_user, "vendor", "") or "",
                     created_at)
                )
                report_id = dcur.lastrowid
                udb.commit()
                photos_for_slack = []
                for field, fname in zip(dmg_fields, [dmg_values[f] for f in dmg_fields]):
                    if fname:
                        photos_for_slack.append((field, fname, os.path.join(DAMAGE_UPLOAD_DIR, fname)))
                car_org = _lookup_car_org(row["차량번호"])
                slack_ts = _send_damage_slack({
                    "car_number": row["차량번호"], "car_org": car_org, "wash_date": row["세차일"],
                    "damage_location": damage_location, "description": description,
                    "reporter": current_user.username,
                    "vendor": getattr(current_user, "vendor", "") or "",
                    "photos": photos_for_slack,
                }, APP_BASE_URL)
                if slack_ts:
                    udb.execute("UPDATE damage_reports SET slack_ts=? WHERE id=?", (slack_ts, report_id))
                    udb.commit()
                udb.close()
                damage_report_created = True
            except Exception as e:
                print(f"[훼손제보 연동] 오류: {e}")
    done_date = today_kst()
    try:
        cur.execute(
            """
            INSERT INTO wash_history
            (차량번호, 차종명, 차량소속, 스팟, 주소,
             지역시도, 지역구군, 업체, 세차완료일, 세차일,
             주행거리, 훼손, 경고등, 특이사항, 작업자, 원본ID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["차량번호"], row["차종명"], row["차량소속"], row["스팟"], row["주소"],
                row["지역시도"], row["지역구군"], row["업체"], done_date, row["세차일"],
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
    if is_photo_org:
        if photo_uploaded and photo_failed:
            flash(f"✔ 세차 내역이 업로드되었습니다. (사진 {photo_uploaded}장 등록, {photo_failed}장 실패 — 이미지 파일만 가능)")
        elif photo_uploaded:
            flash(f"✔ 세차 내역이 업로드되었습니다. (사진 {photo_uploaded}장 등록)")
        else:
            flash("✔ 세차 내역이 업로드되었습니다.")
        if damage_report_created:
            flash("🚩 무인훼손 제보도 함께 접수되어 슬랙으로 전송되었습니다.")
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
    page = request.args.get("page", 1, type=int) or 1
    per_page = request.args.get("per_page", 10, type=int) or 10
    if per_page not in (10, 50, 100):
        per_page = 10
    if page < 1:
        page = 1
    conn = get_wash_db()
    cur = conn.cursor()
    where_sql = " WHERE 1=1"
    params = []
    scope_sql, scope_params = scoped_condition("wash_history", current_user)
    # (2026-09-03) 차량소속(피플카/휴맥스 같은 차량 운영사) 담당자 계정은 role='staff'이지만
    # 실제로 세차를 수행하는 작업자가 아니라 자기 차량소속 차량들의 완료 현황을 "보기만" 하는
    # 계정이다 — 아래 작업자=로그인아이디 좁히기를 적용하면 자기가 세차한 적이 없으니 항상
    # 0건이 되어버린다. 그래서 차량소속 스코프 계정은 이 좁히기 대상에서 제외한다.
    if current_user.is_staff and not _user_fleets(current_user.username):
        # 완료 현황은 계정(작업자)별 실적 화면이어야 하는데, scoped_condition()은 업체+담당
        # 지역까지만 걸러줘서 같은 지역에 작업자 계정이 여러 개 등록돼 있으면(예: 경기도
        # 의정부시에 green63/green109 둘 다 배정) 서로의 완료 건이 섞여 보였다. 이 화면에
        # 한해서만, 개별 작업자(staff) 계정은 로그인 아이디와 작업자 필드가 일치하는 자기
        # 실적만 보이게 추가로 좁힌다 (master/admin은 기존처럼 전체/업체 전체를 그대로 본다).
        scope_sql += " AND 작업자 = ?"
        scope_params = scope_params + [current_user.username]
    where_sql += scope_sql
    params += scope_params
    if s:
        where_sql += " AND (차량번호 LIKE ? OR 스팟 LIKE ?)"
        params += [f"%{s}%", f"%{s}%"]
    if r1:
        where_sql += " AND 지역시도=?"
        params.append(r1)
    if r2:
        where_sql += " AND 지역구군=?"
        params.append(r2)
    if org:
        where_sql += " AND 차량소속=?"
        params.append(org)
    if sp:
        where_sql += " AND 스팟=?"
        params.append(sp)
    if vendor and current_user.is_master:
        where_sql += " AND 업체=?"
        params.append(vendor)
    if start and end:
        # 날짜는 더 이상 기본 필터가 아니고, 필요할 때만 선택적으로 기간을 지정하는 용도
        where_sql += " AND 세차완료일 BETWEEN ? AND ?"
        params += [start, end]

    # 차량번호나 스팟으로 검색하는 게 일반적인 사용 패턴이라, 날짜 상관없이 전체
    # 완료 이력을 대상으로 하고 페이지네이션으로 나눠서 보여준다.
    filtered_count = cur.execute(
        "SELECT COUNT(*) AS c FROM wash_history" + where_sql, params
    ).fetchone()["c"]
    total_pages = max(1, -(-filtered_count // per_page))
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    rows = cur.execute(
        "SELECT * FROM wash_history" + where_sql + " ORDER BY id DESC LIMIT ? OFFSET ?",
        params + [per_page, offset]
    ).fetchall()

    region1 = filter_distinct_values(cur, "wash_history", "지역시도", scope_sql, scope_params)
    region2 = filter_distinct_values(cur, "wash_history", "지역구군", scope_sql, scope_params)
    car_org_list = filter_distinct_values(cur, "wash_history", "차량소속", scope_sql, scope_params)
    spot_list = filter_distinct_values(cur, "wash_history", "스팟", scope_sql, scope_params)
    vendor_list = filter_distinct_values(cur, "wash_history", "업체", scope_sql, scope_params)
    today_completed_count = cur.execute(
        "SELECT COUNT(*) AS c FROM wash_history WHERE 세차완료일 = ?" + scope_sql,
        [today_str] + scope_params
    ).fetchone()["c"]
    total_completed_count = cur.execute(
        "SELECT COUNT(*) AS c FROM wash_history WHERE 1=1" + scope_sql,
        scope_params
    ).fetchone()["c"]
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
        today_completed_count=today_completed_count,
        total_completed_count=total_completed_count,
        filtered_count=filtered_count,
        current_page=page,
        total_pages=total_pages,
        per_page=per_page
    )
# =============================================
# =========================================================
# 누락 라우트 스텁 / 기능 추가
# =========================================================
@app.route("/support_manage")
@login_required
def support_manage():
    if not (current_user.is_master or current_user.is_admin):
        flash("\u274c 권한이 없습니다.")
        return redirect(url_for("dashboard"))
    selected_status = request.args.get("status", "")
    page = request.args.get("page", 1, type=int)
    conn = get_user_db()
    if selected_status:
        rows = conn.execute(
            "SELECT * FROM support_tickets WHERE status=? ORDER BY created_at DESC",
            [selected_status]
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM support_tickets ORDER BY created_at DESC"
        ).fetchall()
    conn.close()
    page_rows, current_page, total_pages = paginate_list(rows, page, per_page=10)
    return render_template(
        "support_manage.html", rows=rows, page_rows=page_rows,
        current_page=current_page, total_pages=total_pages,
        selected_status=selected_status
    )
@app.route("/support_reply/<int:ticket_id>", methods=["POST"])
@login_required
def support_reply(ticket_id):
    if not (current_user.is_master or current_user.is_admin):
        flash("\u274c 권한이 없습니다.")
        return redirect(url_for("dashboard"))
    status = request.form.get("status", "접수")
    admin_reply = request.form.get("admin_reply", "")
    conn = get_user_db()
    conn.execute(
        "UPDATE support_tickets SET status=?, admin_reply=?, updated_at=? WHERE id=?",
        [status, admin_reply, today_kst(), ticket_id]
    )
    conn.commit()
    conn.close()
    flash("\u2714 저장되었습니다.")
    return redirect(url_for("support_manage"))
@app.route("/support_delete/<int:ticket_id>", methods=["POST"])
@login_required
def support_delete(ticket_id):
    if not current_user.is_master:
        flash("\u274c 마스터 계정만 삭제할 수 있습니다.")
        return redirect(url_for("support_manage"))
    conn = get_user_db()
    conn.execute("DELETE FROM support_tickets WHERE id=?", [ticket_id])
    conn.commit()
    conn.close()
    flash("\u2714 삭제되었습니다.")
    return redirect(url_for("support_manage"))
@app.route("/support_bulk_delete", methods=["POST"])
@login_required
def support_bulk_delete():
    if not current_user.is_master:
        return "Forbidden", 403
    ids = request.form.getlist("ids")
    if not ids:
        flash("선택된 항목이 없습니다.")
        return redirect(url_for("support_manage"))
    deleted = 0
    conn = get_user_db()
    for raw_id in ids:
        try:
            rid = int(raw_id)
        except (ValueError, TypeError):
            continue
        conn.execute("DELETE FROM support_tickets WHERE id=?", (rid,))
        deleted += 1
    conn.commit()
    conn.close()
    flash(f"✅ {deleted}건이 삭제되었습니다.")
    return redirect(url_for("support_manage"))
@app.route("/support_chat", methods=["GET", "POST"])
@login_required
def support_chat():
    return redirect(url_for("dashboard"))
@app.route("/api/support_alerts_poll")
@login_required
def support_alerts_poll():
    return jsonify({"alerts": [], "count": 0})
@app.route("/wash_list_delete", methods=["POST"])
@login_required
def wash_list_delete():
    if not current_user.is_master:
        flash("\u274c 마스터 계정만 삭제할 수 있습니다.")
        return redirect(url_for("wash_list"))
    ids = request.form.getlist("ids")
    return_query = request.form.get("return_query", "")
    if ids:
        conn = get_wash_db()
        conn.execute(
            "DELETE FROM wash_list WHERE id IN ({})".format(",".join("?" * len(ids))),
            ids
        )
        conn.commit()
        conn.close()
        flash(f"\u2714 {len(ids)}건 삭제되었습니다.")
    return redirect(url_for("wash_list") + ("?" + return_query if return_query else ""))
@app.route("/wash_status_delete", methods=["POST"])
@login_required
def wash_status_delete():
    if not current_user.is_master:
        flash("\u274c 마스터 계정만 삭제할 수 있습니다.")
        return redirect(url_for("wash_status"))
    ids = request.form.getlist("ids")
    return_query = request.form.get("return_query", "")
    if ids:
        conn = get_wash_db()
        conn.execute(
            "DELETE FROM wash_history WHERE id IN ({})".format(",".join("?" * len(ids))),
            ids
        )
        conn.commit()
        conn.close()
        flash(f"\u2714 {len(ids)}건 삭제되었습니다.")
    return redirect(url_for("wash_status") + ("?" + return_query if return_query else ""))
# =========================================================
# 세차 내역 조회 (완료 현황 전용, 읽기 전용)
# =========================================================
@app.route("/wash_record/<int:id>")
@login_required
def wash_record(id):
    """완료 현황(wash_status)에서만 진입하는 읽기 전용 상세 화면.
    wash_history.id 기준으로 조회하며, 세차 기록 입력 폼이나 완료 처리
    버튼은 없다 — 이미 완료된 오더를 확인만 하는 화면이다."""
    conn = get_wash_db()
    cur = conn.cursor()
    query = "SELECT * FROM wash_history WHERE id=?"
    params = [id]
    scope_sql, scope_params = scoped_condition("wash_history", current_user)
    query += scope_sql
    params += scope_params
    car = cur.execute(query, params).fetchone()
    conn.close()
    if not car:
        return "❌ 세차 내역을 찾을 수 없습니다.", 404
    # 차량소속이 사진 업로드 대상(PHOTO_UPLOAD_ORGS)인 경우에만 사진 섹션 노출.
    # 사진은 완료 처리 시점에 (이월 여부와 무관한) 원래 세차일 기준으로 저장되므로,
    # 완료일이 아니라 wash_history에 함께 저장해 둔 세차일로 조회해야 이월된 오더도
    # 정확히 찾아진다 (세차일 컬럼이 없는 과거 행은 완료일로 대신 채워져 있다).
    show_photo_section = (car["차량소속"] or "").strip() in PHOTO_UPLOAD_ORGS
    photo_lookup_date = car["세차일"] if "세차일" in car.keys() and car["세차일"] else car["세차완료일"]
    car_photos = _sort_photos_by_slot_order(_get_wash_photos(car["차량번호"], photo_lookup_date)) if show_photo_section else []
    return render_template(
        "wash_record.html", car=car,
        show_photo_section=show_photo_section, car_photos=car_photos,
        r2_configured=bool(_get_r2_client())
    )
SLACK_DAMAGE_WEBHOOK = os.environ.get("SLACK_DAMAGE_WEBHOOK", "")
SLACK_BOT_TOKEN      = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL_ID     = os.environ.get("SLACK_CHANNEL_ID", "")
# 차량청결 VOC 채널(#피플카-차량청결voc, private) 읽기 전용 봇 토큰.
# 훼손제보 전송용 SLACK_BOT_TOKEN과는 별개의 앱/토큰을 쓰는 것을 권장한다
# (기존 앱에 스코프를 추가하면 워크스페이스 관리자 재승인이 필요해 번거로움).
# 새 앱을 만들었다면 SLACK_VOC_BOT_TOKEN 환경변수에 그 Bot User OAuth Token을 넣고,
# 봇을 #피플카-차량청결voc 채널에 /invite 한 뒤 groups:history, users:read 스코프로 설치하면 된다.
# 별도 설정이 없으면 기존 SLACK_BOT_TOKEN을 그대로 사용한다(하위 호환).
SLACK_VOC_BOT_TOKEN  = os.environ.get("SLACK_VOC_BOT_TOKEN", "") or SLACK_BOT_TOKEN
SLACK_VOC_CHANNEL_ID = os.environ.get("SLACK_VOC_CHANNEL_ID", "C0785K12R4G")
APP_BASE_URL = os.environ.get("APP_BASE_URL", "https://turucar-wash-system-production.up.railway.app")
# 웹 푸시(브라우저/PWA 알림) — VAPID 키. Railway 환경변수에 VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY 설정.
# 키가 없으면 앱은 정상 동작하되 푸시 발송만 조용히 스킵된다.
VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "")
VAPID_CONTACT_EMAIL = os.environ.get("VAPID_CONTACT_EMAIL", "admin@peoplecar.co.kr")
try:
    from pywebpush import webpush, WebPushException
    _WEBPUSH_AVAILABLE = True
except Exception:
    _WEBPUSH_AVAILABLE = False
ALLOWED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp"}
_PHOTO_MAX_PX = 3840   # 최대 해상도 (px) — 카메라 촬영 요청 해상도(3840x2160)만큼은 리사이즈로
                       # 깎이지 않게 재상향 (2026-09-02). 데스크탑 사진뷰어에 확대(최대 4배)가
                       # 생기면서 2400px 저장본은 확대했을 때 눈에 띄게 흐릿해 보인다는 문의가
                       # 있었다 — 화면에 보이는 해상도가 아니라 실제로 저장되는 픽셀 수 자체가
                       # 부족했던 것. 주의: 이 값을 올려도 "이미" R2에 저장된 기존 사진은 그
                       # 당시 설정으로 이미 압축되어 저장된 것이라 소급 적용되지 않는다 — 이
                       # 값 배포 이후 새로 찍거나 업로드하는 사진부터 더 높은 해상도로 저장된다.
_PHOTO_QUALITY = 95    # JPEG 압축 품질 (%) — 화질 불만 접수 후 92 → 95로 재상향 (2026-09-01)

def _save_damage_photo(file_obj):
    """사진 저장 — Pillow 사용 시 리사이즈+압축 후 저장 (용량 절감)."""
    if not file_obj or not file_obj.filename:
        return None
    ext = os.path.splitext(secure_filename(file_obj.filename))[1].lower()
    if ext not in ALLOWED_IMAGE_EXTS:
        return None
    os.makedirs(DAMAGE_UPLOAD_DIR, exist_ok=True)

    if _PIL_AVAILABLE:
        try:
            img = _PILImage.open(file_obj.stream)

            # 저장 전 원본 색 프로필(ICC)을 미리 챙겨둔다 — 아이폰 등에서 Display P3 같은
            # 광색역 프로필로 찍힌 사진은, 이 프로필을 저장 시 함께 넣어주지 않으면 브라우저가
            # 픽셀값을 sRGB로 잘못 해석해서 사진이 탁하거나 푸르스름하게 보이는 색 틀어짐이
            # 생긴다(2026-09-01, "사진이 파랗게 보인다" 제보로 확인).
            icc_profile = img.info.get("icc_profile")

            # EXIF 회전 보정 (핸드폰 세로 사진) — exif_transpose가 미러링 포함 8종 orientation을
            # 전부 정확히 처리하고 처리 후 EXIF orientation 태그도 정리해준다.
            try:
                img = _ImageOps.exif_transpose(img)
            except Exception:
                pass

            # RGBA / 팔레트 → RGB 변환
            if img.mode in ("RGBA", "P", "LA"):
                img = img.convert("RGB")
            elif img.mode != "RGB":
                img = img.convert("RGB")

            # 리사이즈 (긴 변이 _PHOTO_MAX_PX 초과 시)
            w, h = img.size
            if max(w, h) > _PHOTO_MAX_PX:
                ratio = _PHOTO_MAX_PX / max(w, h)
                img = img.resize((int(w * ratio), int(h * ratio)), _PILImage.LANCZOS)

            # 항상 .jpg 로 저장 (원본에 ICC 프로필이 있었다면 그대로 함께 저장해 색 틀어짐 방지)
            fname = f"{uuid.uuid4().hex}.jpg"
            save_kwargs = {"format": "JPEG", "quality": _PHOTO_QUALITY, "optimize": True}
            if icc_profile:
                save_kwargs["icc_profile"] = icc_profile
            img.save(os.path.join(DAMAGE_UPLOAD_DIR, fname), **save_kwargs)
            return fname
        except Exception as e:
            print(f"[Photo] Pillow 압축 실패, 원본 저장: {e}")
            file_obj.stream.seek(0)

    # Pillow 없거나 실패 시 원본 그대로 저장
    fname = f"{uuid.uuid4().hex}{ext}"
    file_obj.save(os.path.join(DAMAGE_UPLOAD_DIR, fname))
    return fname
# =========================================================
# 세차 현장 사진 (Cloudflare R2 저장) — 지정 차량소속 전용
# =========================================================
# 이 차량소속에 한해서만 세차 현장 사진 업로드/관리 화면이 노출된다.
# 소속을 더 늘리고 싶으면 이 set에 추가하면 된다.
# "카일이삼제스퍼"는 추후 추가 예정이라 우선 제외 (2026-08-28 요청).
PHOTO_UPLOAD_ORGS = {"피플카 카셰어링", "휴맥스모빌리티"}

# 세차 사진 촬영 슬롯 정의 (실제 촬영가이드 기준: 외부 10 + 내부 4 + 특이사항 3 = 17컷,
# 전부 1장씩. '무인훼손 제보' 슬롯은 별도(DAMAGE_SLOT_*)로 다루며 여러 장 촬영 가능하고
# 기존 훼손제보(damage_reports)/슬랙 알림 파이프라인에도 함께 등록된다.
# ---- 슬롯별 참고 이미지 (촬영 가이드 미니 아이콘) ----
# 실제 사진 4장(전/후/운전석측면/조수석측면)은 훼손부위 체크에 쓰던 실사 사진을 재사용하고,
# 나머지는 밝은 배경 타일에 맞춰 회색 선화(line-art)로 직접 그렸다.
_ICON_STROKE = 'stroke="#8a93a3" stroke-opacity=".75" stroke-width="1.6" fill="none" stroke-linecap="round" stroke-linejoin="round"'


def _icon_svg(inner):
    return f'<svg viewBox="0 0 200 380" preserveAspectRatio="xMidYMid meet"><g {_ICON_STROKE}>{inner}</g></svg>'


def _icon_corner(mirror=False, rear=False):
    """45˚ 휀더(범퍼 모서리) 아이콘 — mirror=조수석 쪽, rear=후면 램프 모양"""
    body = 'M20 210 L20 170 Q20 140 55 132 L150 132 Q168 140 172 168 L172 230 Q172 250 150 252 L40 252 Q20 250 20 230 Z'
    if not rear:
        lamp = '<ellipse cx="55" cy="165" rx="20" ry="11"/>'
    else:
        lamp = '<rect x="38" y="150" width="30" height="16" rx="4"/>'
    arch = '<path d="M60 252 A34 34 0 0 0 128 252" stroke-opacity=".4"/>'
    wheel = '<circle cx="94" cy="255" r="30" stroke-opacity=".5"/><circle cx="94" cy="255" r="14" stroke-opacity=".3"/>'
    hood_line = '<path d="M40 170 L155 170" stroke-opacity=".25"/>'
    inner = f'<path d="{body}"/>{lamp}{arch}{wheel}{hood_line}'
    if mirror:
        inner = f'<g transform="translate(200,0) scale(-1,1)">{inner}</g>'
    return _icon_svg(inner)


_ICON_DASH = _icon_svg('''
  <path d="M40 280 A70 70 0 0 1 160 280"/>
  <path d="M55 280 A55 55 0 0 1 145 280" stroke-opacity=".3"/>
  <path d="M30 190 Q30 130 70 118 L130 118 Q170 130 170 190 Z"/>
  <circle cx="75" cy="168" r="34"/>
  <line x1="75" y1="168" x2="60" y2="148" stroke-width="2"/>
  <circle cx="125" cy="168" r="34"/>
  <line x1="125" y1="168" x2="140" y2="150" stroke-width="2"/>
  <rect x="92" y="182" width="16" height="10" rx="2" stroke-opacity=".5"/>
''')

_ICON_SEAT_DRIVER = _icon_svg('''
  <path d="M68 60 Q68 44 100 44 Q132 44 132 60 L132 96 Q132 112 100 112 Q68 112 68 96 Z"/>
  <line x1="82" y1="112" x2="82" y2="128"/>
  <line x1="118" y1="112" x2="118" y2="128"/>
  <path d="M46 300 L46 165 Q46 128 100 128 Q154 128 154 165 L154 300 Q154 320 130 322 L70 322 Q46 320 46 300 Z"/>
  <path d="M100 132 L100 300" stroke-opacity=".3" stroke-dasharray="3 5"/>
  <path d="M46 200 Q34 210 38 250 Q40 280 58 300" stroke-opacity=".4"/>
  <path d="M154 200 Q166 210 162 250 Q160 280 142 300" stroke-opacity=".4"/>
''')

_ICON_SEAT_REAR = _icon_svg('''
  <path d="M40 150 Q40 118 70 112 L86 112 Q92 100 100 100 Q108 100 114 112 L130 112 Q160 118 160 150 L160 168 L40 168 Z"/>
  <path d="M30 300 L30 185 Q30 160 60 160 L140 160 Q170 160 170 185 L170 300 Q170 316 150 318 L50 318 Q30 316 30 300 Z"/>
  <path d="M100 164 L100 300" stroke-opacity=".25" stroke-dasharray="3 5"/>
''')

_ICON_BLACKBOX = _icon_svg('''
  <path d="M30 90 Q100 60 170 90" stroke-opacity=".3"/>
  <path d="M92 96 L92 118" stroke-opacity=".5"/>
  <rect x="66" y="118" width="68" height="34" rx="8"/>
  <circle cx="100" cy="135" r="10" stroke-opacity=".7"/>
  <line x1="80" y1="152" x2="76" y2="168" stroke-opacity=".35"/>
  <line x1="120" y1="152" x2="124" y2="168" stroke-opacity=".35"/>
''')

_ICON_CENTER_FASCIA = _icon_svg('''
  <rect x="46" y="70" width="108" height="60" rx="10"/>
  <rect x="66" y="82" width="68" height="36" rx="6" stroke-opacity=".45"/>
  <path d="M40 150 L40 260 Q40 280 60 282 L140 282 Q160 280 160 260 L160 150 Z"/>
  <line x1="52" y1="170" x2="76" y2="164" stroke-opacity=".5"/>
  <line x1="52" y1="182" x2="76" y2="176" stroke-opacity=".5"/>
  <line x1="52" y1="194" x2="76" y2="188" stroke-opacity=".5"/>
  <line x1="148" y1="170" x2="124" y2="164" stroke-opacity=".5"/>
  <line x1="148" y1="182" x2="124" y2="176" stroke-opacity=".5"/>
  <line x1="148" y1="194" x2="124" y2="188" stroke-opacity=".5"/>
  <circle cx="100" cy="230" r="18" stroke-opacity=".5"/>
  <rect x="80" y="250" width="40" height="14" rx="4" stroke-opacity=".35"/>
''')

_ICON_CARD = _icon_svg('''
  <rect x="42" y="140" width="116" height="76" rx="10"/>
  <rect x="56" y="156" width="26" height="18" rx="3" stroke-opacity=".6"/>
  <line x1="56" y1="188" x2="132" y2="188" stroke-opacity=".4"/>
  <line x1="56" y1="200" x2="104" y2="200" stroke-opacity=".3"/>
''')

_ICON_DAMAGE = _icon_svg('''
  <path d="M20 90 Q100 60 180 90 L180 290 Q100 320 20 290 Z" stroke-opacity=".55"/>
  <path d="M20 150 Q100 122 180 150" stroke-opacity=".25"/>
  <path d="M20 230 Q100 202 180 230" stroke-opacity=".2"/>
  <path d="M55 140 L90 175 L80 195 L130 245" stroke-opacity=".9" stroke-width="2.2"/>
''')

DAMAGE_SLOT_LABEL = "무인훼손 제보"
DAMAGE_SLOT_MAX = 5  # damage_reports 테이블의 photo_damage1~5 컬럼 수에 맞춤
DAMAGE_SLOT_ICON = "📷"

PHOTO_SLOT_GROUPS = [
    {
        "group": "외부",
        "items": [
            {"key": "ext_1",  "label": "전범퍼 정면",          "icon_type": "photo", "icon_src": "wash_slot_ref/slot_ext_1.png"},
            {"key": "ext_2",  "label": "전범퍼 (운전석 45˚)", "icon_type": "photo", "icon_src": "wash_slot_ref/slot_ext_2.png"},
            {"key": "ext_3",  "label": "운전석 전측면",        "icon_type": "photo", "icon_src": "wash_slot_ref/slot_ext_3.png"},
            {"key": "ext_4",  "label": "운전석 후측면",        "icon_type": "photo", "icon_src": "wash_slot_ref/slot_ext_4.png"},
            {"key": "ext_5",  "label": "후범퍼 (운전석 45˚)", "icon_type": "photo", "icon_src": "wash_slot_ref/slot_ext_5.png"},
            {"key": "ext_6",  "label": "후범퍼 정면",          "icon_type": "photo", "icon_src": "wash_slot_ref/slot_ext_6.png"},
            {"key": "ext_7",  "label": "후범퍼 (조수석 45˚)", "icon_type": "photo", "icon_src": "wash_slot_ref/slot_ext_7.png"},
            {"key": "ext_8",  "label": "조수석 후측면",        "icon_type": "photo", "icon_src": "wash_slot_ref/slot_ext_8.png"},
            {"key": "ext_9",  "label": "조수석 전측면",        "icon_type": "photo", "icon_src": "wash_slot_ref/slot_ext_9.png"},
            {"key": "ext_10", "label": "전범퍼 (조수석 45˚)", "icon_type": "photo", "icon_src": "wash_slot_ref/slot_ext_10.png"},
        ],
    },
    {
        "group": "내부",
        "items": [
            {"key": "int_1", "label": "운전석 1열 (계기판·핸들·시트)", "icon_type": "emoji", "icon_src": "📷"},
            {"key": "int_2", "label": "운전석 2열",                    "icon_type": "emoji", "icon_src": "📷"},
            {"key": "int_3", "label": "조수석 2열",                    "icon_type": "emoji", "icon_src": "📷"},
            {"key": "int_4", "label": "조수석 1열 (도어·시트)",        "icon_type": "emoji", "icon_src": "📷"},
            # 계기판(주행거리·경고등 표시부)이 잘 보이도록 별도 슬롯으로 추가 — 기존
            # int_1이 운전석 1열 전체(핸들·시트 포함)를 넓게 찍는 샷이라 계기판 숫자가
            # 잘 안 보인다는 피드백에 따라 계기판만 클로즈업하는 샷을 따로 둔다.
            {"key": "int_5", "label": "계기판 (주행거리·경고등)",     "icon_type": "emoji", "icon_src": "📷"},
        ],
    },
    {
        "group": "특이사항",
        "items": [
            {"key": "etc_blackbox", "label": "블랙박스 작동화면",     "icon_type": "emoji", "icon_src": "📷"},
            {"key": "etc_center",   "label": "센터페시아 및 공조기", "icon_type": "emoji", "icon_src": "📷"},
            {"key": "etc_card",     "label": "카드 사진",             "icon_type": "emoji", "icon_src": "📷"},
            {"key": "etc_trunk",    "label": "트렁크 내부",           "icon_type": "emoji", "icon_src": "📷"},
            # 특정 부위를 정해두지 않은 여분의 슬롯 — 위 항목들에 안 맞는 특이사항이나
            # 애매한 부위를 자유롭게 찍어 남길 수 있게 한다.
            {"key": "etc_extra_1",  "label": "추가 사진 1",           "icon_type": "emoji", "icon_src": "📷"},
            {"key": "etc_extra_2",  "label": "추가 사진 2",           "icon_type": "emoji", "icon_src": "📷"},
            {"key": "etc_extra_3",  "label": "추가 사진 3",           "icon_type": "emoji", "icon_src": "📷"},
        ],
    },
    {
        # 무인훼손 제보: 기존엔 슬롯 1개에 여러 장을 올리고 '완료' 버튼을 눌러야 했는데,
        # 외부/내부 슬롯처럼 한 장씩 바로 찍히는 슬롯 5개로 대체 (완료 버튼 불필요).
        "group": DAMAGE_SLOT_LABEL,
        "hint": "촬영하면 자동으로 훼손 제보가 접수돼요",
        "items": [
            {"key": f"damage_{n}", "label": f"{DAMAGE_SLOT_LABEL} {n}", "icon_type": "emoji", "icon_src": DAMAGE_SLOT_ICON}
            for n in range(1, DAMAGE_SLOT_MAX + 1)
        ],
    },
]
DAMAGE_SLOT_KEYS = {item["key"] for item in PHOTO_SLOT_GROUPS[-1]["items"]}
# 슬롯 key -> 라벨 조회용 (즉시업로드 API에서 클라이언트가 보낸 slot_key를 서버가 신뢰할
# 수 있는 라벨로 변환할 때 사용 — 라벨 자체를 클라이언트 입력값으로 받지 않는다).
_SLOT_LABEL_BY_KEY = {item["key"]: item["label"] for group in PHOTO_SLOT_GROUPS for item in group["items"]}

# 세차 내역 조회(wash_record)에서 사진을 촬영 순서(정면 → 45˚ → 측면 → ... → 무인훼손)
# 그대로 보여주기 위한 라벨 → 순번 매핑. DB 저장 순서(id)에 의존하지 않고 항상 이
# 순서로 정렬한다 (작업자가 슬롯을 건너뛰거나 나중에 추가로 올려도 순서가 흐트러지지 않음).
_PHOTO_LABEL_ORDER = {}
for _grp in PHOTO_SLOT_GROUPS:
    for _item in _grp["items"]:
        _PHOTO_LABEL_ORDER.setdefault(_item["label"], len(_PHOTO_LABEL_ORDER))
# 예전 방식(무인훼손 제보 슬롯 1개에 여러 장 업로드)으로 저장된 과거 사진들은
# shot_label이 "무인훼손 제보"(번호 없음)로 남아있으므로, 새 슬롯들과 같은 자리로 정렬되게 매핑해준다.
_PHOTO_LABEL_ORDER.setdefault(DAMAGE_SLOT_LABEL, _PHOTO_LABEL_ORDER.get(f"{DAMAGE_SLOT_LABEL} 1", len(_PHOTO_LABEL_ORDER)))
del _grp, _item

def _sort_photos_by_slot_order(photos):
    """wash_photos 목록을 촬영 슬롯 순서대로 정렬한다. shot_label이 슬롯 라벨과
    일치하지 않는(예전 데이터 등) 사진은 순서 매핑 목록 뒤쪽에, 원래 순서(업로드 id)
    그대로 붙여 최소한 서로의 상대 순서는 유지한다."""
    unknown = len(_PHOTO_LABEL_ORDER)
    return sorted(
        photos,
        key=lambda p: (_PHOTO_LABEL_ORDER.get(p.get("shot_label"), unknown), p.get("id", 0))
    )

R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "turu-wash-photos")
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else ""

try:
    import boto3
    from botocore.client import Config as _BotoConfig
    from botocore.exceptions import BotoCoreError as _BotoCoreError, ClientError as _BotoClientError
    _BOTO3_AVAILABLE = True
except ImportError:
    _BOTO3_AVAILABLE = False

_r2_client = None
def _get_r2_client():
    """Cloudflare R2용 S3 호환 클라이언트. 환경변수가 없으면 None을 반환한다
    (앱은 정상 구동되고, 사진 업로드 화면에서만 '아직 설정 안 됨' 안내가 뜬다)."""
    global _r2_client
    if not (_BOTO3_AVAILABLE and R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY):
        return None
    if _r2_client is None:
        try:
            _r2_client = boto3.client(
                "s3",
                endpoint_url=R2_ENDPOINT_URL,
                aws_access_key_id=R2_ACCESS_KEY_ID,
                aws_secret_access_key=R2_SECRET_ACCESS_KEY,
                config=_BotoConfig(signature_version="s3v4"),
                region_name="auto",
            )
        except Exception as e:
            print(f"[R2] 클라이언트 생성 실패: {e}")
            return None
    return _r2_client

def _compress_image_bytes(file_obj):
    """업로드된 사진을 EXIF 회전 보정 + 긴 변 _PHOTO_MAX_PX 리사이즈 + JPEG _PHOTO_QUALITY%
    압축 후 bytes로 반환한다 (훼손제보 사진 저장 로직과 동일한 설정). Pillow가 없거나
    처리에 실패하면 원본 바이트를 그대로 반환한다."""
    try:
        file_obj.stream.seek(0)
    except Exception:
        pass
    if not _PIL_AVAILABLE:
        return file_obj.read()
    try:
        img = _PILImage.open(file_obj.stream)
        # 원본 색 프로필(ICC) 보존 — _save_damage_photo와 동일한 이유(아이폰 Display P3
        # 사진의 색 틀어짐/파랗게 보임 방지).
        icc_profile = img.info.get("icc_profile")
        try:
            img = _ImageOps.exif_transpose(img)
        except Exception:
            pass
        if img.mode in ("RGBA", "P", "LA"):
            img = img.convert("RGB")
        elif img.mode != "RGB":
            img = img.convert("RGB")
        w, h = img.size
        if max(w, h) > _PHOTO_MAX_PX:
            ratio = _PHOTO_MAX_PX / max(w, h)
            img = img.resize((int(w * ratio), int(h * ratio)), _PILImage.LANCZOS)
        buf = io.BytesIO()
        save_kwargs = {"format": "JPEG", "quality": _PHOTO_QUALITY, "optimize": True}
        if icc_profile:
            save_kwargs["icc_profile"] = icc_profile
        img.save(buf, **save_kwargs)
        return buf.getvalue()
    except Exception as e:
        print(f"[Photo] 압축 실패, 원본 업로드로 대체: {e}")
        try:
            file_obj.stream.seek(0)
            return file_obj.read()
        except Exception:
            return None

_PHOTO_UPLOAD_MAX_WORKERS = 6  # 압축+R2 업로드를 몇 장까지 동시에 처리할지

def _store_wash_photos(conn, client, files, 차량번호, 세차일, uploaded_by, labels=None):
    """선택된 사진 파일들을 압축해 R2에 올리고 wash_photos 테이블에 기록한다.
    car_photo_upload(진행중 오더에 사진만 추가)와 wash_complete(사진 업로드 대상 소속의
    '내역업로드' — 사진 등록과 완료 처리를 한 번에 처리) 양쪽에서 공유하는 로직이다.
    labels가 주어지면 files와 같은 순서로 매칭해 shot_label(촬영 슬롯 라벨, 예:
    '전범퍼 정면')을 함께 저장한다 — 없으면 NULL.
    반환: (uploaded_count, failed_count).

    (2026-09-02) 파일별 압축(Pillow)과 R2 업로드는 서로 독립적이라 스레드풀로 동시에
    처리한다 — R2 업로드는 대부분 네트워크 대기 시간이고 Pillow 처리도 대부분 시간을
    C 확장 안에서 보내(GIL 해제) 스레드로 실제 병렬 이득이 있다. 세차완료 한 번에
    최대 22장까지 순서대로 처리하던 예전 방식은 사진이 많을 때 전체 요청이 gunicorn
    워커 타임아웃(30초)보다 오래 걸려 요청 자체가 통째로 죽는 문제가 있었다(실사용
    중 확인, ERR_HTTP2_PROTOCOL_ERROR로 관측됨). sqlite 커넥션은 스레드 간에 공유하면
    안 되므로, DB 기록만은 전부 끝난 뒤 메인 스레드에서 순서대로 처리한다."""
    uploaded_at = now_kst().strftime("%Y-%m-%d %H:%M:%S")

    def _compress_and_upload(item):
        idx, f = item
        ext = os.path.splitext(secure_filename(f.filename))[1].lower()
        if ext not in ALLOWED_IMAGE_EXTS:
            return (idx, None)
        img_bytes = _compress_image_bytes(f)
        if not img_bytes:
            return (idx, None)
        key = f"wash_photos/{secure_filename(차량번호)}/{세차일}/{uuid.uuid4().hex}.jpg"
        try:
            client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=img_bytes, ContentType="image/jpeg")
        except Exception as e:
            print(f"[R2] 업로드 실패: {e}")
            return (idx, None)
        return (idx, {"key": key, "original_name": secure_filename(f.filename)})

    valid_items = [(idx, f) for idx, f in enumerate(files) if f and f.filename]
    results = {}
    if valid_items:
        max_workers = min(_PHOTO_UPLOAD_MAX_WORKERS, len(valid_items))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for idx, result in pool.map(_compress_and_upload, valid_items):
                results[idx] = result

    uploaded, failed = 0, 0
    for idx in range(len(files)):
        f = files[idx]
        if not f or not f.filename:
            continue
        result = results.get(idx)
        if not result:
            failed += 1
            continue
        label = labels[idx] if labels and idx < len(labels) else None
        try:
            conn.execute(
                """INSERT INTO wash_photos (차량번호, 세차일, r2_key, original_name, shot_label, uploaded_by, uploaded_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (차량번호, 세차일, result["key"], result["original_name"], label, uploaded_by, uploaded_at)
            )
        except Exception as e:
            # R2에는 이미 올라갔는데 DB 기록만 실패한 경우 — 이 한 장 때문에 나머지 사진과
            # 세차 내역(wash_history) 전체가 통째로 날아가면 안 되므로, 실패로 집계만 하고
            # 계속 진행한다 (예전엔 여기서 예외가 그대로 올라가 wash_complete()의 뒷부분
            # — 완료 이력 저장 자체 — 까지 실행되지 못하고 통째로 무산되는 위험이 있었다).
            print(f"[Photo] wash_photos 기록 실패: {e}")
            failed += 1
            continue
        uploaded += 1
    return uploaded, failed

def _lookup_wash_car_for_photo(id, user):
    """id로 세차 오더를 찾는다. 아직 진행중이면 wash_list에서, 이미 완료 처리돼
    wash_list에서 삭제됐으면 wash_history(원본ID)에서 찾는다.
    car_detail과 동일하게 scoped_condition으로 담당 업체/지역 범위를 벗어난
    오더는 조회되지 않게 막는다 (다른 업체 오더에 사진을 붙이는 것 방지).
    반환: {'차량번호':.., '세차일':.., '차량소속':.., '완료':bool} 또는 None."""
    conn = get_wash_db()
    scope_sql, scope_params = scoped_condition("wash_list", user)
    row = conn.execute(
        f"SELECT 차량번호, 세차일, 차량소속 FROM wash_list WHERE id=?{scope_sql}",
        [id] + scope_params
    ).fetchone()
    if row:
        conn.close()
        return {"차량번호": row["차량번호"], "세차일": row["세차일"], "차량소속": row["차량소속"], "완료": False}
    scope_sql, scope_params = scoped_condition("wash_history", user)
    row = conn.execute(
        f"SELECT 차량번호, 세차완료일, 차량소속 FROM wash_history WHERE 원본ID=?{scope_sql} ORDER BY id DESC LIMIT 1",
        [id] + scope_params
    ).fetchone()
    conn.close()
    if row:
        return {"차량번호": row["차량번호"], "세차일": row["세차완료일"], "차량소속": row["차량소속"], "완료": True}
    return None

def _get_wash_photos(차량번호, 세차일):
    conn = get_wash_db()
    rows = conn.execute(
        "SELECT * FROM wash_photos WHERE 차량번호=? AND 세차일=? ORDER BY id DESC",
        (차량번호, 세차일)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
def _lookup_car_org(car_number):
    """차량번호로 차량소속을 조회한다.
    세차 대상(wash_list, 최신순) → 차량마스터(vehicle_master) → 세차이력(wash_history, 최신순)
    순으로 데이터가 있는 곳에서 가져온다 (모두 업로드 파일 기반 데이터)."""
    if not car_number:
        return ""
    try:
        wash_conn = get_wash_db()
        row = wash_conn.execute(
            """SELECT 차량소속 FROM wash_list
               WHERE 차량번호=? AND 차량소속 IS NOT NULL AND TRIM(차량소속)!=''
               ORDER BY 세차일 DESC, id DESC LIMIT 1""",
            (car_number,)
        ).fetchone()
        if not row:
            row = wash_conn.execute(
                """SELECT 차량소속 FROM vehicle_master
                   WHERE 차량번호=? AND 차량소속 IS NOT NULL AND TRIM(차량소속)!=''""",
                (car_number,)
            ).fetchone()
        if not row:
            row = wash_conn.execute(
                """SELECT 차량소속 FROM wash_history
                   WHERE 차량번호=? AND 차량소속 IS NOT NULL AND TRIM(차량소속)!=''
                   ORDER BY id DESC LIMIT 1""",
                (car_number,)
            ).fetchone()
        wash_conn.close()
        return row["차량소속"].strip() if row and row["차량소속"] else ""
    except Exception as e:
        print(f"[Damage] 차량소속 조회 오류: {e}")
        return ""
def _send_damage_slack(report, base_url):
    """슬랙으로 훼손 제보 알림 전송. Bot Token 사용 시 ts 반환 (삭제용)."""
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "🚨 차량 훼손 제보 접수"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*차량번호*\n{report['car_number']}"},
            {"type": "mrkdwn", "text": f"*차량소속*\n{report.get('car_org') or '-'}"},
            {"type": "mrkdwn", "text": f"*세차일자*\n{report['wash_date']}"},
            {"type": "mrkdwn", "text": f"*훼손부위*\n{report['damage_location']}"},
            {"type": "mrkdwn", "text": f"*제보 업체명*\n{report.get('vendor') or '-'}"},
            {"type": "mrkdwn", "text": f"*제보자*\n{report['reporter']}"},
        ]},
    ]
    if report.get("description"):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*상세 내용*\n{report['description']}"}})

    # Bot Token 방식: 텍스트 메시지 전송 후 사진을 Slack에 직접 업로드 (Railway 파일 삭제해도 Slack에 영구 보존)
    if SLACK_BOT_TOKEN and SLACK_CHANNEL_ID:
        try:
            # 1. 텍스트/정보 메시지 먼저 전송 → ts 획득
            resp = _requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                         "Content-Type": "application/json"},
                json={"channel": SLACK_CHANNEL_ID, "blocks": blocks},
                timeout=10
            )
            data = resp.json()
            if not data.get("ok"):
                print(f"[Slack Bot] 메시지 오류: {data.get('error')} needed={data.get('needed')} provided={data.get('provided')} — webhook으로 fallback")
                raise RuntimeError("bot_failed")
            slack_ts = data.get("ts")
            print(f"[Slack Bot] 메시지 전송 성공 ts={slack_ts}")

            # 2. 사진 파일 Slack에 직접 업로드 (새 API: getUploadURLExternal → upload → complete)
            photos = report.get("photos", [])
            label_map = {
                "photo_front": "전면 사진",
                "photo_damage1": "훼손 사진 1", "photo_damage2": "훼손 사진 2",
                "photo_damage3": "훼손 사진 3", "photo_damage4": "훼손 사진 4",
                "photo_damage5": "훼손 사진 5",
            }
            file_ids = []
            for field, fname, fpath in photos:
                if not os.path.exists(fpath):
                    continue
                label = label_map.get(field, "사진")
                file_size = os.path.getsize(fpath)
                # Step A: 업로드 URL 요청
                url_resp = _requests.post(
                    "https://slack.com/api/files.getUploadURLExternal",
                    headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
                    data={"filename": fname, "length": file_size},
                    timeout=10
                )
                url_data = url_resp.json()
                if not url_data.get("ok"):
                    print(f"[Slack Bot] URL 요청 오류: {url_data.get('error')}")
                    continue
                upload_url = url_data["upload_url"]
                file_id   = url_data["file_id"]
                # Step B: 파일 업로드
                with open(fpath, "rb") as f:
                    put_resp = _requests.post(upload_url, data=f, timeout=30)
                if put_resp.status_code != 200:
                    print(f"[Slack Bot] 파일 업로드 실패: {fname}")
                    continue
                file_ids.append({"id": file_id, "title": label})
            # Step C: 업로드 완료 — 채널 스레드에 첨부
            if file_ids:
                comp_resp = _requests.post(
                    "https://slack.com/api/files.completeUploadExternal",
                    headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                             "Content-Type": "application/json"},
                    json={
                        "files": file_ids,
                        "channel_id": SLACK_CHANNEL_ID,
                        "thread_ts": slack_ts,
                    },
                    timeout=10
                )
                comp_data = comp_resp.json()
                if comp_data.get("ok"):
                    print(f"[Slack Bot] 사진 {len(file_ids)}장 업로드 완료")
                else:
                    print(f"[Slack Bot] 사진 완료 오류: {comp_data.get('error')}")
            return slack_ts
        except Exception as e:
            print(f"[Slack Bot] 전송 오류: {e} — webhook으로 fallback")

    # Webhook fallback (Bot Token 없거나 실패 시 — 사진은 URL 링크)
    if SLACK_DAMAGE_WEBHOOK:
        photos = report.get("photos", [])
        label_map = {
            "photo_front": "전면 사진",
            "photo_damage1": "훼손 사진 1", "photo_damage2": "훼손 사진 2",
            "photo_damage3": "훼손 사진 3", "photo_damage4": "훼손 사진 4",
            "photo_damage5": "훼손 사진 5",
        }
        for field, fname, _fpath in photos:
            photo_url = f"{base_url.rstrip('/')}/damage_photo/{fname}"
            label = label_map.get(field, "사진")
            blocks.append({
                "type": "image",
                "title": {"type": "plain_text", "text": label},
                "image_url": photo_url,
                "alt_text": label,
            })
        try:
            resp = _requests.post(SLACK_DAMAGE_WEBHOOK, json={"blocks": blocks}, timeout=10)
            print(f"[Slack Webhook] status={resp.status_code}")
        except Exception as e:
            print(f"[Slack Webhook] 전송 오류: {e}")
    else:
        print("[Slack] SLACK_BOT_TOKEN 또는 SLACK_DAMAGE_WEBHOOK 환경변수가 비어있습니다.")
    return None
@app.context_processor
def inject_damage_badge_count():
    if not current_user.is_authenticated:
        return {"damage_badge_count": 0}
    if not (current_user.is_master or getattr(current_user, 'is_admin', False)):
        return {"damage_badge_count": 0}
    try:
        conn = get_user_db()
        row = conn.execute("SELECT COUNT(*) AS c FROM damage_reports WHERE status='접수'").fetchone()
        conn.close()
        return {"damage_badge_count": row["c"] if row else 0}
    except Exception:
        return {"damage_badge_count": 0}
@app.route("/damage_photo/<filename>")
def serve_damage_photo(filename):
    safe = secure_filename(filename)
    photo_path = os.path.join(DAMAGE_UPLOAD_DIR, safe)
    if not os.path.exists(photo_path):
        return "Not found", 404
    return send_from_directory(DAMAGE_UPLOAD_DIR, safe)
@app.route("/support_submit", methods=["GET", "POST"])
@login_required
def support_submit():
    if request.method == "POST":
        car_number = request.form.get("car_number", "").strip()
        category = request.form.get("category", "").strip()
        message = request.form.get("message", "").strip()
        if not car_number or not category or not message:
            flash("차량번호, 문의 유형, 문의 내용은 필수입니다.")
            return redirect(url_for("support_submit"))
        created_at = now_kst().strftime("%Y-%m-%d %H:%M")
        conn = get_user_db()
        conn.execute(
            """INSERT INTO support_tickets
               (category, car_number, message, requester, requester_role, vendor, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (category, car_number, message,
             current_user.username,
             getattr(current_user, "role", "staff"),
             getattr(current_user, "vendor", "") or "",
             created_at)
        )
        conn.commit()
        conn.close()
        flash("✅ 문의가 접수되었습니다.")
        return redirect(url_for("support_submit"))
    return render_template("support_submit.html")


@app.route("/support_choice")
@login_required
def support_choice():
    return render_template("support_choice.html")
@app.route("/damage_submit", methods=["GET", "POST"])
@login_required
def damage_submit():
    if request.method == "POST":
        car_number = request.form.get("car_number", "").strip()
        wash_date = request.form.get("wash_date", "").strip()
        damage_location = request.form.get("damage_location", "").strip()
        description = request.form.get("description", "").strip()
        if not car_number or not wash_date or not damage_location:
            flash("차량번호, 세차일자, 훼손 부위는 필수입니다.")
            return redirect(url_for("damage_submit"))
        photo_front   = _save_damage_photo(request.files.get("photo_front"))
        photo_damage1 = _save_damage_photo(request.files.get("photo_damage1"))
        photo_damage2 = _save_damage_photo(request.files.get("photo_damage2"))
        photo_damage3 = _save_damage_photo(request.files.get("photo_damage3"))
        photo_damage4 = _save_damage_photo(request.files.get("photo_damage4"))
        photo_damage5 = _save_damage_photo(request.files.get("photo_damage5"))
        created_at = now_kst().strftime("%Y-%m-%d %H:%M")
        conn = get_user_db()
        cur = conn.execute(
            """INSERT INTO damage_reports
               (car_number, wash_date, damage_location, description,
                photo_front, photo_damage1, photo_damage2,
                photo_damage3, photo_damage4, photo_damage5,
                reporter, vendor, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (car_number, wash_date, damage_location, description,
             photo_front, photo_damage1, photo_damage2,
             photo_damage3, photo_damage4, photo_damage5,
             current_user.username,
             getattr(current_user, "vendor", "") or "",
             created_at)
        )
        report_id = cur.lastrowid
        conn.commit()
        # 슬랙 전송 - 사진 경로 포함 (field, fname, fpath)
        photos_for_slack = []
        for field, fname in [
            ("photo_front", photo_front), ("photo_damage1", photo_damage1),
            ("photo_damage2", photo_damage2), ("photo_damage3", photo_damage3),
            ("photo_damage4", photo_damage4), ("photo_damage5", photo_damage5),
        ]:
            if fname:
                fpath = os.path.join(DAMAGE_UPLOAD_DIR, fname)
                photos_for_slack.append((field, fname, fpath))
        # 차량소속 조회 (세차 대상/차량마스터/세차이력 업로드 데이터에서 자동 매핑)
        car_org = _lookup_car_org(car_number)
        slack_ts = _send_damage_slack({
            "car_number": car_number, "car_org": car_org, "wash_date": wash_date,
            "damage_location": damage_location, "description": description,
            "reporter": current_user.username,
            "vendor": getattr(current_user, "vendor", "") or "",
            "photos": photos_for_slack,
        }, APP_BASE_URL)
        if slack_ts:
            conn.execute("UPDATE damage_reports SET slack_ts=? WHERE id=?", (slack_ts, report_id))
            conn.commit()
        conn.close()
        flash("✅ 제보가 접수되었습니다.")
        return redirect(url_for("damage_submit"))
    return render_template("damage_submit.html", today=today_kst())
@app.route("/damage_manage")
@login_required
def damage_manage():
    if not (current_user.is_master or getattr(current_user, 'is_admin', False)):
        flash("접근 권한이 없습니다.")
        return redirect(url_for("dashboard"))
    status_filter = request.args.get("status", "")
    page = request.args.get("page", 1, type=int)
    conn = get_user_db()
    if status_filter:
        rows = conn.execute("SELECT * FROM damage_reports WHERE status=? ORDER BY id DESC", (status_filter,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM damage_reports ORDER BY id DESC").fetchall()
    conn.close()
    page_rows, current_page, total_pages = paginate_list(rows, page, per_page=10)
    return render_template(
        "damage_manage.html", rows=rows, page_rows=page_rows,
        current_page=current_page, total_pages=total_pages,
        selected_status=status_filter
    )
@app.route("/damage_reply/<int:report_id>", methods=["POST"])
@login_required
def damage_reply(report_id):
    if not (current_user.is_master or getattr(current_user, 'is_admin', False)):
        return "Forbidden", 403
    status = request.form.get("status", "접수")
    admin_reply = request.form.get("admin_reply", "")
    updated_at = now_kst().strftime("%Y-%m-%d %H:%M")
    conn = get_user_db()
    conn.execute("UPDATE damage_reports SET status=?, admin_reply=?, updated_at=? WHERE id=?",
                 (status, admin_reply, updated_at, report_id))
    conn.commit()
    conn.close()
    return redirect(url_for("damage_manage"))
@app.route("/damage_delete/<int:report_id>", methods=["POST"])
@login_required
def damage_delete(report_id):
    if not current_user.is_master:
        return "Forbidden", 403
    conn = get_user_db()
    row = conn.execute("SELECT * FROM damage_reports WHERE id=?", (report_id,)).fetchone()
    if row:
        # Slack 메시지 자동 삭제
        if row["slack_ts"] and SLACK_BOT_TOKEN and SLACK_CHANNEL_ID:
            try:
                _requests.post(
                    "https://slack.com/api/chat.delete",
                    headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                             "Content-Type": "application/json"},
                    json={"channel": SLACK_CHANNEL_ID, "ts": row["slack_ts"]},
                    timeout=5
                )
            except Exception:
                pass
        for field in ("photo_front", "photo_damage1", "photo_damage2", "photo_damage3", "photo_damage4", "photo_damage5"):
            fname = row[field]
            if fname:
                try:
                    os.remove(os.path.join(DAMAGE_UPLOAD_DIR, fname))
                except OSError:
                    pass
        conn.execute("DELETE FROM damage_reports WHERE id=?", (report_id,))
        conn.commit()
    conn.close()
    return redirect(url_for("damage_manage"))
@app.route("/damage_bulk_delete", methods=["POST"])
@login_required
def damage_bulk_delete():
    if not current_user.is_master:
        return "Forbidden", 403
    ids = request.form.getlist("ids")
    if not ids:
        flash("선택된 항목이 없습니다.")
        return redirect(url_for("damage_manage"))
    deleted = 0
    conn = get_user_db()
    for raw_id in ids:
        try:
            rid = int(raw_id)
        except (ValueError, TypeError):
            continue
        row = conn.execute("SELECT * FROM damage_reports WHERE id=?", (rid,)).fetchone()
        if row:
            # Slack 메시지 자동 삭제
            if row["slack_ts"] and SLACK_BOT_TOKEN and SLACK_CHANNEL_ID:
                try:
                    _requests.post(
                        "https://slack.com/api/chat.delete",
                        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                                 "Content-Type": "application/json"},
                        json={"channel": SLACK_CHANNEL_ID, "ts": row["slack_ts"]},
                        timeout=5
                    )
                except Exception:
                    pass
            for field in ("photo_front", "photo_damage1", "photo_damage2",
                          "photo_damage3", "photo_damage4", "photo_damage5"):
                try:
                    fname = row[field]
                except (IndexError, KeyError):
                    fname = None
                if fname:
                    try:
                        os.remove(os.path.join(DAMAGE_UPLOAD_DIR, fname))
                    except OSError:
                        pass
            conn.execute("DELETE FROM damage_reports WHERE id=?", (rid,))
            deleted += 1
    conn.commit()
    conn.close()
    flash(f"✅ {deleted}건이 삭제되었습니다.")
    return redirect(url_for("damage_manage"))
@app.route("/damage_slack_delete/<int:report_id>", methods=["POST"])
@login_required
def damage_slack_delete(report_id):
    if not (current_user.is_master or getattr(current_user, 'is_admin', False)):
        return "Forbidden", 403
    if not (SLACK_BOT_TOKEN and SLACK_CHANNEL_ID):
        flash("⚠️ SLACK_BOT_TOKEN 또는 SLACK_CHANNEL_ID 환경변수가 설정되지 않았습니다.")
        return redirect(url_for("damage_manage"))
    conn = get_user_db()
    row = conn.execute("SELECT slack_ts FROM damage_reports WHERE id=?", (report_id,)).fetchone()
    if not row or not row["slack_ts"]:
        flash("삭제할 Slack 메시지가 없습니다.")
        conn.close()
        return redirect(url_for("damage_manage"))
    try:
        resp = _requests.post(
            "https://slack.com/api/chat.delete",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                     "Content-Type": "application/json"},
            json={"channel": SLACK_CHANNEL_ID, "ts": row["slack_ts"]},
            timeout=5
        )
        data = resp.json()
        if data.get("ok"):
            conn.execute("UPDATE damage_reports SET slack_ts=NULL WHERE id=?", (report_id,))
            conn.commit()
            flash("✅ Slack 메시지가 삭제되었습니다.")
        else:
            flash(f"Slack 삭제 실패: {data.get('error', '알 수 없음')}")
    except Exception as e:
        flash(f"Slack 삭제 오류: {e}")
    conn.close()
    return redirect(url_for("damage_manage"))
@app.route("/damage_alerts_poll")
@login_required
def damage_alerts_poll():
    if not (current_user.is_master or getattr(current_user, 'is_admin', False)):
        return jsonify({"count": 0})
    since_id = request.args.get("since_id", 0, type=int)
    conn = get_user_db()
    rows = conn.execute("SELECT id FROM damage_reports WHERE status='접수' AND id > ? ORDER BY id DESC", (since_id,)).fetchall()
    conn.close()
    return jsonify({"count": len(rows), "new_ids": [r["id"] for r in rows]})
# =========================================================
# 차량 관리 — 차량별 훼손관리 대시보드 + AI 훼손 판독(라벨링)
# (2026-09-03 추가) 차량소속(피플카/휴맥스 등 차량 운영사) 담당자가 자기 차량소속
# 차량만 조회할 수 있게 하고, 세차 이력·사진·훼손 제보 이력을 한 화면에서 보여주는
# '훼손제보 관리'와는 별도인 차량 중심 대시보드. AI 훼손 판독은 완료현황 사진마다
# 매번 API를 호출하는 대신, 여기서 모은 라벨 데이터로 가벼운 이미지 분류 모델을
# 자체 학습시켜(반복 호출 비용 없이) 서버에서 직접 판독하는 것이 목표다 — 그 첫
# 단계인 라벨링 도구부터 제공한다.
# =========================================================
def _vehicle_scope_condition(user):
    """vehicle_master 조회용 스코프 조건. vehicle_master는 담당업체 컬럼명이
    scoped_condition()이 쓰는 업체와 달라서 별도로 둔다. 우선순위는 scoped_condition()과
    동일하게 차량소속 배정 > 업체(vendor) 순."""
    if user.is_master or getattr(user, "is_contact_center", False):
        return "", []
    fleets = user.fleets
    if fleets:
        clause = " OR ".join(["차량소속 = ?"] * len(fleets))
        return f" AND ({clause})", list(fleets)
    if user.vendor:
        return " AND 담당업체 = ?", [user.vendor]
    return " AND 1=0", []
def _can_view_vehicle_management():
    return current_user.is_admin or bool(current_user.fleets)
# 세차완료 시 작업자가 입력한 훼손/경고등 메모 중 "특이사항 없음"으로 볼 수 있는 값들.
# 이 값이 아니면(즉 뭔가 적혀 있으면) 차량별 훼손관리 대시보드에서 훼손 관련 건으로 취급한다.
_NO_ISSUE_VALUES = ("", "없음")
def _wash_history_damage_where():
    """훼손 또는 경고등에 뭔가 적힌(=특이사항 있는) wash_history 행만 골라내는 조건."""
    placeholders = ",".join(["?"] * len(_NO_ISSUE_VALUES))
    return (
        f"(TRIM(COALESCE(훼손,'')) NOT IN ({placeholders}) "
        f"OR TRIM(COALESCE(경고등,'')) NOT IN ({placeholders}))",
        list(_NO_ISSUE_VALUES) + list(_NO_ISSUE_VALUES)
    )
# 훼손 부위를 항상 같은 순서(앞모습 → 뒷모습 → 운전석 쪽 → 조수석 쪽)로 보여주기 위한
# 정렬 기준 — car_detail.html의 부위선택 피커(renderCarDamagePicker)가 제공하는 부위
# 이름과 논리적 순서(앞→뒤)를 그대로 따른다. 화면에 찍히는 좌우 반전(조수석 쪽 사진은
# 거울상이라 클릭 순서가 뒤바뀔 수 있음)과는 무관하게, 텍스트로 보여줄 땐 항상 이 순서.
_DAMAGE_PART_ORDER = [
    "조)전범퍼", "운)전범퍼", "보닛(후드)", "전면유리",
    "운)후범퍼", "조)후범퍼", "트렁크", "후면유리",
    "운)전휀더", "운)전도어", "운)후도어", "운)후휀더", "운)스텝",
    "조)전휀더", "조)전도어", "조)후도어", "조)후휀더", "조)스텝",
]
_DAMAGE_PART_RANK = {part: i for i, part in enumerate(_DAMAGE_PART_ORDER)}
def _format_damage_text(text):
    """훼손 텍스트(쉼표로 여러 부위를 나열한 문자열)를 화면에 보여줄 때만 항상 같은 순서로
    재배열한다. 세차완료 화면의 부위선택 피커는 작업자가 부위를 클릭한 순서 그대로 저장하므로
    (예: "운)후도어, 운)후휀더, 조)후도어, 조)전휀더"), 부위 조합이 똑같아도 클릭 순서에 따라
    매번 다르게 보여서 이력을 눈으로 비교하기 어렵다는 피드백에 따라 추가함(2026-09-04).
    DB에 저장된 원본 문자열은 건드리지 않는다 — 정렬은 오직 표시용이고, 신규훼손 판정
    (_compute_new_damage_ids)이나 검색 등은 그대로 원본/토큰 집합 기준으로 동작한다.
    피커의 부위 목록에 없는 자유 서술형 텍스트(쉼표로 안 쪼개지거나, 쪼개져도 목록에 없는
    토큰)는 정해진 순서가 없으므로 원래 순서 그대로 맨 뒤에 붙는다."""
    tokens = _split_damage_tokens(text)
    if not tokens:
        return text
    unknown_rank = len(_DAMAGE_PART_ORDER)
    return ", ".join(sorted(tokens, key=lambda t: _DAMAGE_PART_RANK.get(t, unknown_rank)))
app.jinja_env.filters["damage_order"] = _format_damage_text
def _split_damage_tokens(text):
    """훼손 텍스트를 개별 '부위' 단위로 쪼갠다.
    세차완료 화면의 '부위 선택해서 입력하기' 피커는 선택한 부위들을 ", " 로 이어붙여
    textarea에 채워넣는다 (예: "운)전범퍼, 조)전범퍼" — car_detail.html의
    renderCarDamagePicker: `Array.from(selected).join(', ')`). 그래서 이번 세차에서
    부위가 하나 줄었을 뿐인데(예: "조)전범퍼, 운)전범퍼" → "조)전범퍼", 운전석 쪽은 이미
    수리돼서 이번엔 조수석 쪽만 다시 체크한 경우) 훼손 텍스트 전체를 통째로 비교하면
    "이 문자열은 처음 본다"는 이유만으로 신규 훼손으로 잘못 표시된다. 쉼표 기준으로
    나눠 부위 단위로 비교해야 이런 오탐을 막을 수 있다. 피커를 안 쓰고 자유 서술로 적은
    경우(쉼표가 없는 한 문장)에도 그 문장 전체가 토큰 하나로 취급되어 동일한 방식으로
    비교된다."""
    if not text or not isinstance(text, str):
        return []
    return [t.strip() for t in text.split(",") if t.strip()]
def _compute_new_damage_ids(conn, scope_sql, scope_params):
    """차량별로 '가장 최근' 세차완료 기록에만 신규 훼손 표시 여부를 판단한다(그 결과가 참이면
    wash_history.id로 반환 — 신규 훼손 강조 대상). 같은 차량의 더 오래된 기록들은 — 그 자체가
    당시엔 새로 등장한 표현이었더라도 — 다시 강조하지 않는다. 지금 확인이 필요한 것은
    "가장 최근 세차에서 새로 생긴 훼손이 있는가"이지, 과거 이력 전체를 훑어 처음 등장한
    문구를 전부 표시하는 게 아니기 때문이다(그렇게 하면 오래된 차량일수록 이력 대부분이
    붉게 표시되어 오히려 눈에 띄어야 할 것을 가려버린다).
    '새로 생겼다'의 판단은 훼손 텍스트를 _split_damage_tokens()로 부위 단위로 쪼갠 뒤,
    최신 기록의 부위 중 그 차량의 이전 기록 어디에도 등장한 적 없는 부위가 하나라도 있는지로
    본다(통째 문자열 완전일치가 아님 — 이유는 _split_damage_tokens 참고). 그래도 작업자가
    같은 부위를 다른 표현으로 적으면(예: "조수석 앞범퍼" vs "조)전범퍼") 놓칠 수 있는 한계는
    남아있다."""
    rows = conn.execute(
        "SELECT id, 차량번호, 훼손 FROM wash_history "
        "WHERE TRIM(COALESCE(훼손,'')) NOT IN ('', '없음')" + scope_sql +
        " ORDER BY 차량번호, 세차완료일 ASC, id ASC",
        scope_params
    ).fetchall()
    rows_by_plate = {}
    for r in rows:
        rows_by_plate.setdefault(r["차량번호"], []).append(r)
    new_ids = set()
    for plate_rows in rows_by_plate.values():
        latest = plate_rows[-1]
        prior_tokens = set()
        for r in plate_rows[:-1]:
            prior_tokens.update(_split_damage_tokens(r["훼손"]))
        latest_tokens = _split_damage_tokens(latest["훼손"])
        if any(tok not in prior_tokens for tok in latest_tokens):
            new_ids.add(latest["id"])
    return new_ids
@app.route("/vehicle_damage_dashboard")
@login_required
def vehicle_damage_dashboard():
    if not _can_view_vehicle_management():
        flash("❌ 접근 권한이 없습니다.")
        return redirect(url_for("dashboard"))
    q = request.args.get("q", "").strip()
    org = request.args.get("org", "").strip()
    tab = request.args.get("tab", "all").strip()
    if tab not in ("all", "new", "warning"):
        tab = "all"
    page = request.args.get("page", 1, type=int)
    conn = get_wash_db()
    cur = conn.cursor()
    scope_sql, scope_params = scoped_condition("wash_history", current_user)
    issue_sql, issue_params = _wash_history_damage_where()
    base_query = f"SELECT * FROM wash_history WHERE {issue_sql}" + scope_sql
    base_params = list(issue_params) + list(scope_params)
    if org:
        base_query += " AND 차량소속 = ?"
        base_params.append(org)
    if q:
        base_query += " AND 차량번호 LIKE ?"
        base_params.append(f"%{q}%")
    all_rows = cur.execute(
        base_query + " ORDER BY 세차완료일 DESC, id DESC",
        base_params
    ).fetchall()
    # "신규 훼손" 강조 — 전체 스코프 기준으로 계산해야 하므로 검색/필터 조건과 무관하게 별도 조회한다.
    new_damage_ids = _compute_new_damage_ids(conn, scope_sql, scope_params)
    # 세차 오더(wash_list) 화면의 "전체 / 장기 미세차" 탭과 동일한 패턴 —
    # q/org 검색조건은 그대로 유지한 채, "신규 훼손"/"경고등" 탭을 고르면 그 조건을
    # 만족하는 행 중 각 조건에 해당하는 것만 다시 추려서 페이지네이션한다.
    new_rows = [r for r in all_rows if r["id"] in new_damage_ids]
    # "경고등" 탭: 경고등에 실제로 뭔가 적힌(=없음/공란이 아닌) 행만. 목록/상세 화면의
    # "{{ h['경고등'] or '-' }}" 표시용 '-'는 화면에서만 붙는 placeholder이지 DB에 저장된
    # 값이 아니므로, 여기서는 DB 원본 기준(없음/공란)으로 판단하면 된다.
    warning_rows = [r for r in all_rows if (r["경고등"] or "").strip() not in _NO_ISSUE_VALUES]
    display_rows = new_rows if tab == "new" else (warning_rows if tab == "warning" else all_rows)
    org_list = filter_distinct_values(cur, "wash_history", "차량소속", scope_sql, scope_params)
    conn.close()
    page_rows, current_page, total_pages = paginate_list(display_rows, page, per_page=20)
    return render_template(
        "vehicle_damage_dashboard.html",
        rows=page_rows, current_page=current_page, total_pages=total_pages,
        total_count=len(display_rows), all_count=len(all_rows), new_count=len(new_rows),
        warning_count=len(warning_rows),
        q=q, org=org, org_list=org_list, tab=tab,
        new_damage_ids=new_damage_ids,
    )
@app.route("/vehicle_damage_dashboard/export")
@login_required
def vehicle_damage_dashboard_export():
    if not _can_view_vehicle_management():
        flash("❌ 접근 권한이 없습니다.")
        return redirect(url_for("dashboard"))
    q = request.args.get("q", "").strip()
    org = request.args.get("org", "").strip()
    tab = request.args.get("tab", "all").strip()
    if tab not in ("all", "new", "warning"):
        tab = "all"
    conn = get_wash_db()
    scope_sql, scope_params = scoped_condition("wash_history", current_user)
    issue_sql, issue_params = _wash_history_damage_where()
    query = f"SELECT id, 차량번호, 훼손 AS 훼손부위, 경고등, 세차완료일 FROM wash_history WHERE {issue_sql}" + scope_sql
    params = list(issue_params) + list(scope_params)
    if org:
        query += " AND 차량소속 = ?"
        params.append(org)
    if q:
        query += " AND 차량번호 LIKE ?"
        params.append(f"%{q}%")
    query += " ORDER BY 세차완료일 DESC, id DESC"
    df = pd.read_sql_query(query, conn, params=params)
    if tab == "new":
        new_damage_ids = _compute_new_damage_ids(conn, scope_sql, scope_params)
        df = df[df["id"].isin(new_damage_ids)]
    elif tab == "warning":
        df = df[~df["경고등"].fillna("").str.strip().isin(_NO_ISSUE_VALUES)]
    df = df.drop(columns=["id"])
    df["훼손부위"] = df["훼손부위"].apply(_format_damage_text)
    conn.close()
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="차량별 훼손관리")
        worksheet = writer.sheets["차량별 훼손관리"]
        for column_cells in worksheet.columns:
            max_length = 10
            column_letter = column_cells[0].column_letter
            for cell in column_cells:
                value = "" if cell.value is None else str(cell.value)
                max_length = max(max_length, min(len(value) + 2, 40))
            worksheet.column_dimensions[column_letter].width = max_length
    output.seek(0)
    filename = f"vehicle_damage_{today_kst()}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
@app.route("/vehicle_damage_dashboard/<path:plate>")
@login_required
def vehicle_damage_detail(plate):
    if not _can_view_vehicle_management():
        flash("❌ 접근 권한이 없습니다.")
        return redirect(url_for("dashboard"))
    conn = get_wash_db()
    scope_sql, scope_params = _vehicle_scope_condition(current_user)
    vehicle = conn.execute(
        "SELECT * FROM vehicle_master WHERE 차량번호=?" + scope_sql,
        [plate] + list(scope_params)
    ).fetchone()
    if not vehicle:
        conn.close()
        flash("❌ 해당 차량을 찾을 수 없거나 접근 권한이 없습니다.")
        return redirect(url_for("vehicle_damage_dashboard"))
    wash_history_rows = conn.execute(
        "SELECT * FROM wash_history WHERE 차량번호=? ORDER BY 세차완료일 DESC, id DESC LIMIT 30",
        (plate,)
    ).fetchall()
    conn.close()
    uconn = get_user_db()
    damage_rows = uconn.execute(
        "SELECT * FROM damage_reports WHERE car_number=? ORDER BY id DESC",
        (plate,)
    ).fetchall()
    uconn.close()
    # (2026-09-04) 세차 이력의 사진은 이 페이지에서 별도로 모아 보여주지 않고, 각 행을
    # 눌러서 완료현황의 실제 완료내역(wash_record)으로 이동해 그 안에서 확인하도록 변경.
    return render_template(
        "vehicle_damage_detail.html",
        vehicle=vehicle, wash_history_rows=wash_history_rows,
        damage_rows=damage_rows, plate=plate,
    )
@app.route("/damage_ai_label", methods=["GET", "POST"])
@login_required
def damage_ai_label():
    # 라벨링(학습 데이터 구축)은 실제로 모델을 만들고 운영하는 관리자 작업이라
    # 차량소속 담당자가 아니라 admin/master만 접근할 수 있게 한다.
    if not current_user.is_admin:
        flash("❌ 접근 권한이 없습니다.")
        return redirect(url_for("dashboard"))
    conn = get_wash_db()
    if request.method == "POST":
        photo_id = request.form.get("photo_id", type=int)
        label = request.form.get("label", "").strip()
        if photo_id and label in ("normal", "suspect"):
            photo = conn.execute("SELECT * FROM wash_photos WHERE id=?", (photo_id,)).fetchone()
            if photo:
                now_str = now_kst().strftime("%Y-%m-%d %H:%M:%S")
                existing = conn.execute("SELECT id FROM damage_ai_labels WHERE photo_id=?", (photo_id,)).fetchone()
                if existing:
                    conn.execute(
                        "UPDATE damage_ai_labels SET label=?, labeled_by=?, labeled_at=? WHERE photo_id=?",
                        (label, current_user.username, now_str, photo_id)
                    )
                else:
                    conn.execute(
                        "INSERT INTO damage_ai_labels (photo_id, 차량번호, shot_label, label, labeled_by, labeled_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (photo_id, photo["차량번호"], photo["shot_label"], label, current_user.username, now_str)
                    )
                conn.commit()
        conn.close()
        return redirect(url_for("damage_ai_label"))
    total_photos = conn.execute("SELECT COUNT(*) AS c FROM wash_photos").fetchone()["c"]
    labeled_counts = conn.execute(
        "SELECT label, COUNT(*) AS c FROM damage_ai_labels GROUP BY label"
    ).fetchall()
    label_summary = {"normal": 0, "suspect": 0}
    for r in labeled_counts:
        if r["label"] in label_summary:
            label_summary[r["label"]] = r["c"]
    labeled_total = sum(label_summary.values())
    next_photo = conn.execute(
        """
        SELECT wp.* FROM wash_photos wp
        LEFT JOIN damage_ai_labels dl ON dl.photo_id = wp.id
        WHERE dl.id IS NULL
        ORDER BY wp.id
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    return render_template(
        "damage_ai_label.html",
        next_photo=next_photo, total_photos=total_photos,
        labeled_total=labeled_total, label_summary=label_summary,
        remaining=max(total_photos - labeled_total, 0),
    )
# =========================================================
# 차량청결 VOC (슬랙 연동) + 긴급세차 — 지역 담당자 전달 큐
# =========================================================
_SLACK_USER_NAME_CACHE = {}
def _slack_resolve_user(user_id):
    """슬랙 user id → 표시 이름. users:read 스코프 필요. 실패 시 id 그대로 반환."""
    if not user_id:
        return ""
    if user_id in _SLACK_USER_NAME_CACHE:
        return _SLACK_USER_NAME_CACHE[user_id]
    name = user_id
    if SLACK_VOC_BOT_TOKEN:
        try:
            resp = _requests.get(
                "https://slack.com/api/users.info",
                headers={"Authorization": f"Bearer {SLACK_VOC_BOT_TOKEN}"},
                params={"user": user_id},
                timeout=5
            )
            data = resp.json()
            if data.get("ok"):
                profile = data.get("user", {}).get("profile", {})
                name = profile.get("real_name") or data["user"].get("name") or user_id
        except Exception as e:
            print(f"[VOC Slack] users.info 오류: {e}")
    _SLACK_USER_NAME_CACHE[user_id] = name
    return name
_SLACK_ERROR_HINTS = {
    "not_in_channel": "봇이 #피플카-차량청결voc 채널에 초대되어 있지 않습니다. 슬랙에서 해당 채널에 봇을 /invite 해주세요.",
    "channel_not_found": "채널 ID(SLACK_VOC_CHANNEL_ID)가 올바르지 않습니다.",
    "missing_scope": "슬랙 앱에 groups:history(비공개 채널 읽기) 권한이 없습니다. OAuth 스코프 추가 후 워크스페이스에 재설치해주세요.",
    "invalid_auth": "SLACK_VOC_BOT_TOKEN이 유효하지 않습니다. 토큰을 다시 확인해주세요.",
    "account_inactive": "슬랙 앱/토큰이 비활성화된 상태입니다.",
    "token_revoked": "슬랙 봇 토큰이 폐기(재발급)되었습니다.",
}
def _sync_voc_from_slack(limit=50):
    """#피플카-차량청결voc 채널의 최근 메시지를 voc_items 테이블로 동기화.
    SLACK_VOC_BOT_TOKEN(전용 봇, 없으면 SLACK_BOT_TOKEN)이 해당(private) 채널에
    초대되어 있어야 하며 groups:history, users:read 스코프가 필요하다.
    반환값: (신규 건수, 오류 메시지 또는 None)"""
    if not SLACK_VOC_BOT_TOKEN or not SLACK_VOC_CHANNEL_ID:
        return 0, "SLACK_VOC_BOT_TOKEN 환경변수가 설정되어 있지 않습니다."
    try:
        resp = _requests.get(
            "https://slack.com/api/conversations.history",
            headers={"Authorization": f"Bearer {SLACK_VOC_BOT_TOKEN}"},
            params={"channel": SLACK_VOC_CHANNEL_ID, "limit": limit},
            timeout=10
        )
        data = resp.json()
        if not data.get("ok"):
            err = data.get("error", "알 수 없는 오류")
            hint = _SLACK_ERROR_HINTS.get(err, "")
            message = f"슬랙 API 오류: {err}" + (f" — {hint}" if hint else "")
            print(f"[VOC Slack] sync 오류: {message}")
            return 0, message
        messages = data.get("messages", [])
        conn = get_user_db()
        synced_at = now_kst().strftime("%Y-%m-%d %H:%M")
        new_count = 0
        for m in reversed(messages):  # 오래된 순으로 삽입
            if m.get("subtype"):  # 채널 입장/시스템 메시지 등은 제외
                continue
            ts = m.get("ts")
            text = (m.get("text") or "").strip()
            if not ts or not text:
                continue
            exists = conn.execute("SELECT 1 FROM voc_items WHERE slack_ts=?", (ts,)).fetchone()
            if exists:
                continue
            user_id = m.get("user", "")
            author = _slack_resolve_user(user_id) if user_id else (m.get("username") or "슬랙")
            permalink = f"https://peoplecarhq.slack.com/archives/{SLACK_VOC_CHANNEL_ID}/p{ts.replace('.', '')}"
            photos = []
            for f in m.get("files", []) or []:
                mimetype = f.get("mimetype", "")
                url_private = f.get("url_private")
                if url_private and (mimetype.startswith("image/") or f.get("filetype") in ("jpg", "jpeg", "png", "gif", "webp", "heic")):
                    photos.append({
                        "id": f.get("id"),
                        "name": f.get("name") or "photo",
                        "mimetype": mimetype or "image/jpeg",
                        "url_private": url_private,
                    })
            cur_ins = conn.execute(
                """INSERT OR IGNORE INTO voc_items (channel_id, slack_ts, author, text, permalink, synced_at, photos)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (SLACK_VOC_CHANNEL_ID, ts, author, text, permalink, synced_at, json.dumps(photos, ensure_ascii=False))
            )
            # OR IGNORE라 동시 실행 중인 다른 워커가 먼저 넣었으면 rowcount=0 (정상, 중복 아님)
            if cur_ins.rowcount:
                new_count += 1
        conn.commit()
        conn.close()
        if new_count:
            print(f"[VOC Slack] 신규 {new_count}건 동기화됨")
        return new_count, None
    except Exception as e:
        print(f"[VOC Slack] sync 예외: {e}")
        return 0, f"동기화 중 오류가 발생했습니다: {e}"
def _scheduled_voc_sync():
    try:
        _, err = _sync_voc_from_slack()
        set_app_setting("voc_last_sync_error", err or "")
    except Exception as e:
        print(f"[VOC Slack] 스케줄 동기화 오류: {e}")
_scheduler.add_job(_scheduled_voc_sync, "interval", minutes=5)
def _post_voc_thread_reply(voc_item_id, text):
    """VOC 원본 슬랙 메시지에 스레드 댓글을 단다.
    SLACK_VOC_BOT_TOKEN에 chat:write 스코프가 있어야 하고, 봇이 채널 멤버여야 한다."""
    if not SLACK_VOC_BOT_TOKEN or not voc_item_id:
        return
    conn = get_user_db()
    item = conn.execute("SELECT slack_ts FROM voc_items WHERE id=?", (voc_item_id,)).fetchone()
    conn.close()
    if not item or not item["slack_ts"]:
        return
    try:
        resp = _requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {SLACK_VOC_BOT_TOKEN}",
                     "Content-Type": "application/json"},
            json={"channel": SLACK_VOC_CHANNEL_ID, "thread_ts": item["slack_ts"], "text": text},
            timeout=10
        )
        data = resp.json()
        if not data.get("ok"):
            print(f"[VOC Slack] 댓글 전송 실패: {data.get('error')}")
    except Exception as e:
        print(f"[VOC Slack] 댓글 전송 예외: {e}")
def _post_voc_completion_reply(voc_item_id, completed_by, complete_note=""):
    """긴급세차(VOC 요청 건)가 완료되면 원본 슬랙 메시지에 완료 댓글을 단다."""
    text = f"✅ {now_kst().strftime('%m/%d')} 긴급세차 요청 완료 (처리: {completed_by})"
    if complete_note:
        text += f"\n특이사항: {complete_note}"
    _post_voc_thread_reply(voc_item_id, text)
def _post_voc_schedule_reply(voc_item_id, scheduled_date, accepted_by):
    """작업자가 작업조치예정일을 지정하고 접수하면 원본 슬랙 메시지에 예정 댓글을 단다."""
    text = f"🗓 {scheduled_date} 세차 진행 예정입니다. (담당: {accepted_by})"
    _post_voc_thread_reply(voc_item_id, text)
def _post_voc_cancel_reply(voc_item_id, cancelled_by):
    """긴급세차 요청이 취소/회수되면 원본 슬랙 메시지에 취소 댓글을 단다."""
    text = f"🚫 긴급세차 요청이 취소되었습니다. (처리: {cancelled_by})"
    _post_voc_thread_reply(voc_item_id, text)
def _send_push_to_username(username, title, body, url="/urgent_wash"):
    """해당 계정이 구독해 둔 모든 기기에 웹 푸시 알림을 보낸다. 실패한 구독은 정리한다."""
    if not (_WEBPUSH_AVAILABLE and VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY):
        return
    conn = get_user_db()
    subs = conn.execute("SELECT * FROM push_subscriptions WHERE username=?", (username,)).fetchall()
    stale_ids = []
    for s in subs:
        subscription_info = {
            "endpoint": s["endpoint"],
            "keys": {"p256dh": s["p256dh"], "auth": s["auth"]},
        }
        try:
            webpush(
                subscription_info=subscription_info,
                data=json.dumps({"title": title, "body": body, "url": url}),
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": f"mailto:{VAPID_CONTACT_EMAIL}"},
            )
        except WebPushException as e:
            status = getattr(e.response, "status_code", None)
            if status in (404, 410):
                stale_ids.append(s["id"])
            else:
                print(f"[Push] 전송 실패({username}): {e}")
        except Exception as e:
            print(f"[Push] 전송 예외({username}): {e}")
    if stale_ids:
        conn.executemany("DELETE FROM push_subscriptions WHERE id=?", [(i,) for i in stale_ids])
        conn.commit()
    conn.close()
def _target_usernames_for_request(city, district, vendor):
    """긴급세차 요청 알림 대상 계정 목록.
    지역/업체 매칭 여부와 무관하게 담당자(staff)/관리자(admin) 계정 전원에게 무조건 발송한다
    (담당 작업자가 아직 배정되지 않은 지역이어도 알림은 항상 나가야 하므로 필터를 두지 않는다)."""
    conn = get_user_db()
    accounts = conn.execute("SELECT username FROM accounts WHERE role IN ('staff','admin')").fetchall()
    conn.close()
    return [a["username"] for a in accounts]
def _notify_urgent_wash_targets(city, district, vendor, title, body):
    for username in _target_usernames_for_request(city, district, vendor):
        _send_push_to_username(username, title, body)
def _resolve_vendor_for_region(city, district):
    """해당 시/도+구/군을 담당 지역으로 등록해 둔 staff 계정의 업체명을 반환. 없으면 빈 문자열."""
    if not city or not district:
        return ""
    conn = get_user_db()
    row = conn.execute(
        """
        SELECT a.vendor
        FROM account_region ar
        JOIN accounts a ON a.username = ar.username
        WHERE ar.city = ? AND ar.district = ? AND a.role = 'staff'
        LIMIT 1
        """,
        (city, district)
    ).fetchone()
    conn.close()
    return row["vendor"] if row and row["vendor"] else ""
def _account_regions(username):
    conn = get_user_db()
    rows = conn.execute(
        "SELECT city, district FROM account_region WHERE username=?",
        (username,)
    ).fetchall()
    conn.close()
    return [(r["city"], r["district"]) for r in rows]
def _field_request_visible(row, user):
    """field_requests/voc_items 한 건이 이 사용자에게 보여야 하는지 판단.
    master: 전체, admin(업체 관리자): 같은 업체 전체(하위 작업자 포함), staff: 업체 + 담당 지역 일치."""
    if user.is_master:
        return True
    row_vendor = row["vendor"] or ""
    if row_vendor and row_vendor != (user.vendor or ""):
        return False
    if user.role == "admin":
        return True
    # staff: 담당 지역과 일치해야 함
    regions = _account_regions(user.username)
    return (row["city"], row["district"]) in regions
def _visible_field_requests(rows, user):
    return [r for r in rows if _field_request_visible(r, user)]
@app.context_processor
def inject_field_request_badge_count():
    if not current_user.is_authenticated:
        return {"field_request_badge_count": 0}
    try:
        conn = get_user_db()
        rows = conn.execute("SELECT * FROM field_requests WHERE status IN ('대기','접수')").fetchall()
        conn.close()
        count = len(_visible_field_requests(rows, current_user))
        return {"field_request_badge_count": count}
    except Exception as e:
        print(f"[FieldRequest] badge 오류: {e}")
        return {"field_request_badge_count": 0}
def _parse_field_request_note(note):
    """field_requests.note에서 (VOC 요청 건은 '소속:'/'스팟명:'/'내용:' 라벨로 저장돼 있음)
    차량소속/스팟명/내용만 뽑아낸다. 라벨이 없는 일반 긴급세차 메모는 그대로 내용으로 취급."""
    note = note or ""
    org, spot = "", ""
    content_lines = []
    matched_any = False
    for line in note.split("\n"):
        s = line.strip()
        if s.startswith("소속:"):
            org = s[len("소속:"):].strip()
            matched_any = True
        elif s.startswith("스팟명:"):
            spot = s[len("스팟명:"):].strip()
            matched_any = True
        elif s.startswith("내용:"):
            content_lines.append(s[len("내용:"):].strip())
            matched_any = True
        elif s.startswith("[전달 메모]"):
            content_lines.append(s)
        elif not matched_any:
            content_lines.append(s)
    return {"org": org, "spot": spot, "content": "\n".join(l for l in content_lines if l)}
@app.route("/urgent_wash", methods=["GET", "POST"])
@login_required
def urgent_wash():
    if request.method == "POST":
        if not current_user.is_admin:
            flash("❌ 관리자/마스터 계정만 긴급세차를 요청할 수 있습니다.")
            return redirect(url_for("urgent_wash"))
        car_number = request.form.get("car_number", "").strip()
        city = request.form.get("city", "").strip()
        district = request.form.get("district", "").strip()
        note = request.form.get("note", "").strip()
        if not city or not district:
            flash("❌ 시/도, 구/군을 선택해주세요.")
            return redirect(url_for("urgent_wash"))
        if current_user.role == "admin":
            vendor = current_user.vendor or ""
        else:
            vendor = _resolve_vendor_for_region(city, district)
        conn = get_user_db()
        conn.execute(
            """INSERT INTO field_requests
               (source, car_number, city, district, vendor, note, created_by, created_at)
               VALUES ('urgent', ?, ?, ?, ?, ?, ?, ?)""",
            (car_number, city, district, vendor, note,
             current_user.username, now_kst().strftime("%Y-%m-%d %H:%M"))
        )
        conn.commit()
        conn.close()
        if not vendor:
            flash("⚠️ 요청은 등록됐지만 해당 지역에 담당 작업자가 배정되어 있지 않습니다. (알림은 전체 담당자에게 발송됩니다)")
        else:
            flash("✅ 긴급세차 요청이 담당 작업자에게 전달되었습니다.")
        # 지역/업체 매칭 여부와 무관하게 긴급세차가 배정되면 무조건 push 알림을 보낸다.
        _notify_urgent_wash_targets(
            city, district, vendor,
            "🚨 긴급세차 요청",
            f"{city} {district}" + (f" · {car_number}" if car_number else "") + " 긴급세차 요청이 있습니다."
        )
        return redirect(url_for("urgent_wash"))
    page = request.args.get("page", 1, type=int)
    done_page = request.args.get("done_page", 1, type=int)
    conn = get_user_db()
    all_rows = conn.execute("SELECT * FROM field_requests ORDER BY id DESC").fetchall()
    conn.close()
    visible = _visible_field_requests(all_rows, current_user)
    waiting_all = [r for r in visible if r["status"] == "대기"]
    progress_all = [r for r in visible if r["status"] == "접수"]
    pending_all = waiting_all + progress_all
    done_all = [r for r in visible if r["status"] == "완료"]
    pending_page, pending_current_page, pending_total_pages = paginate_list(pending_all, page, per_page=10)
    done_page_rows, done_current_page, done_total_pages = paginate_list(done_all, done_page, per_page=10)
    pending_items = []
    for r in pending_page:
        d = dict(r)
        parsed = _parse_field_request_note(d.get("note"))
        d["org_display"] = parsed["org"]
        d["spot_display"] = parsed["spot"]
        d["content_display"] = parsed["content"]
        pending_items.append(d)
    waiting_items = [d for d in pending_items if d["status"] == "대기"]
    progress_items = [d for d in pending_items if d["status"] == "접수"]
    return render_template(
        "urgent_wash.html",
        pending=pending_items,
        waiting_items=waiting_items,
        progress_items=progress_items,
        waiting_count=len(waiting_all),
        progress_count=len(progress_all),
        pending_count=len(pending_all),
        pending_current_page=pending_current_page,
        pending_total_pages=pending_total_pages,
        done=done_page_rows,
        done_count=len(done_all),
        done_current_page=done_current_page,
        done_total_pages=done_total_pages,
        city_options=list(KOREA_REGIONS.keys()),
        region_map=KOREA_REGIONS,
        today_str=now_kst().strftime("%Y-%m-%d"),
        push_available=bool(_WEBPUSH_AVAILABLE and VAPID_PUBLIC_KEY),
        current_username=current_user.username,
    )
@app.route("/urgent_wash/accept/<int:req_id>", methods=["POST"])
@login_required
def urgent_wash_accept(req_id):
    scheduled_date = request.form.get("scheduled_date", "").strip()
    if not scheduled_date:
        flash("❌ 작업 조치 예정일을 선택해주세요.")
        return redirect(url_for("urgent_wash"))
    conn = get_user_db()
    row = conn.execute("SELECT * FROM field_requests WHERE id=?", (req_id,)).fetchone()
    if not row or not _field_request_visible(row, current_user):
        conn.close()
        flash("❌ 해당 요청을 처리할 권한이 없습니다.")
        return redirect(url_for("urgent_wash"))
    conn.execute(
        """UPDATE field_requests
           SET status='접수', scheduled_date=?, accepted_by=?, accepted_at=?
           WHERE id=?""",
        (scheduled_date, current_user.username, now_kst().strftime("%Y-%m-%d %H:%M"), req_id)
    )
    conn.commit()
    conn.close()
    if row["source"] == "voc" and row["voc_item_id"]:
        _post_voc_schedule_reply(row["voc_item_id"], scheduled_date, current_user.username)
    flash(f"✅ {scheduled_date} 작업 예정으로 접수되었습니다.")
    return redirect(url_for("urgent_wash"))
@app.route("/urgent_wash/complete/<int:req_id>", methods=["POST"])
@login_required
def urgent_wash_complete(req_id):
    complete_note = request.form.get("complete_note", "").strip()
    conn = get_user_db()
    row = conn.execute("SELECT * FROM field_requests WHERE id=?", (req_id,)).fetchone()
    if not row or not _field_request_visible(row, current_user):
        conn.close()
        flash("❌ 해당 요청을 처리할 권한이 없습니다.")
        return redirect(url_for("urgent_wash"))
    conn.execute(
        "UPDATE field_requests SET status='완료', completed_by=?, completed_at=?, complete_note=? WHERE id=?",
        (current_user.username, now_kst().strftime("%Y-%m-%d %H:%M"), complete_note, req_id)
    )
    conn.commit()
    conn.close()
    if row["source"] == "voc" and row["voc_item_id"]:
        _post_voc_completion_reply(row["voc_item_id"], current_user.username, complete_note)
    flash("✅ 완료 처리되었습니다.")
    return redirect(url_for("urgent_wash"))
@app.route("/urgent_wash/cancel/<int:req_id>", methods=["POST"])
@login_required
def urgent_wash_cancel(req_id):
    conn = get_user_db()
    row = conn.execute("SELECT * FROM field_requests WHERE id=?", (req_id,)).fetchone()
    if not row:
        conn.close()
        flash("❌ 해당 요청을 찾을 수 없습니다.")
        return redirect(url_for("urgent_wash"))
    # 요청을 만든 본인, 또는 마스터 계정만 취소/회수할 수 있다.
    if row["created_by"] != current_user.username and not current_user.is_master:
        conn.close()
        flash("❌ 본인이 등록한 요청만 취소할 수 있습니다.")
        return redirect(url_for("urgent_wash"))
    if row["status"] == "완료":
        conn.close()
        flash("❌ 이미 완료 처리된 요청은 취소할 수 없습니다.")
        return redirect(url_for("urgent_wash"))
    conn.execute(
        "UPDATE field_requests SET status='취소', cancelled_by=?, cancelled_at=? WHERE id=?",
        (current_user.username, now_kst().strftime("%Y-%m-%d %H:%M"), req_id)
    )
    conn.commit()
    conn.close()
    if row["source"] == "voc" and row["voc_item_id"]:
        _post_voc_cancel_reply(row["voc_item_id"], current_user.username)
    flash("✅ 요청이 취소되었습니다.")
    return redirect(url_for("urgent_wash"))
@app.route("/push/vapid_public_key")
@login_required
def push_vapid_public_key():
    return jsonify({"key": VAPID_PUBLIC_KEY, "available": bool(_WEBPUSH_AVAILABLE and VAPID_PUBLIC_KEY)})
@app.route("/push/subscribe", methods=["POST"])
@login_required
def push_subscribe():
    data = request.get_json(silent=True) or {}
    endpoint = data.get("endpoint")
    keys = data.get("keys") or {}
    p256dh = keys.get("p256dh")
    auth = keys.get("auth")
    if not endpoint or not p256dh or not auth:
        return jsonify({"ok": False, "message": "잘못된 구독 정보입니다."}), 400
    conn = get_user_db()
    existing = conn.execute("SELECT id FROM push_subscriptions WHERE endpoint=?", (endpoint,)).fetchone()
    if existing:
        conn.execute(
            "UPDATE push_subscriptions SET username=?, p256dh=?, auth=? WHERE endpoint=?",
            (current_user.username, p256dh, auth, endpoint)
        )
    else:
        conn.execute(
            "INSERT INTO push_subscriptions (username, endpoint, p256dh, auth, created_at) VALUES (?, ?, ?, ?, ?)",
            (current_user.username, endpoint, p256dh, auth, now_kst().strftime("%Y-%m-%d %H:%M"))
        )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})
@app.route("/push/unsubscribe", methods=["POST"])
@login_required
def push_unsubscribe():
    data = request.get_json(silent=True) or {}
    endpoint = data.get("endpoint")
    if endpoint:
        conn = get_user_db()
        conn.execute("DELETE FROM push_subscriptions WHERE endpoint=? AND username=?", (endpoint, current_user.username))
        conn.commit()
        conn.close()
    return jsonify({"ok": True})
_VOC_LABELS = [
    "서비스구분", "예약번호", "고객명", "연락처", "차량 번호", "차량번호", "차종",
    "차량 소속", "소속", "사용 건수", "출발스팟명", "도착스팟명", "스팟명",
    "출발 스테이션명", "도착 스테이션명", "출발스테이션명", "도착스테이션명",
    "운행 시간", "운행시간", "주소",
    "예약시간", "회원명", "전화번호", "세차 경과일", "세차경과일",
    "정비 경과일", "정비경과일", "관리자예약 생성여부", "관리자예약 기간", "내용",
]
_VOC_LABEL_ALT = "|".join(re.escape(l) for l in sorted(_VOC_LABELS, key=len, reverse=True))
def _extract_voc_field(text, *label_variants):
    # 슬랙 원문이 "*차량번호*: 172허1475"처럼 라벨을 볼드(mrkdwn *…*)로 감싸거나,
    # "차량번호： 172허1475"처럼 전각 콜론(：)을 쓰는 경우가 있어 이를 모두 허용한다.
    for label in label_variants:
        pat = rf"\*?{re.escape(label)}\*?\s*[:：]\s*(.*?)(?=(?:\*?(?:{_VOC_LABEL_ALT})\*?\s*[:：])|$)"
        m = re.search(pat, text, re.DOTALL)
        if m:
            val = m.group(1).strip(" \n\t-`*")
            if val:
                return val
    return ""
def _parse_voc_summary(text):
    """카쉐어링 VOC 원문(슬랙 메시지)에서 예약번호/차량번호/소속/스팟명/내용만 뽑아낸다.
    메시지 포맷이 케이스마다 조금씩 달라서(차량번호 vs 차량 번호 등) 라벨 변형을 모두 시도한다."""
    text = text or ""
    reservation_no = _extract_voc_field(text, "예약번호")
    car_number = _extract_voc_field(text, "차량 번호", "차량번호")
    org = _extract_voc_field(text, "차량 소속", "소속")
    spot = _extract_voc_field(
        text, "스팟명", "출발스팟명",
        "출발 스테이션명", "출발스테이션명", "도착 스테이션명", "도착스테이션명"
    )
    # 원문에 "OOO 스팟"처럼 라벨과 같은 단어가 값 끝에 중복으로 붙는 경우가 있어 정리
    spot = re.sub(r"\s*스팟\s*$", "", spot).strip()
    content = _extract_voc_field(text, "내용")
    if not content:
        # "내용:" 라벨이 없는 포맷 — 마지막으로 매칭된 라벨 뒤의 텍스트를 내용으로 간주
        last_end = 0
        for m in re.finditer(rf"\*?(?:{_VOC_LABEL_ALT})\*?\s*[:：]\s*", text):
            last_end = m.end()
        content = text[last_end:].strip(" \n\t-`") if last_end else text.strip()
    content = re.sub(r"`+", "", content)
    content = re.sub(r"\s+", " ", content).strip()
    return {
        "reservation_no": reservation_no,
        "car_number": car_number,
        "org": org,
        "spot": spot,
        "content": content,
    }
def _norm_plate(s):
    """차량번호 비교용 정규화: 대괄호 태그/괄호 설명/공백/하이픈 등 표기 차이를 제거."""
    if not s:
        return ""
    s = re.sub(r"\[[^\]]*\]", "", s)
    s = re.sub(r"\([^)]*\)", "", s)
    s = re.sub(r"[\s\-]", "", s)
    return s.strip().upper()
def _voc_vehicle_master_lookup():
    """차량마스터(wash.db, 왕복=고정 스팟 차량 목록)에서 차량번호 집합과
    스팟 → (지역시도, 지역구군) 매핑을 만든다."""
    conn = get_wash_db()
    rows = conn.execute("SELECT 차량번호, 스팟, 지역시도, 지역구군 FROM vehicle_master").fetchall()
    conn.close()
    by_car = set()
    by_spot = {}
    for r in rows:
        if r["차량번호"]:
            norm = _norm_plate(r["차량번호"])
            if norm:
                by_car.add(norm)
        if r["스팟"]:
            spot_key = r["스팟"].strip()
            if spot_key and spot_key not in by_spot:
                by_spot[spot_key] = (r["지역시도"] or "", r["지역구군"] or "")
    return by_car, by_spot
def _match_car_bm(car_number, by_car):
    """차량번호 정규화 값이 차량마스터(왕복 차량 목록)에 정확히 있으면 '왕복', 없으면 '혼용'."""
    if not car_number:
        return "혼용"
    full = _norm_plate(car_number)
    if not full:
        return "혼용"
    return "왕복" if full in by_car else "혼용"
def _match_spot_region(spot, by_spot):
    if not spot:
        return "", ""
    if spot in by_spot:
        return by_spot[spot]
    for key, region in by_spot.items():
        if key and (key in spot or spot in key):
            return region
    return "", ""
@app.route("/voc_manage")
@login_required
def voc_manage():
    if not current_user.is_master:
        flash("❌ 마스터 계정만 접근할 수 있습니다.")
        return redirect(url_for("dashboard"))
    # 마지막 동기화로부터 60초 이상 지났으면 자동 새로고침 (페이지 열 때마다 슬랙 API 과호출 방지)
    last_sync = get_app_setting("voc_last_sync_ts", "")
    now_ts = now_kst().timestamp()
    if not last_sync or (now_ts - float(last_sync)) > 60:
        _, sync_error = _sync_voc_from_slack()
        set_app_setting("voc_last_sync_ts", str(now_ts))
        set_app_setting("voc_last_sync_error", sync_error or "")
    selected_status = request.args.get("status", "").strip()
    page = request.args.get("page", 1, type=int)
    conn = get_user_db()
    if selected_status:
        rows = conn.execute(
            "SELECT * FROM voc_items WHERE status=? ORDER BY slack_ts DESC",
            (selected_status,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM voc_items ORDER BY slack_ts DESC").fetchall()
    all_rows = conn.execute("SELECT status FROM voc_items").fetchall()
    seq_rows = conn.execute("SELECT id FROM voc_items ORDER BY slack_ts ASC, id ASC").fetchall()
    conn.close()
    seq_map = {r["id"]: i + 1 for i, r in enumerate(seq_rows)}
    by_car, by_spot = _voc_vehicle_master_lookup()
    page_rows, current_page, total_pages = paginate_list(rows, page, per_page=10)
    items = []
    for r in page_rows:
        d = dict(r)
        d.update(_parse_voc_summary(r["text"]))
        try:
            photos = json.loads(r["photos"] or "[]")
        except Exception:
            photos = []
        d["photos"] = [{"id": p["id"], "name": p.get("name", "photo")} for p in photos if p.get("id")]
        d["seq"] = seq_map.get(r["id"], 0)
        d["bm"] = _match_car_bm(d["car_number"], by_car)
        region_large, region_small = _match_spot_region(d["spot"], by_spot)
        d["region_large"] = region_large
        d["region_small"] = region_small
        items.append(d)
    cnt_total = len(all_rows)
    cnt_new = sum(1 for r in all_rows if r["status"] == "신규")
    cnt_requested = sum(1 for r in all_rows if r["status"] == "요청됨")
    return render_template(
        "voc_manage.html",
        items=items,
        selected_status=selected_status,
        current_page=current_page,
        total_pages=total_pages,
        cnt_total=cnt_total,
        cnt_new=cnt_new,
        cnt_requested=cnt_requested,
        city_options=list(KOREA_REGIONS.keys()),
        region_map=KOREA_REGIONS,
        slack_configured=bool(SLACK_VOC_BOT_TOKEN and SLACK_VOC_CHANNEL_ID),
        sync_error=get_app_setting("voc_last_sync_error", ""),
    )
@app.route("/voc_photo/<file_id>")
@login_required
def voc_photo(file_id):
    if not current_user.is_master:
        return "Forbidden", 403
    if not SLACK_VOC_BOT_TOKEN:
        return "Not configured", 404
    conn = get_user_db()
    rows = conn.execute("SELECT photos FROM voc_items WHERE photos LIKE ?", (f'%{file_id}%',)).fetchall()
    conn.close()
    url_private = None
    for r in rows:
        try:
            photos = json.loads(r["photos"] or "[]")
        except Exception:
            continue
        for p in photos:
            if p.get("id") == file_id:
                url_private = p.get("url_private")
                break
        if url_private:
            break
    if not url_private:
        return "Not found", 404
    try:
        resp = _requests.get(
            url_private,
            headers={"Authorization": f"Bearer {SLACK_VOC_BOT_TOKEN}"},
            timeout=15
        )
        if resp.status_code != 200:
            return "Slack fetch failed", 502
        return Response(resp.content, mimetype=resp.headers.get("Content-Type", "image/jpeg"))
    except Exception as e:
        print(f"[VOC Slack] 사진 프록시 오류: {e}")
        return "Error", 502
@app.route("/voc_manage/sync", methods=["POST"])
@login_required
def voc_sync():
    if not current_user.is_master:
        return "Forbidden", 403
    new_count, sync_error = _sync_voc_from_slack()
    set_app_setting("voc_last_sync_ts", str(now_kst().timestamp()))
    set_app_setting("voc_last_sync_error", sync_error or "")
    if sync_error:
        flash(f"❌ 슬랙 동기화 실패 — {sync_error}")
    else:
        flash(f"✅ 슬랙 동기화 완료 — 신규 {new_count}건")
    return redirect(url_for("voc_manage"))
@app.route("/voc_manage/request/<int:item_id>", methods=["POST"])
@login_required
def voc_request(item_id):
    if not current_user.is_master:
        return "Forbidden", 403
    city = request.form.get("city", "").strip()
    district = request.form.get("district", "").strip()
    note = request.form.get("note", "").strip()
    if not city or not district:
        flash("❌ 시/도, 구/군을 선택해주세요.")
        return redirect(url_for("voc_manage"))
    conn = get_user_db()
    item = conn.execute("SELECT * FROM voc_items WHERE id=?", (item_id,)).fetchone()
    if not item:
        conn.close()
        flash("❌ VOC 항목을 찾을 수 없습니다.")
        return redirect(url_for("voc_manage"))
    vendor = _resolve_vendor_for_region(city, district)
    requested_at = now_kst().strftime("%Y-%m-%d %H:%M")
    conn.execute(
        """UPDATE voc_items
           SET status='요청됨', city=?, district=?, note=?, requested_by=?, requested_at=?
           WHERE id=?""",
        (city, district, note, current_user.username, requested_at, item_id)
    )
    # 담당 작업자에게는 원본 슬랙 텍스트(고객명/연락처 등 개인정보 포함) 대신,
    # 차량번호/소속/스팟명/내용만 파싱해서 전달한다.
    summary = _parse_voc_summary(item["text"])
    field_note_lines = [
        f"소속: {summary['org']}" if summary["org"] else "",
        f"스팟명: {summary['spot']}" if summary["spot"] else "",
        f"내용: {summary['content']}" if summary["content"] else "",
    ]
    field_note = "\n".join(line for line in field_note_lines if line)
    if note:
        field_note += f"\n[전달 메모] {note}"
    conn.execute(
        """INSERT INTO field_requests
           (source, car_number, city, district, vendor, note, voc_item_id, created_by, created_at)
           VALUES ('voc', ?, ?, ?, ?, ?, ?, ?, ?)""",
        (summary["car_number"] or None, city, district, vendor, field_note, item_id, current_user.username, requested_at)
    )
    conn.commit()
    conn.close()
    if not vendor:
        flash("⚠️ 요청은 등록됐지만 해당 지역에 담당 작업자가 배정되어 있지 않습니다. (알림은 전체 담당자에게 발송됩니다)")
    else:
        flash("✅ VOC 요청이 담당 작업자의 긴급세차 목록으로 전달되었습니다.")
    # 지역/업체 매칭 여부와 무관하게 긴급세차가 배정되면 무조건 push 알림을 보낸다.
    _notify_urgent_wash_targets(
        city, district, vendor,
        "🚨 긴급세차 요청 (VOC)",
        f"{city} {district}" + (f" · {summary['car_number']}" if summary["car_number"] else "") + " 긴급세차 요청이 있습니다."
    )
    return redirect(url_for("voc_manage"))
@app.route("/voc_manage/delete/<int:item_id>", methods=["POST"])
@login_required
def voc_delete(item_id):
    if not current_user.is_master:
        return "Forbidden", 403
    conn = get_user_db()
    conn.execute("DELETE FROM field_requests WHERE voc_item_id=?", (item_id,))
    conn.execute("DELETE FROM voc_items WHERE id=?", (item_id,))
    conn.commit()
    conn.close()
    flash("✅ VOC 항목이 삭제되었습니다.")
    return redirect(url_for("voc_manage"))

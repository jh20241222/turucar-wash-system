import sqlite3
from werkzeug.security import generate_password_hash

conn = sqlite3.connect("db.sqlite3")
cur = conn.cursor()

cur.executescript("""
DROP TABLE IF EXISTS vendors;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS account_region;

CREATE TABLE vendors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
);

CREATE TABLE accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    password TEXT,
    role TEXT,
    vendor TEXT,
    parent_id INTEGER
);

CREATE TABLE account_region (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    city TEXT,
    district TEXT,
    created_by TEXT
);
""")

vendors = [
    "그린카워시", "하이클리너", "다돌", "다온",
    "디테일러", "카워시", "송정프라자"
]
for v in vendors:
    cur.execute("INSERT INTO vendors (name) VALUES (?)", (v,))


def hash_pw(pw):
    return generate_password_hash(pw)

accounts = [
    ("jeongyeon.kim", hash_pw("1111"), "master", None, None),
    ("greencarwash", hash_pw("1111"), "admin", "그린카워시", None),
    ("hicleaner", hash_pw("1111"), "admin", "하이클리너", None),
    ("dadol", hash_pw("1111"), "admin", "다돌", None),
    ("daon", hash_pw("1111"), "admin", "다온", None),
    ("detailer", hash_pw("1111"), "admin", "디테일러", None),
    ("carwash", hash_pw("1111"), "admin", "카워시", None),
    ("songjeongwash", hash_pw("1111"), "admin", "송정프라자", None),
]
for a in accounts:
    cur.execute(
        "INSERT INTO accounts (username, password, role, vendor, parent_id) VALUES (?, ?, ?, ?, ?)",
        a
    )

conn.commit()
conn.close()

print("✔ DB 초기 세팅 완료!")

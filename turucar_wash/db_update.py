import sqlite3

conn = sqlite3.connect("db.sqlite3")
cur = conn.cursor()

try:
    cur.execute("ALTER TABLE wash_list ADD COLUMN completed INTEGER DEFAULT 0;")
except:
    print("completed 컬럼 이미 존재함")

try:
    cur.execute("ALTER TABLE wash_list ADD COLUMN completed_time TEXT;")
except:
    print("completed_time 컬럼 이미 존재함")

conn.commit()
conn.close()

print("DB 업데이트 완료!")

"""
People Car API 연동 모듈
예약현황 데이터를 People Car 내부 API에서 읽어옵니다.
"""

import re
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://pm.peoplecar.co.kr:8082"


class PeopleCarLoginError(Exception):
    pass


class PeopleCarClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": BASE_URL + "/",
        })
        self.session_key = None
        self.user_id = None
        self.corp_id = None
        self.corp_name = None

    def login(self, user_id: str, password: str) -> None:
        resp = self.session.post(
            BASE_URL + "/login/login_process.asp",
            data={"login_Id": user_id, "login_Pw": password},
            verify=False,
            timeout=15,
        )
        resp.raise_for_status()
        html = resp.text

        def extract(key):
            m = re.search(
                rf"localStorage\.setItem\(['\"]PEOPLECAR_{key}['\"]\s*,\s*['\"]([^'\"]+)['\"]",
                html
            )
            return m.group(1) if m else None

        self.session_key = extract("SessionKey")
        self.user_id     = extract("UserId")
        self.corp_id     = extract("CorpId")
        self.corp_name   = extract("CorpName")

        if not self.session_key:
            raise PeopleCarLoginError(f"로그인 실패: {user_id}")

    def get_reservations(self, start_date, end_date, search="", status="", page=1):
        if not self.session_key:
            raise PeopleCarLoginError("먼저 login()을 호출하세요.")

        resp = self.session.post(
            BASE_URL + "/reshistory/reshistory.asp",
            params={"SessionKey": self.session_key, "UserId": self.user_id, "CorpId": self.corp_id},
            data={
                "s_kind": "1", "page": str(page), "maxpage": "0", "temp": "0",
                "CorpId": self.corp_id,
                "Startdt": start_date, "Enddt": end_date,
                "Searchkey": search, "Status": status,
            },
            verify=False, timeout=15,
        )
        resp.raise_for_status()
        return self._parse_html(resp.text, page)

    def get_all_reservations(self, start_date, end_date, search="", status=""):
        result = self.get_reservations(start_date, end_date, search, status, page=1)
        items = result["reservations"]
        for p in range(2, result["max_page"] + 1):
            items.extend(self.get_reservations(start_date, end_date, search, status, page=p)["reservations"])
        return items

    @staticmethod
    def _parse_html(html, page):
        soup = BeautifulSoup(html, "html.parser")
        max_page_input = soup.find("input", {"name": "maxpage"})
        max_page = int(max_page_input["value"]) if max_page_input else 1
        table = soup.find("table")
        if not table:
            return {"page": page, "max_page": max_page, "total": 0, "reservations": []}
        rows = table.find_all("tr")
        if len(rows) < 2:
            return {"page": page, "max_page": max_page, "total": 0, "reservations": []}
        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        reservations = []
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) == len(headers):
                reservations.append(dict(zip(headers, cells)))
        return {"page": page, "max_page": max_page, "total": len(reservations), "reservations": reservations}

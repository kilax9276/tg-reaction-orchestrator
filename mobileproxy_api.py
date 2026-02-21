# -----------------------------------------------------------------------------
# mobileproxy_api.py
#
# Thin wrapper around MobileProxy.Space HTTP API.
#
# The API is rate-limited *globally* across all processes using a SQLite-backed
# limiter table (mobileproxy_limiter.db). This prevents hammering the provider
# and avoids being temporarily blocked.
#
# Methods used by this project:
#   - get_my_proxies(), get_proxy_ip()
#   - reboot_proxy(), change_equipment()
#   - (optional/advanced) edit_proxy(), blacklists, etc.
# -----------------------------------------------------------------------------

import os
import time
import platform
import sqlite3
import requests
from typing import Union, List, Optional
from datetime import datetime


class MobileProxyAPI:
    BASE_URL = "https://mobileproxy.space/api.html"
    CHANGE_IP_URL = "https://changeip.mobileproxy.space/"

    def __init__(self, token: str):
        self.token = token
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self._min_interval = 3.5  # seconds between API calls
        self._db_path = "mobileproxy_limiter.db"

    def _request(self, command: str, method: str = 'GET', params: dict = None, data: dict = None):
        self._acquire_global_rate_limit(command)

        url = f"{self.BASE_URL}?command={command}"
        headers = self.headers

        try:
            if method.upper() == 'POST':
                response = requests.post(url, headers=headers, data=data)
            else:
                response = requests.get(url, headers=headers, params=params)

            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]📤 URL: {response.url}")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]📨 Код: {response.status_code}")

            text = response.text
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]📨 Ответ raw: {text[:200]}")  # ограничение длины в логе

            # Если ответ не 200 или это HTML-страница — ошибка API
            if response.status_code != 200 or text.lstrip().startswith("<html"):
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Ошибка API MobileProxy ({response.status_code}): {text[:100]!r}")
                return None

            # Парсим JSON
            try:
                return response.json()
            except ValueError:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Невалидный JSON от MobileProxy: {text[:100]!r}")
                return None

        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Ошибка при запросе к MobileProxy API: {e}")
            return None
    def _acquire_global_rate_limit(self, command):
        try:
            conn = sqlite3.connect(self._db_path, timeout=10)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS limiter (
                    id INTEGER PRIMARY KEY,
                    last_call REAL
                )
            """)
            conn.commit()

            with conn:
                conn.execute("BEGIN IMMEDIATE")  # 🔒 глобальная блокировка на запись

                row = conn.execute("SELECT last_call FROM limiter WHERE id = 1").fetchone()
                now = time.time()                               # ⬅️ вместо time.monotonic()
                last_call = float(row[0]) if row and row[0] else 0.0
                elapsed = now - last_call

                # ⛑ защита от странных значений (рестарт, NTP‑скачок и т.п.)
                # если elapsed отрицательный или слишком большой — считаем, что лимит не нарушен
                if elapsed < 0 or elapsed > 3600:               # > 1 часа считаем «невалидным»
                    elapsed = self._min_interval

                wait_time = self._min_interval - elapsed
                if wait_time > 0:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⏳ Ждём {wait_time:.2f}s перед API ({command})")
                    time.sleep(wait_time)

                conn.execute("REPLACE INTO limiter (id, last_call) VALUES (1, ?)", (time.time(),))
                conn.commit()

        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠️ RateLimit error (SQLite): {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass


    # --- API METHODS ---

    def get_proxy_ip(self, proxy_id: Union[int, List[int]]):
        proxy_id_list = [proxy_id] if isinstance(proxy_id, int) else proxy_id
        proxy_id_str = ",".join(map(str, proxy_id_list))
        result = self._request("proxy_ip", params={"proxy_id": proxy_id_str})

        if not isinstance(result, dict):
            return {"status": "err", "message": "Неверный формат ответа"}

        if result.get("status", "").lower() != "ok":
            return result

        ip_map = result.get("proxy_id")
        single_ip = result.get("ip")

        if not ip_map and single_ip and len(proxy_id_list) == 1:
            return {
                "status": "ok",
                "proxy_id": {
                    str(proxy_id_list[0]): single_ip
                }
            }

        return {
            "status": "ok",
            "proxy_id": ip_map or {}
        }

    def get_balance(self):
        return self._request("get_balance")

    def get_my_proxies(self, proxy_id: Optional[Union[int, List[int]]] = None):
        params = {}
        if proxy_id:
            proxy_id_list = [proxy_id] if isinstance(proxy_id, int) else proxy_id
            proxy_id_str = ",".join(map(str, proxy_id_list))
            params["proxy_id"] = proxy_id_str

        result = self._request("get_my_proxy", params=params)

        if isinstance(result, list):
            return result

        if isinstance(result, dict) and result.get("status") == "ok":
            proxies = result.get("proxy", [])
            if proxy_id:
                proxy_id_list = set(map(str, proxy_id_list))
                proxies = [p for p in proxies if p.get("proxy_id") in proxy_id_list]
            return proxies

        return []

    def change_login_password(self, proxy_id, proxy_login, proxy_pass):
        proxy_id = ",".join(map(str, proxy_id)) if isinstance(proxy_id, list) else str(proxy_id)
        params = {"proxy_id": proxy_id, "proxy_login": proxy_login, "proxy_pass": proxy_pass}
        return self._request("change_proxy_login_password", params=params)

    def reboot_proxy(self, proxy_id: Union[int, List[int]]):
        proxy_id = ",".join(map(str, proxy_id)) if isinstance(proxy_id, list) else str(proxy_id)
        return self._request("reboot_proxy", params={"proxy_id": proxy_id})

    def get_prices(self, id_country, currency="rub"):
        return self._request("get_price", params={"id_country": id_country, "currency": currency})

    def get_black_list(self, proxy_id):
        proxy_id = ",".join(map(str, proxy_id)) if isinstance(proxy_id, list) else str(proxy_id)
        return self._request("get_black_list", params={"proxy_id": proxy_id})

    def add_operator_to_black_list(self, proxy_id, operator_id):
        return self._request("add_operator_to_black_list", params={"proxy_id": proxy_id, "operator_id": operator_id})

    def remove_operator_black_list(self, proxy_id=None, operator_id=None):
        params = {}
        if proxy_id: params["proxy_id"] = proxy_id
        if operator_id: params["operator_id"] = operator_id
        return self._request("remove_operator_black_list", params=params)

    def remove_equipment_black_list(self, proxy_id=None, black_list_id=None, eid=None):
        params = {}
        if proxy_id: params["proxy_id"] = proxy_id
        if black_list_id: params["black_list_id"] = black_list_id
        if eid: params["eid"] = eid
        return self._request("remove_black_list", params=params)

    def edit_proxy(self, proxy_id, **kwargs):
        proxy_id = ",".join(map(str, proxy_id)) if isinstance(proxy_id, list) else str(proxy_id)
        params = {"proxy_id": proxy_id}
        params.update(kwargs)
        return self._request("edit_proxy", params=params)

    def get_geo_operator_list(self, **kwargs):
        return self._request("get_geo_operator_list", params=kwargs)

    def get_operators_list(self, geoid):
        return self._request("get_operators_list", params={"geoid": geoid})

    def get_countries(self, only_avaliable=True):
        return self._request("get_id_country", params={"only_avaliable": int(only_avaliable)})

    def get_cities(self):
        return self._request("get_id_city")

    def get_geo_list(self, proxy_id=None, geoid=None):
        params = {}
        if proxy_id: params["proxy_id"] = proxy_id
        if geoid: params["geoid"] = geoid
        return self._request("get_geo_list", params=params)

    def change_equipment(self, proxy_id, **kwargs):
        proxy_id = ",".join(map(str, proxy_id)) if isinstance(proxy_id, list) else str(proxy_id)
        params = {"proxy_id": proxy_id}
        params.update(kwargs)
        return self._request("change_equipment", params=params)

    def buy_proxy(self, **kwargs):
        return self._request("buyproxy", params=kwargs)

    def get_ipstat(self):
        return self._request("get_ipstat")

    def see_url_from_different_ips(self, url, id_country=None):
        data = {"url": url}
        if id_country:
            data["id_country"] = id_country
        return self._request("see_the_url_from_different_IPs", method="POST", data=data)

    def get_task_result(self, tasks_id=None):
        params = {"tasks_id": tasks_id} if tasks_id else {}
        return self._request("tasks", params=params)

    def is_equipment_available(self, eid: Union[int, List[int]]):
        eid = ",".join(map(str, eid)) if isinstance(eid, list) else str(eid)
        return self._request("eid_avaliable", params={"eid": eid})

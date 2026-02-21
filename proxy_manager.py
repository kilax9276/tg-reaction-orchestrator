# -----------------------------------------------------------------------------
# proxy_manager.py
# Copyright Kolobov Aleksei @kilax9276
#
# Mobile proxy orchestration layer.
#
# Responsibilities:
#   - fetch proxy connection data (SOCKS5 host/port/login/pass) and external IP
#     via MobileProxyAPI (mobileproxy_api.py)
#   - cache proxy metadata and IP history in SQLite (ip_database.IPDatabase)
#   - enforce usage policy:
#       * limit number of unique sessions per external IP per hour
#       * rotate IP when current IP is banned/reused
#   - coordinate IP rotation between multiple OS processes:
#       * acquire_ip_lock() uses a SQLite WAL row as an inter-process mutex per proxy_id
#       * only one process rotates the same proxy_id at a time
#
# Notes:
#   - The code includes verbose logging by design; it is intended for operations.
#   - There are two definitions of get_proxy_info_cached() in the original code.
#     We keep it as-is for compatibility; the second definition overrides the first.
# -----------------------------------------------------------------------------

import asyncio
import aiohttp
import json
import time
import re
import ipaddress
import sqlite3

from typing import List, Union, Optional
from collections import defaultdict
from ip_database import IPDatabase
from datetime import datetime, timedelta

# Глобальная блокировка по proxy_id для защиты смены IP
def acquire_ip_lock(proxy_id: int, db_path="proxy_lock.db", timeout=10) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS proxy_locks (
            proxy_id INTEGER PRIMARY KEY,
            last_acquired REAL
        )
    """)
    conn.commit()

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with conn:
                conn.execute("INSERT OR REPLACE INTO proxy_locks (proxy_id, last_acquired) VALUES (?, ?)", (proxy_id, time.time()))
            return conn
        except sqlite3.OperationalError:
            time.sleep(0.2)

    raise TimeoutError(f"Не удалось получить блокировку proxy_id={proxy_id}")

def release_ip_lock(conn: sqlite3.Connection):
    conn.close()


class ProxyRateLimiter:
    def __init__(self, max_requests_per_proxy=3):
        self.timestamps = defaultdict(list)
        self.max_requests_per_proxy = max_requests_per_proxy

    async def wait(self, proxy_id: Union[int, str]):
        now = time.monotonic()
        proxy_id = str(proxy_id)
        self.timestamps[proxy_id] = [t for t in self.timestamps[proxy_id] if now - t < 1]
        while len(self.timestamps[proxy_id]) >= self.max_requests_per_proxy:
            await asyncio.sleep(0.1)
            now = time.monotonic()
            self.timestamps[proxy_id] = [t for t in self.timestamps[proxy_id] if now - t < 1]
        self.timestamps[proxy_id].append(now)


class AsyncProxyManager:
    def __init__(self, api, user_agent=None, ip_db_path="ip_data.db", max_total_bots_per_ip=2):
        self.api = api
        self.user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        self.limiter = ProxyRateLimiter()
        self.db = IPDatabase(db_path=ip_db_path)
        self.last_ip_change = {}  # время последней смены IP по external_ip
        self.active_ips = {}      # счётчик активных сессий по external_ip   
        self.used_ips = defaultdict(set) # IP → set(сессий)
        self.max_total_bots_per_ip = max_total_bots_per_ip        

    async def wait_for_external_ip(self, proxy_id: int, timeout=10, interval=0.5):
        """
        Ждёт, пока в БД появится external_ip для заданного proxy_id.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = self.get_proxy_connection_info(proxy_id)
            ip = info.get("external_ip")
            if ip:
                return ip
            await asyncio.sleep(interval)
        return None

    def get_last_ip_for_proxy(self, proxy_id):
        cur = self.db.conn.cursor()
        cur.execute("""
            SELECT ip_address FROM ip_history
            WHERE proxy_id = ? AND ip_address != '0.0.0.0'
            ORDER BY time_acquired DESC
            LIMIT 1
        """, (proxy_id,))
        row = cur.fetchone()
        return row[0] if row else None


    def check_socks5_connectivity(self, proxy_info, timeout=60, interval=5):
        import socks
        import socket
        import time

        hostname = proxy_info.get("socks5_ip")
        port = int(proxy_info.get("socks5_port"))
        login = proxy_info.get("proxy_login")
        password = proxy_info.get("proxy_pass")

        deadline = time.time() + timeout
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🌐 Проверка доступности интернета через SOCKS5 ({hostname}:{port})...")

        while time.time() < deadline:
            try:
                s = socks.socksocket()
                s.set_proxy(socks.SOCKS5, hostname, port, True, login, password)
                s.settimeout(10)
                s.connect(("8.8.8.8", 53))  # DNS-запрос к Google
                s.close()
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]✅ SOCKS5-прокси работает")
                return True
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⏳ SOCKS5 пока не доступен:", e)
                time.sleep(interval)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ SOCKS5-прокси не заработал в отведённое время")
        return False


    def get_proxy_info_cached(self, proxy_id: int):
        cur = self.db.conn.cursor()
        cur.execute("""
            SELECT proxy_id FROM proxy_info WHERE proxy_id = ?
        """, (proxy_id,))
        row = cur.fetchone()

        if row:
            # Данные есть — возвращаем из БД
            return self.get_proxy_connection_info(proxy_id)
        
        # Данных нет — загружаем с сервера и сохраняем
        self.update_and_save_proxy_info(proxy_id)
        return self.get_proxy_connection_info(proxy_id)


    def update_and_save_proxy_info(self, proxy_id: int):
        proxies = self.api.get_my_proxies(proxy_id)
        if not proxies:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Не удалось получить данные для proxy_id={proxy_id}")
            return None

        info = proxies[0]

        ip_result = self.api.get_proxy_ip(proxy_id)
        external_ip = None

        if ip_result:
            if ip_result.get("status") == "ok":
                external_ip = ip_result.get("proxy_id", {}).get(str(proxy_id)) \
                    or ip_result.get("ip")
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Не удалось получить внешний IP для proxy_id={proxy_id}: {ip_result.get('message', 'неизвестная ошибка')}")
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Пустой ответ при получении IP для proxy_id={proxy_id}")

        self.db.save_proxy_info(info, external_ip)
        return info


    def read_proxy_info_from_db(self, proxy_id: int):
        cur = self.db.conn.cursor()
        cur.execute("""
            SELECT proxy_id, socks5_ip, socks5_port, proxy_login, proxy_pass, proxy_exp,
                proxy_key, proxy_change_ip_url, geoid, id_country, proxy_operator, eid, external_ip
            FROM proxy_info
            WHERE proxy_id = ?
        """, (proxy_id,))
        row = cur.fetchone()
        if row:
            keys = ["proxy_id", "socks5_ip", "socks5_port", "proxy_login", "proxy_pass", "proxy_exp", 
                    "proxy_key", "proxy_change_ip_url", "geoid", "id_country", "proxy_operator", "eid", "external_ip"]
            return dict(zip(keys, row))
        return None


    def get_proxy_connection_info(self, proxy_id: int):
        info = self.read_proxy_info_from_db(proxy_id)
        
        # Если info есть, но external_ip отсутствует — обновляем
        if info and not info.get("external_ip"):
            self.update_and_save_proxy_info(proxy_id)
            info = self.read_proxy_info_from_db(proxy_id)
        
        # Если вообще ничего нет — тоже пробуем обновить
        if not info:
            self.update_and_save_proxy_info(proxy_id)
            info = self.read_proxy_info_from_db(proxy_id)

        return info



    def get_proxy_info_cached(self, proxy_id: int):
        info = self.read_proxy_info_from_db(proxy_id)
        if info:
            return info

        self.update_and_save_proxy_info(proxy_id)
        return self.read_proxy_info_from_db(proxy_id)
    


    def is_valid_ip(self, ip):
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False

    def get_external_ip(self, pid=None):
        import requests

        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🔍 Определение внешнего IP... pid={pid}")

        if pid is None:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Не указан proxy_id — не можем получить кэшированные данные.")
            return None

        proxy_info = self.get_proxy_info_cached(pid)
        if not proxy_info:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Не удалось получить данные о прокси.")
            return None

        proxy_id = proxy_info.get("proxy_id")
        ip = proxy_info.get("socks5_ip")
        port = proxy_info.get("socks5_port")
        login = proxy_info.get("proxy_login")
        password = proxy_info.get("proxy_pass")

        if not all([ip, port, login, password, proxy_id]):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Недостаточно данных для подключения к прокси.")
            return None

        def extract_ip_from_result(result):
            raw_ip = result.get("ip")
            mapped_ip = result.get("proxy_id", {}).get(str(proxy_id))
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]📦 API вернул: ip = {raw_ip}, proxy_id[{proxy_id}] = {mapped_ip}")
            return raw_ip if raw_ip else mapped_ip


        # Первая попытка
        api_result = self.api._request("proxy_ip", params={"proxy_id": proxy_id})
        api_ip = extract_ip_from_result(api_result)

        # Проверка на HTML-мусор
        if isinstance(api_ip, str) and "<html" in api_ip.lower():
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Обнаружен HTML в ответе вместо IP — обновляем данные прокси с сервера...")
            self.update_and_save_proxy_info(proxy_id)

            # Повторная попытка
            api_result = self.api._request("proxy_ip", params={"proxy_id": proxy_id})
            api_ip = extract_ip_from_result(api_result)

            if isinstance(api_ip, str) and "<html" in api_ip.lower():
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Даже после обновления получен HTML. Возврат None.")
                return None

        if not api_ip:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Не удалось извлечь IP из ответа API.")
            return None

        if not self.is_valid_ip(api_ip):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Получен некорректный IP: {api_ip}")
            return None

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]✅ Внешний IP получен: {api_ip}")
        return api_ip


    async def get_valid_proxy_ip(self, proxy_id: Union[int, List[int]]) -> dict:
        if isinstance(proxy_id, int):
            proxy_id = [proxy_id]

        for pid in proxy_id:
            ban_counter = 0
            fail_counter = 0
            while True:
                await self.limiter.wait(pid)
                try:
                    ip = self.get_external_ip(pid)

                    if not self.is_valid_ip(ip=ip):
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Получен некорректный IP:", ip)
                        continue

                    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]📦 Реальный внешний IP от get_external_ip: {ip}")
                except Exception as e:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Ошибка получения IP: {e}")
                    continue

                if not ip:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Не удалось получить IP")
                    continue

                status = self.db.get_ip_status(ip)

                if status == "BAN":
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⛔ IP {ip} под BAN, пробуем сменить...")
                    ban_counter += 1
                    need_swap = True
                else:
                    existing_ip = self.get_last_ip_for_proxy(pid)
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🧠 Последний IP в базе для proxy_id {pid}: {existing_ip}")
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]📦 Текущий IP от API: {ip}")

                    if existing_ip == ip:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🔁 IP {ip} совпадает с предыдущим. Помечаем как BAN и меняем...")
                        self.db.mark_banned(ip)
                        need_swap = True
                    else:
                        need_swap = False

                if need_swap:
                    # ✅ SQLite-блокировка между процессами
                    try:
                        lock_conn = acquire_ip_lock(pid, db_path="proxy_lock.db")
                    except TimeoutError as e:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Не удалось получить блокировку proxy_id={pid}: {e}")
                        continue

                    try:
                        # Повторная проверка IP после блокировки
                        ip = self.get_external_ip(pid)
                        status = self.db.get_ip_status(ip)
                        if status != "BAN" and ip != self.get_last_ip_for_proxy(pid):
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🔁 Пока ждали блокировку, IP уже изменился. Повторяем проверку.")
                            continue

                        if ban_counter >= 5:
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ 5 подряд IP под баном — пробуем сменить оборудование...")
                            try:
                                response = self.api.change_equipment(pid)
                                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🔁 Результат смены оборудования:", response)
                                ban_counter = 0

                                await asyncio.sleep(60)
                                proxy_info = self.get_proxy_info_cached(pid)

                                if not self.check_socks5_connectivity(proxy_info):
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ SOCKS-прокси не заработал после смены оборудования")
                                    return {"proxy_id": pid, "ip": None, "status": "fail_socks"}

                                self.update_and_save_proxy_info(pid)
                                self.reset_used_ip(proxy_info.get("external_ip"))
                                proxy_info["proxy_id"] = pid
                                proxy_info["ip"] = ip
                                proxy_info["status"] = "ok"
                                return proxy_info

                            except Exception as e:
                                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Ошибка при смене оборудования:", e)
                            await asyncio.sleep(10)

                        attempt = 0
                        while attempt < 5:
                            attempt += 1
                            result = await self.change_ip_with_retry_internal(pid)

                            if result:
                                return result
                            else:
                                fail_counter += 1

                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ 5 попыток смены IP не удались. Пробуем перезагрузить прокси...")
                        try:
                            response = self.api.reboot_proxy(pid)
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]♻ Результат перезагрузки:", response)

                            await asyncio.sleep(60)
                            proxy_info = self.get_proxy_info_cached(pid)

                            if not self.check_socks5_connectivity(proxy_info):
                                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ SOCKS-прокси не заработал после перезагрузки")
                                return {"proxy_id": pid, "ip": None, "status": "fail_socks"}

                            self.update_and_save_proxy_info(pid)
                            self.reset_used_ip(proxy_info.get("external_ip"))
                            proxy_info["proxy_id"] = pid
                            proxy_info["ip"] = ip
                            proxy_info["status"] = "ok"
                            return proxy_info

                        except Exception as e:
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Ошибка при перезагрузке:", e)

                        if fail_counter >= 10:
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Даже после перезагрузки 5 неудач — меняем оборудование")
                            try:
                                response = self.api.change_equipment(pid)
                                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🔁 Результат смены оборудования:", response)
                                fail_counter = 0

                                await asyncio.sleep(60)
                                proxy_info = self.get_proxy_info_cached(pid)

                                if not self.check_socks5_connectivity(proxy_info):
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ SOCKS-прокси не заработал после смены оборудования")
                                    return {"proxy_id": pid, "ip": None, "status": "fail_socks"}

                                self.update_and_save_proxy_info(pid)
                                self.reset_used_ip(proxy_info.get("external_ip"))
                                proxy_info["proxy_id"] = pid
                                proxy_info["ip"] = ip
                                proxy_info["status"] = "ok"
                                return proxy_info

                            except Exception as e:
                                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Ошибка при смене оборудования:", e)
                            await asyncio.sleep(10)

                    finally:
                        release_ip_lock(lock_conn)

                else:
                    proxy_info = self.get_proxy_info_cached(pid)

                    login = proxy_info.get("proxy_login", "-")
                    password = proxy_info.get("proxy_pass", "-")

                    if login != "-" and password != "-":
                        self.db.add_ip(pid, ip, login, password)
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]💾 IP {ip} сохранён как GOOD")
                    else:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❗️ Не удалось получить логин/пароль, но IP всё равно будет записан")
                        self.db.add_ip(pid, ip, "-", "-")

                    socks5_ip = proxy_info.get("socks5_ip")
                    self.db.update_external_ip(socks5_ip, ip)
                    self.reset_used_ip(proxy_info.get("external_ip"))
                    proxy_info["proxy_id"] = pid
                    proxy_info["ip"] = ip
                    proxy_info["status"] = "ok"
                    return proxy_info

        raise Exception("Не удалось получить рабочий IP")



    async def change_ip_with_retry_internal(self, pid: int, max_attempts: int = 5, wait_seconds: int = 10):
        
        proxy_info = self.get_proxy_info_cached(pid)

#        await asyncio.sleep(3)

        if not proxy_info:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❗️ Прокси не найден для смены IP")
            return None

        url = proxy_info.get("proxy_change_ip_url")
        if not url:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Нет ссылки смены IP")
            return None

        if "format=" not in url:
            url += "&format=json"

        for attempt in range(1, max_attempts + 1):
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🔁 Попытка #{attempt} смены IP...")

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers={"User-Agent": self.user_agent}, timeout=20) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data.get("status", "").lower() == "ok":
                                new_ip = data.get("new_ip")
                                if new_ip:
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]✅ Внешний IP успешно сменён: {new_ip}")
                                    login = proxy_info.get("proxy_login", "-")
                                    password = proxy_info.get("proxy_pass", "-")

                                    # Проверяем доступность SOCKS
                                    if not self.check_socks5_connectivity(proxy_info):
                                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Прокси не работает после смены IP")
                                        return {"proxy_id": pid, "ip": new_ip, "status": "fail_socks"}

                                    self.reset_used_ip(proxy_info.get("external_ip"))  # старый IP

                                    # IP работает — сохраняем
                                    self.db.add_ip(pid, new_ip, login, password)
                                    self.update_and_save_proxy_info(pid)
                                    proxy_info = self.get_proxy_info_cached(pid)  # 💡 обязательный повторный fetch
                                    proxy_info["proxy_id"] = pid
                                    proxy_info["ip"] = new_ip
                                    proxy_info["status"] = "ok"
                                    return proxy_info

                                else:
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Нет поля new_ip в ответе")
                            else:
                                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Ответ с ошибкой: {data}")
                        else:
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ HTTP ошибка: {response.status}")
            except asyncio.TimeoutError:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⏱ Таймаут: сервер не ответил вовремя.")
            except aiohttp.ClientError as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🚫 Ошибка клиента:", e)

            await asyncio.sleep(wait_seconds)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Все попытки смены IP не удались.")
        return None

    async def get_available_proxy(self, proxy_ids, session_name=None):
        # Удаляем устаревшие записи (например, старше 24 ч)
        self.db.purge_old_sessions(24)

        for pid in proxy_ids:
            info = self.get_proxy_connection_info(pid)
            if not info:
                continue

            external_ip = info.get("external_ip")
            if not external_ip:
                continue

            # ✅ Проверка количества уникальных сессий за последний 1 час
            recent_count = self.db.count_recent_sessions(external_ip, hours=1)
            if recent_count < self.max_total_bots_per_ip:
                if session_name:
                    self.db.add_active_session(external_ip, session_name)
                info["status"] = "ok"
                return info

            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⛔ IP {external_ip} уже использовался {recent_count} раз за последний 1 час — требуется смена IP")

            # ✅ Межпроцессная блокировка
            try:
                lock_conn = acquire_ip_lock(pid, db_path="proxy_lock.db")
            except TimeoutError as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Не удалось получить блокировку proxy_id={pid}: {e}")
                continue

            try:
                # 🔁 Повторная проверка, вдруг IP уже обновился
                info = self.get_proxy_connection_info(pid)
                new_ip = info.get("external_ip")
                if not new_ip:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Новый IP не определён после повторной проверки для proxy_id={pid}")
                    continue

                recent_count = self.db.count_recent_sessions(new_ip, hours=1)
                if recent_count < self.max_total_bots_per_ip:
                    if session_name:
                        self.db.add_active_session(new_ip, session_name)
                    info["status"] = "ok"
                    return info

                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🔁 Новый IP {new_ip} тоже использовался {recent_count} раз — пробуем сменить IP")

                # 🚀 Попытка смены IP
                result = await self.get_valid_proxy_ip(pid)

                if result.get("status") == "ok":
                    updated_ip = result.get("external_ip")
                    if session_name:
                        self.db.add_active_session(updated_ip, session_name)
                    result["status"] = "ok"
                    return result
                else:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Не удалось сменить IP для proxy_id={pid}")

            finally:
                release_ip_lock(lock_conn)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Нет доступного прокси по лимитам использования за последний 1 час")
        return None



    def release_proxy_ip(self, external_ip, session_name=None):
        if external_ip and session_name:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]release_proxy_ip = {external_ip} | session = {session_name}")
            self.db.remove_active_session(external_ip, session_name)
            
    def reset_used_ip(self, external_ip):
        if external_ip in self.used_ips:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]reset_used_ip = {external_ip}")
            self.used_ips[external_ip].clear()            
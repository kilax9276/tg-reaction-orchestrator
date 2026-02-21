# -----------------------------------------------------------------------------
# session_validator.py
#
# Session validation worker. It can be launched as a separate process (legacy path)
# that scans a folder of .session files and tries to authorize them.
#
# In the current run.py, validation is primarily done via jobs queue:
#   - controller_bot inserts validate_session jobs when ZIP is uploaded
#   - ReactionWorkerPool executes validate_session jobs
#
# This module remains useful if you want a dedicated validator process with its
# own concurrency controls and proxy waiting strategy.
# -----------------------------------------------------------------------------

# session_validator.py
import os
import asyncio
import socks
import traceback
import time
from datetime import datetime

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

from BotManager import BotManager
from proxy_manager import AsyncProxyManager
from controller_bot import log_session_status
from session_store import store            # ← глобальный реестр
import code_manager                         # set_code_queues вызывается из run.py


class SessionValidator:
    """
    Проверяет .session-файлы, при необходимости запрашивает SMS-код
    через controller-bot и добавляет валидные сессии в bots.db.
    """

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        *,
        proxy_manager: AsyncProxyManager | None = None,
        proxy_id: int | None = None,
        session_dir: str = "sessions",
        bots_db_path: str = "bots.db",
        proxy_info: dict | None = None,
        config: dict | None = None,
    ):
        self.api_id = api_id
        self.api_hash = api_hash
        self.proxy_manager = proxy_manager
        self.proxy_id = proxy_id
        self.proxy_info = proxy_info        # используется при CLI-запуске
        self.config = config or {}
        self.session_dir = session_dir

        # настройки ожиданий IP
        self.validator_concurrency     = int(self.config.get("validator_concurrency", 3))
        self.ip_max_wait               = int(self.config.get("validator_ip_max_wait", 600))       # 10 мин
        self.ip_retry_interval         = int(self.config.get("validator_ip_retry_interval", 15))  # 15 сек
        self.round_sleep               = int(self.config.get("validator_round_sleep", 20))        # 20 сек

        self.bot_manager = BotManager(
            api_id=api_id,
            api_hash=api_hash,
            db_path=bots_db_path,
            sessions_dir=session_dir,
        )

    # --------------------------- вспомогательное --------------------------- #
    def _collect_known_sessions(self) -> set[str]:
        """
        Возвращает множество сессий, которые уже считаются «известными/проверенными»:
        1) все имена из bots.db (таблица bots — независимо от статусов),
        2) все .session-файлы, уже лежащие в рабочей директории validator’а (sessions_dir).
        """
        known = set()

        try:
            rows = self.bot_manager.list_bots()  # (session_name, phone, last_used, is_banned)
            for r in rows:
                # sqlite3.Row → индексация по имени или позиции
                name = r["session_name"] if isinstance(r, dict) else r[0]
                if name:
                    known.add(name)
        except Exception:
            # на всякий случай не рушим выполнение валидатора
            pass

        try:
            if os.path.isdir(self.session_dir):
                for fn in os.listdir(self.session_dir):
                    if fn.endswith(".session"):
                        known.add(fn[:-8])  # без суффикса ".session"
        except Exception:
            pass

        return known

    async def _get_proxy_with_wait(self, session_name: str) -> dict | None:
        """
        Пытается получить доступный прокси с ожиданием.
        Использует тот же менеджер, что и планировщик реакций:
        AsyncProxyManager.get_available_proxy — внутри уже есть
        логика межпроцессной блокировки и смены IP.
        """
        # если прокси задан напрямую (CLI), возвращаем сразу
        if self.proxy_info:
            return self.proxy_info

        if not (self.proxy_manager and self.proxy_id):
            return None

        deadline = time.time() + self.ip_max_wait
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            info = await self.proxy_manager.get_available_proxy([self.proxy_id], session_name=session_name)
            if info and info.get("status") == "ok" and info.get("socks5_ip"):
                return info

            # Сообщим в лог «ждём IP», но не помечаем ошибкой — сессия пойдёт в следующий заход
            if attempt == 1:
                log_session_status(
                    session_name.split("_")[0] if "_" in session_name else "unknown",
                    session_name,
                    "waiting",
                    "Ожидание доступного IP/смены IP"
                )
            await asyncio.sleep(self.ip_retry_interval)

        return None

    # ------------------------------------------------------------------ #
    #  Проверка одной сессии
    # ------------------------------------------------------------------ #
    async def validate_single_session(
        self,
        session_path: str,
        session_name: str,
        phone: str,
    ) -> bool:
        """
        Возвращает True, если проверка завершена (успех/ошибка/бан/2FA и т.п.).
        Возвращает False, если сейчас IP недоступен — следует повторить позже.
        """
        # 🔒 межпроцессный lock на сессию (учитывает MIN_REUSE_DELAY внутри session_store)
        if not await store.acquire(session_name):
            log_session_status(phone, session_name, "busy", "Сессия занята в другом процессе")
            return True  # работа по этой сессии уже идёт где-то ещё — считаем обработанной

        client: TelegramClient | None = None
        proxy_info: dict | None = None

        should_add = False

        try:
            # 1) ---- выбираем/ждём прокси --------------------------------
            proxy_info = await self._get_proxy_with_wait(session_name)
            if (
                not proxy_info
                or proxy_info.get("status") != "ok"
                or not proxy_info.get("socks5_ip")
            ):
                # Не удалось получить IP даже после ожидания — НЕ логируем error/ban,
                # а даём шанс следующему заходу.
                return False

            proxy = (
                socks.SOCKS5,
                proxy_info["socks5_ip"],
                int(proxy_info["socks5_port"]),
                True,
                proxy_info["proxy_login"],
                proxy_info["proxy_pass"],
            )

            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]TelegramClient path = {session_path}")
            # 2) ---- подключаемся ----------------------------------------
            client = TelegramClient(session_path, self.api_id, self.api_hash, proxy=proxy)
            await client.connect()

            if await client.is_user_authorized():
                log_session_status(phone, session_name, "success")
                should_add = True
                return True

            # 3) ---- ждём SMS-код от администратора ----------------------
            timeout = self.config.get("sms_code_timeout", 120)
            try:
                code = await code_manager.wait_for_code(session_name, phone, timeout=timeout)
            except TimeoutError:
                log_session_status(phone, session_name, "error", "Timeout waiting for SMS code")
                return True

            # 4) ---- авторизация -----------------------------------------
            should_add = False
            try:
                await client.sign_in(phone=phone, code=code)
                if await client.is_user_authorized():
                    should_add = True
                    log_session_status(phone, session_name, "success")
                else:
                    log_session_status(phone, session_name, "banned", "Auth failed after code")
            except PhoneCodeInvalidError:
                log_session_status(phone, session_name, "error", "Invalid code")
            except SessionPasswordNeededError:
                log_session_status(phone, session_name, "error", "2FA not supported")
            except Exception as e:
                log_session_status(phone, session_name, "error", f"Auth error: {e}")
            finally:
#                if should_add:
#                    self.bot_manager.add_bot(session_name, phone, source_path=session_path)
                return True

        except Exception as e:
            traceback.print_exc()
            log_session_status(phone, session_name, "error", str(e))
            return True

        finally:
            # 5) ---- закрываем клиент, освобождаем ресурсы ----------------
            try:
                if client:
                    await client.disconnect()

                if should_add:
                    self.bot_manager.add_bot(session_name, phone, source_path=session_path)

            except Exception:
                pass

            if self.proxy_manager and proxy_info and proxy_info.get("external_ip"):
                self.proxy_manager.release_proxy_ip(proxy_info["external_ip"], session_name)

            await store.release(session_name)        # 🔓

    # ------------------------------------------------------------------ #
    #  Проверка всех .session-файлов в папке — в несколько заходов
    # ------------------------------------------------------------------ #
    async def validate_folder(self, path: str):
        # 0) Собираем уже известные/проверенные
        known = self._collect_known_sessions()

        # 1) Собираем список сессий из входной папки, фильтруя «известные»
        pending: list[tuple[str, str, str]] = []
        for fname in os.listdir(path):
            if not fname.endswith(".session"):
                continue
            name  = fname[:-8]
            full  = os.path.join(path, fname)
            phone = name.split("_")[0] if "_" in name else "unknown"

            if name in known:
                # Пропускаем без проверки и расхода IP
                log_session_status(phone, name, "skipped", "Уже присутствует в bots.db/рабочих сессиях")
                continue

            pending.append((full, name, phone))

        if not pending:
            return

        # 2) Мягкое ограничение параллелизма
        sem = asyncio.Semaphore(self.validator_concurrency)

        async def _run_one(item):
            full, name, phone = item
            async with sem:
                return await self.validate_single_session(full, name, phone)

        # 3) Многораундовая обработка: повторяем те, кому не хватило IP
        round_idx = 0
        while pending:
            round_idx += 1
            tasks = [asyncio.create_task(_run_one(item)) for item in pending]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            new_pending: list[tuple[str, str, str]] = []
            progressed = False
            for (item, res) in zip(pending, results):
                if isinstance(res, Exception):
                    progressed = True
                    continue
                if res is True:
                    progressed = True
                else:
                    new_pending.append(item)

            pending = new_pending

            if pending and not progressed:
                await asyncio.sleep(self.round_sleep)


# ---------------------------------------------------------------------- #
#  Точка входа процесса-валидатора (вызывается из run.py)
# ---------------------------------------------------------------------- #
def run_validator_process(
    queue,              # multiprocessing.Queue: {"type":"check_sessions", "path":...}
    code_req_q,         # очередь запросов на код
    code_res_q,         # очередь ответов на код
    api_id,
    api_hash,
    proxy_api,
    proxy_id,
    config,
):
    """
    Отдельный процесс: ждёт задач на проверку сессий,
    запрашивает SMS-коды у controller-bot’а и валидирует файлы.
    """

    # подключаем очереди для кода
    code_manager.set_code_queues(code_req_q, code_res_q)

    # 1) Настройка прокси-менеджера
    proxy_manager = AsyncProxyManager(
        proxy_api,
        ip_db_path=config.get("ip_db_path", "ip_data.db"),
        max_total_bots_per_ip=config.get("max_bots_per_ip", 2),
    )

    # 2) Создание валидатора сессий
    validator = SessionValidator(
        api_id=api_id,
        api_hash=api_hash,
        proxy_manager=proxy_manager,
        proxy_id=proxy_id,
        session_dir=config.get("sessions_dir", "sessions"),
        bots_db_path=config.get("bots_db_path", "bots.db"),
        proxy_info=None,
        config=config,
    )

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Validator] 🔁 процесс запущен, ожидание заданий…")

    while True:
        msg = queue.get()           # блокирующий вызов multiprocessing.Queue
        if msg.get("type") != "check_sessions":
            continue
        path = msg.get("path")
        if not path:
            continue
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Validator] 🔍 Проверяем сессии в папке: {path}")
        asyncio.run(validator.validate_folder(path))

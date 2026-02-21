# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Reaction worker pool (reaction_worker_pool.py)
#
# This is the *executor* side of the system. Workers continuously:
#   1) reserve_next(): atomically reserve one job row from bots.db/jobs
#   2) pick a Telegram session (.session file) that is eligible for the job
#   3) acquire an IPC session lock (session_store.acquire) to avoid reuse races
#   4) acquire a mobile proxy / rotate IP if needed (proxy_manager.AsyncProxyManager)
#   5) connect TelegramClient and execute exactly ONE job:
#        - collect_posts: fetch recent messages and store reaction counters
#        - react: send a reaction emoji to the target message
#        - validate_session: login (request SMS code via controller_bot)
#   6) mark_done / requeue / dead-letter the job depending on outcome
#   7) release proxy usage and session lock
#
# The pool itself does not decide "which posts need reactions" — that is planned
# offline by scheduler_bot.py into the jobs table.
# -----------------------------------------------------------------------------

"""
reaction_worker_pool.py — общий пул воркеров: ВЫПОЛНЯЕТ задачи из очереди `jobs`.
Воркеры НЕ решают «ставить ли реакцию» и НЕ сканируют каналы — они только исполняют:
  • collect_posts   — собрать посты (обновить кеш) для chat_id
  • react           — поставить emoji на (chat_id, msg_id) (эмодзи задано в задаче)
  • validate_session— проверить/авторизовать конкретную session_name

Telegram затрагивается ТОЛЬКО после успешного `reserve_next`.
"""
import asyncio
import os
from datetime import datetime
from urllib.parse import urlparse
from telethon.tl.types import PeerChannel
import random

import socks
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.errors.rpcerrorlist import (
    SessionRevokedError,
    AuthKeyUnregisteredError,
    UserDeactivatedError,
    UserDeactivatedBanError,
    ChannelPrivateError,
    UserNotParticipantError,
    UserAlreadyParticipantError
)
from telethon.tl.functions.messages import ImportChatInviteRequest
from session_store import store as session_store
import code_manager                         # set_code_queues вызывается из run.py

from job_store import (
    connect as jobs_connect,
    reserve_next,
    requeue_expired,
    mark_done, mark_dead,
    fail_and_maybe_requeue,
    set_fetched_now,
    get_validation_code,
    clear_validation_code,
)
from BotManager import BotManager
from PostManager import PostManager
from proxy_manager import AsyncProxyManager
from mobileproxy_api import MobileProxyAPI
#from session_store import store
from frozen_checker import check_frozen_without_messages
from controller_bot import log_session_status


class ReactionWorkerPool:

    async def _queue_refiller(self):
        """Периодически пополняет очередь готовыми сессиями и чистит её от забаненных/замороженных."""
        while True:
            try:
                await session_store.refill_ready()
                bad = []
                try:
                    for row in self.bot_manager.list_bots():
                        try:
                            banned = int(row[3]) if len(row) > 3 and row[3] is not None else 0
                        except Exception:
                            banned = 0
                        try:
                            frozen = int(row[4]) if len(row) > 4 and row[4] is not None else 0
                        except Exception:
                            frozen = 0
                        try:
                            revoked = int(row[5]) if len(row) > 5 and row[5] is not None else 0
                        except Exception:
                            revoked = 0
                        if banned or frozen or revoked:
                            bad.append(row[0])
                except Exception:
                    pass
                if bad:
                    await session_store.remove_many_from_queue(bad)
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Refiller] error: {e}")
            await asyncio.sleep(int(self.config.get("queue_refill_interval_sec", 10)))
    def __init__(self, api_id, api_hash, proxy_ids, config: dict):
        self.api_id   = api_id
        self.api_hash = api_hash
        self.config   = config
        self.proxy_ids = proxy_ids

        # paths / dbs
        self.sessions_dir = config.get("sessions_dir", "sessions")
        self.bots_db_path = config.get("bots_db_path", "bots.db")
        self.session_unpack_dir = config.get("session_unpack_dir", "/tmp/telethon_sessions")
        self.posts_db_path = config.get("posts_db_path", "posts.db")
        os.makedirs(self.sessions_dir, exist_ok=True)

        # managers
        self.bot_manager = BotManager(api_id, api_hash, db_path=self.bots_db_path, sessions_dir=self.sessions_dir)
        self.proxy_api = MobileProxyAPI(config["mobileproxy_token"])
        self.proxy_manager = AsyncProxyManager(
            self.proxy_api,
            ip_db_path=config.get("ip_db_path", "ip_data.db"),
            max_total_bots_per_ip=config.get("max_bots_per_ip", 2),
        )

        # timings
        self.worker_sleep   = float(config.get("worker_sleep", 1.0))
        self.reaction_delay = float(config.get("reaction_delay", 1.5))
        self.min_reuse_delay = int(config.get("min_bot_reuse_delay", 120))
        self.job_ttl = int(config.get("job_reserve_ttl_sec", 180))

        # extras
        self.channel_invites = config.get("channel_invite_links", {})

    async def _pick_session(self, *, relaxed: bool = False,
                            chat_id: int | None = None, msg_id: int | None = None) -> str | None:
        bots = self.bot_manager.get_active_bots()
        eligible_whitelist = None
        if chat_id is not None and msg_id is not None:
            eligible_whitelist = set(self.bot_manager.eligible_bots_for_post(chat_id, msg_id))
            if not eligible_whitelist:
                return None  # никого нет — дальше пусть решает worker_loop

        now = datetime.utcnow()
        def ok(last_used: str | None, name: str) -> bool:
            if eligible_whitelist is not None and name not in eligible_whitelist:
                return False
            if chat_id is not None and not self.bot_manager.can_access_chat(name, chat_id):
                return False
            if not last_used:
                return True
            try:
                return (now - datetime.fromisoformat(last_used)).total_seconds() >= self.min_reuse_delay
            except Exception:
                return True

        candidates, weights = [], []
        for name, last_used in bots:
            if ok(last_used, name):
                w = 999999.0 if not last_used else max(1.0, (now - datetime.fromisoformat(last_used)).total_seconds())
                candidates.append(name); weights.append(w)

        if not candidates:
            return None
        return random.choices(candidates, weights=weights, k=1)[0]

    async def _make_proxy(self, proxy_info: dict | None):
        if not proxy_info or proxy_info.get("status") != "ok":
            return None
        return (
            socks.SOCKS5,
            proxy_info["socks5_ip"],
            int(proxy_info["socks5_port"]),
            True,
            proxy_info["proxy_login"],
            proxy_info["proxy_pass"]
        )



    @staticmethod
    def _is_invite_invalid(exc: Exception) -> bool:
        """True если попытка ImportChatInviteRequest упала из-за протухшего/невалидного инвайта."""
        s = str(exc).lower()
        # Telethon может давать InviteHashExpired/InviteHashInvalid, или текстовую формулировку
        if 'importchatinviterequest' in s and ('expired' in s or 'not valid' in s or 'invalid' in s):
            return True
        if 'invitehashexpired' in s or 'invitehashinvalid' in s:
            return True
        if 'invite_hash_expired' in s or 'invite_hash_invalid' in s:
            return True
        return False

    @staticmethod
    def _is_no_access(exc: Exception) -> bool:
        """True если ошибка означает отсутствие доступа/участия в канале."""
        # типы уже импортированы сверху, но на всякий случай дублируем по строке
        if isinstance(exc, (ChannelPrivateError, UserNotParticipantError)):
            return True
        s = str(exc).lower()
        if 'channelprivateerror' in s or 'usernotparticipanterror' in s:
            return True
        if 'channel_private' in s or 'user_not_participant' in s:
            return True
        if 'chat_admin_required' in s:
            return True
        return False
    async def _safe_get_channel(self, client, chat_id: int):
        """
        Гарантированно возвращает entity канала, даже если кэш пуст,
        или аккаунт уже/ещё состоит в привате и т.п.
        """
        invite_url = self.channel_invites.get(str(chat_id))

        # 1) Пробуем сразу с явным типом
        try:
            return await client.get_entity(PeerChannel(chat_id))
        except Exception:
            pass

        # 2) Прогреваем кэш диалогов
        try:
            await client.get_dialogs()
            return await client.get_entity(PeerChannel(chat_id))
        except Exception:
            pass

        # 3) Если есть инвайт — пробуем вступить (или игнорируем, если уже участник)
        if invite_url:
            try:
                invite_hash = urlparse(invite_url).path.split("+")[-1]
                await client(ImportChatInviteRequest(invite_hash))
            except UserAlreadyParticipantError:
                pass

            await client.get_dialogs()
            return await client.get_entity(PeerChannel(chat_id))

        # если совсем никак — пробрасываем наружу
        raise

    async def _ensure_session_file(self, session_name: str) -> bool:
        """
        Гарантирует, что .session-файл находится в self.sessions_dir.
        Ищет в session_unpack_dir (включая подпапки unpacked_*).
        Если найден — переносит в self.sessions_dir.
        """
        base = f"{session_name}.session"
        primary = os.path.join(self.sessions_dir, base)
        if os.path.exists(primary):
            return True

        root = self.session_unpack_dir
        if not root or not os.path.isdir(root):
            return False

        # 1) прямой путь
        direct = os.path.join(root, base)
        if os.path.exists(direct):
            os.makedirs(self.sessions_dir, exist_ok=True)
            try:
                os.replace(direct, primary)
            except Exception:
                import shutil
                shutil.move(direct, primary)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W] Переместил {base} → {self.sessions_dir}")
            return True

        # 2) рекурсивный поиск (unpacked_*)
        for cur, dirs, files in os.walk(root):
            if base in files:
                src = os.path.join(cur, base)
                os.makedirs(self.sessions_dir, exist_ok=True)
                try:
                    os.replace(src, primary)
                except Exception:
                    import shutil
                    shutil.move(src, primary)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W] Нашёл {base} в {cur}; переместил → {self.sessions_dir}")
                return True

        return False


    async def worker_loop(self, wid: int):
        conn = jobs_connect(self.bots_db_path)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Worker {wid}] started")
        while True:
            try:
                requeue_expired(conn, ttl_sec=self.job_ttl)

                # 1) резервируем задачу (атомарно)
                job = reserve_next(conn, worker_id=f"W{wid}")
                if not job:
                    await asyncio.sleep(self.worker_sleep)
                    continue

                job_id  = job["id"] 
                jtype   = job["type"]
                chat_id = job["chat_id"]
                msg_id  = job["msg_id"]
                emoji   = job["emoji"]
                prefer_session = job["session_name"] if "session_name" in job.keys() else None
#                prefer_session = job.get("session_name") if isinstance(job, dict) else None

                # 2) выбрать сессию
                session_name = None
                if jtype == "react":
                    # а) соберём кандидатов, которые ЕЩЁ НЕ реагировали на этот пост
                    bots_rows = self.bot_manager.get_active_bots()  # [(name, last_used), ...]
                    candidates = []
                    now = datetime.utcnow()
                    for name, last_used in bots_rows:
                        if self.bot_manager.has_bot_reacted(name, chat_id, msg_id):
                            continue
                        if not self.bot_manager.can_access_chat(name, chat_id):
                            continue
                        if not self.bot_manager.can_access_chat(name, chat_id):
                            continue
                        # выдерживаем min_reuse_delay
                        ok = True
                        if last_used:
                            try:
                                ok = (now - datetime.fromisoformat(last_used)).total_seconds() >= self.min_reuse_delay
                            except Exception:
                                ok = True
                        if ok:
                            # вес — чем дольше отдыхал, тем выше шанс
                            w = 999999.0 if not last_used else max(1.0, (now - datetime.fromisoformat(last_used)).total_seconds())
                            candidates.append((name, w))

                    if not candidates:
                        # некому ставить реакцию — задача бесперспективна
                        #conn.execute("UPDATE jobs SET status='dead' WHERE id= ?", (job_id,))
                        mark_dead(conn, job_id)
                        continue

                    # б) пробуем ПО ЛОКУ сессии, чтобы реально захватить «живого» кандидата
                    #    (иначе два воркера могут одновременно «выбрать» одного и того же)
                    candidates.sort(key=lambda x: -x[1])  # сначала самые «отдохнувшие»
                    for name, _ in candidates:
                        if await session_store.acquire(name):
                            session_name = name
                            break

                    if not session_name:
                        # никто из подходящих прямо сейчас не доступен → мягкий ре-кью
                        fail_and_maybe_requeue(conn, job_id, backoff_sec=20)
                        continue

                else:
                    # прежняя логика для collect/validate

                    if jtype == "validate_session":
                        session_name = prefer_session
                    else:
                        session_name = await self._pick_session(relaxed=(jtype == "collect_posts"), chat_id=chat_id)

                    if not session_name and jtype == "collect_posts":
                        # Fallback: очередь session_store
                        try:
                            await session_store.refill_ready()
                            session_name = await session_store.dequeue()
                        except Exception:
                            session_name = None

                    if not session_name:
                        fail_and_maybe_requeue(conn, job_id, backoff_sec=30)
                        continue

                    # lock для collect/validate берём здесь (для react мы уже взяли выше)
                    if not await session_store.acquire(session_name):
                        fail_and_maybe_requeue(conn, job_id, backoff_sec=20)
                        continue

                client = None; proxy_info = None
                try:
                    # 4) прокси
                    proxy_info = await self.proxy_manager.get_available_proxy(self.proxy_ids, session_name=session_name)
                    proxy = await self._make_proxy(proxy_info)
                    if not proxy:
                        fail_and_maybe_requeue(conn, job_id, backoff_sec=60)
                        continue

                    # 5) TelegramClient
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]TelegramClient session_name = {session_name}")

                    ok_file = await self._ensure_session_file(session_name)
                    if ok_file == False:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]Session not found.")
                        #fail_and_maybe_requeue(conn, job_id, backoff_sec=60)
                        continue

                    if jtype == "react":

                        # двойная страховка: если наш бот уже реагировал — закрываем задачу без вызова TG
                        if self.bot_manager.has_bot_reacted(session_name, chat_id, msg_id):
                            #mark_done(conn, job_id)
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] (protection: double reaction) for {chat_id} for session: {session_name}")
                            mark_dead(conn, job_id)
                            continue

                        skip_sessions = ['959682925135_189438905_telethon.session','959683715324_189438923_telethon.session','959683481504_189438914_telethon.session','959674991231_189217116_telethon.session','959676857252_189217233_telethon.session','959678411130_189217354_telethon.session', '959675875663_189217188_telethon.session']

                        # Принудительно не даю некоторым сессиям ставить реакции!
                        if any(session_name in s for s in skip_sessions):
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] skip reaction for {chat_id} for session: {session_name}")
                            mark_dead(conn, job_id)
                            continue                             

                    session_path = os.path.join(self.sessions_dir, session_name)
                    client = TelegramClient(session_path, self.api_id, self.api_hash, proxy=proxy)
                    await client.connect()

                    if jtype == "validate_session":
                        # Only DB-persisted code path.
                        phone = session_name.split("_")[0] if "_" in session_name else None
                        if await client.is_user_authorized():
                            self.bot_manager.add_bot(session_name, phone or "")
                            self.bot_manager.update_last_used(session_name)
                            log_session_status(phone, session_name, "success")
                            mark_done(conn, job_id)
                            continue
                        try:
                            timeout = int(self.config.get("sms_code_timeout", 120))
                            code = await code_manager.wait_for_code(session_name, phone or "unknown", timeout=timeout)
                            await client.sign_in(phone=phone, code=code)
                            if await client.is_user_authorized():
                                self.bot_manager.add_bot(session_name, phone or "")
                                self.bot_manager.update_last_used(session_name)
                                mark_done(conn, job_id)
                            else:
                                fail_and_maybe_requeue(conn, job_id, backoff_sec=120)
                        except Exception as e:

                            msg = str(e).lower()
                            if "cancelled" in msg or "cancel" in msg:
                                try:
                                    #conn.execute("UPDATE jobs SET status='dead', reserved_by=NULL, reserved_at=NULL WHERE id=?", (job_id,))
                                    mark_dead(conn, job_id)
                                except Exception:
                                    pass
                            else:
                                fail_and_maybe_requeue(conn, job_id, backoff_sec=180)

                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] validate error for {session_name}: {e}")
                            log_session_status(phone, session_name, "error", "Timeout waiting for SMS code")
 #                           fail_and_maybe_requeue(conn, job_id, backoff_sec=180)
                            continue
                        # ... (без изменений весь блок авторизации) ...
                        if await client.is_user_authorized():
                            if hasattr(self.bot_manager, "register_bot_if_missing"):
                                self.bot_manager.register_bot_if_missing(session_name)
                            self.bot_manager.update_last_used(session_name)
                            mark_done(conn, job_id)
                        else:
                            code = get_validation_code(conn, session_name)
                            phone = session_name.split("_")[0] if "_" in session_name else None
                            if code and phone:
                                try:
                                    await client.sign_in(phone=phone, code=code)
                                    if await client.is_user_authorized():
                                        if hasattr(self.bot_manager, "register_bot_if_missing"):
                                            self.bot_manager.register_bot_if_missing(session_name)
                                        clear_validation_code(conn, session_name)
                                        self.bot_manager.update_last_used(session_name)
                                        mark_done(conn, job_id)
                                    else:
                                        fail_and_maybe_requeue(conn, job_id, backoff_sec=120)
                                except Exception as e:
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] sign_in error for {session_name}: {e}")
                                    fail_and_maybe_requeue(conn, job_id, backoff_sec=180)
                            else:
                                try:
                                    if hasattr(code_manager, "code_request_queue"):
                                        code_manager.code_request_queue.put({"session": session_name, "phone": phone or "unknown"})
                                except Exception as e:
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] code request enqueue error: {e}")
                                fail_and_maybe_requeue(conn, job_id, backoff_sec=180)

                    else:
                        # freeze-check перед collect/react
                        try:
                            freeze = await check_frozen_without_messages(client)
                            inf = (freeze or {}).get("inference")
                            if inf == "write_restricted_global":
                                self.bot_manager.mark_frozen(session_name, 1)
                                try: await session_store.remove_from_queue(session_name)
                                except Exception: pass
                                raise RuntimeError("write_restricted_global")
                            elif inf in ("account_deactivated_or_banned",):
                                self.bot_manager.mark_banned(session_name)
                                try: await session_store.remove_from_queue(session_name)
                                except Exception: pass
                                raise RuntimeError("banned")
                            elif inf in ("session_invalid_or_revoked",):
                                self.bot_manager.mark_revoked(session_name)
                                try: await session_store.remove_from_queue(session_name)
                                except Exception: pass
                                raise RuntimeError("revoked")
                            elif inf == "temporarily_rate_limited":
                                raise RuntimeError("flood_wait")
                        except Exception as _e:
                            # если внутри check_frozen распознали ревок/бан — бот уже помечен, просто перекидываем задачу на другого
                            fail_and_maybe_requeue(conn, job_id, backoff_sec=10)
                            continue

                        pm = PostManager(client, db_path=self.posts_db_path)

                        if jtype == "collect_posts":
                            # ... (как у тебя) ...
                            try:
                                limit = int(self.config.get("message_limit", 10))
                                # ✅ безопасно резолвим канал и берём его id
                                try:
                                    peer = await self._safe_get_channel(client, chat_id)
                                    self.bot_manager.mark_chat_access(session_name, chat_id, 'ok')
                                    real_id = peer.id
                                except (SessionRevokedError, AuthKeyUnregisteredError, UserDeactivatedError, UserDeactivatedBanError) as join_auth_err:
                                    # сессия недействительна → помечаем и выкидываем из очереди
                                    self.bot_manager.mark_revoked(session_name)
                                    try: await session_store.remove_from_queue(session_name)
                                    except Exception: pass
                                    fail_and_maybe_requeue(conn, job_id, backoff_sec=5)
                                    continue
                                except Exception as join_err:
                                    if self._is_invite_invalid(join_err):
                                        self.bot_manager.mark_chat_access(session_name, chat_id, 'invite_invalid', str(join_err))
                                        fail_and_maybe_requeue(conn, job_id, backoff_sec=5)
                                        continue
                                    if self._is_no_access(join_err):
                                        self.bot_manager.mark_chat_access(session_name, chat_id, 'no_access', str(join_err))
                                        fail_and_maybe_requeue(conn, job_id, backoff_sec=5)
                                        continue
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] safe_get_channel failed {chat_id}: {join_err}")
                                    fail_and_maybe_requeue(conn, job_id, backoff_sec=120)
                                    continue


                                result = await pm.fetch_posts(channel_id=real_id, limit=limit)

                                if result.get("status") == "ok":
                                    set_fetched_now(conn, real_id)
                                    self.bot_manager.update_last_used(session_name)
                                    mark_done(conn, job_id)
                                else:
                                    fail_and_maybe_requeue(conn, job_id, backoff_sec=120)
                            except (SessionRevokedError, AuthKeyUnregisteredError, UserDeactivatedError, UserDeactivatedBanError):
                                self.bot_manager.mark_revoked(session_name)
                                try: await session_store.remove_from_queue(session_name)
                                except Exception: pass
                                fail_and_maybe_requeue(conn, job_id, backoff_sec=120)
                            except Exception as e:
                                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] collect error {chat_id}: {e}")
                                fail_and_maybe_requeue(conn, job_id, backoff_sec=120)

                        elif jtype == "react":

                            try:                       

                                # ✅ безопасно резолвим канал
                                try:
                                    peer = await self._safe_get_channel(client, chat_id)
                                    self.bot_manager.mark_chat_access(session_name, chat_id, 'ok')
                                except (SessionRevokedError, AuthKeyUnregisteredError, UserDeactivatedError, UserDeactivatedBanError) as e:
                                    self.bot_manager.mark_revoked(session_name)
                                    try: await session_store.remove_from_queue(session_name)
                                    except Exception: pass
                                    fail_and_maybe_requeue(conn, job_id, backoff_sec=5)
                                    continue
                                except Exception as e:
                                    if self._is_invite_invalid(e):
                                        self.bot_manager.mark_chat_access(session_name, chat_id, 'invite_invalid', str(e))
                                        fail_and_maybe_requeue(conn, job_id, backoff_sec=5)
                                        continue
                                    if self._is_no_access(e):
                                        self.bot_manager.mark_chat_access(session_name, chat_id, 'no_access', str(e))
                                        fail_and_maybe_requeue(conn, job_id, backoff_sec=5)
                                        continue
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] safe_get_channel/react failed {chat_id}: {e}")
                                    fail_and_maybe_requeue(conn, job_id, backoff_sec=120)
                                    continue


                                # финальная проверка перед отправкой реакции
                                try:
                                    ovr = pm.get_overrides(chat_id, msg_id)
                                    if int(ovr.get("blocked", 0)):
                                        mark_dead(conn, job_id)
                                        continue
                                    fe = ovr.get("forced_emoji")
                                    if fe:
                                        emoji = fe
                                except Exception:
                                    pass
                                await pm.set_reaction(peer, msg_id, emoji)

                                # лог может выполниться повторно, но уникальный индекс не даст задублировать запись
                                self.bot_manager.log_action(session_name, 'reaction', msg_id, chat_id, details=emoji)

                                self.bot_manager.update_last_used(session_name)
                                mark_done(conn, job_id)
                                await asyncio.sleep(self.reaction_delay)

                            except (SessionRevokedError, AuthKeyUnregisteredError, UserDeactivatedError, UserDeactivatedBanError):
                                self.bot_manager.mark_revoked(session_name)
                                try: await session_store.remove_from_queue(session_name)
                                except Exception: pass
                                fail_and_maybe_requeue(conn, job_id, backoff_sec=120)
                            except FloodWaitError as fw:
                                fail_and_maybe_requeue(conn, job_id, backoff_sec=int(getattr(fw, 'seconds', 60)))
                            except Exception as e:
                                if self._is_invite_invalid(e):
                                    self.bot_manager.mark_chat_access(session_name, chat_id, 'invite_invalid', str(e))
                                    fail_and_maybe_requeue(conn, job_id, backoff_sec=5)
                                elif self._is_no_access(e):
                                    self.bot_manager.mark_chat_access(session_name, chat_id, 'no_access', str(e))
                                    fail_and_maybe_requeue(conn, job_id, backoff_sec=5)
                                else:
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] react error {chat_id}/{msg_id}: {e}")
                                    fail_and_maybe_requeue(conn, job_id, backoff_sec=120)

                finally:
                    try:
                        if client: await client.disconnect()
                    except Exception: pass
                    try:
                        if self.proxy_manager and proxy_info and proxy_info.get("external_ip"):
                            self.proxy_manager.release_proxy_ip(proxy_info["external_ip"], session_name)
                    except Exception: pass
                    await session_store.release(session_name)

            except asyncio.CancelledError:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] cancelled; exiting worker loop")
                break
            except Exception as loop_err:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][W{wid}] loop error: {loop_err}")
                await asyncio.sleep(self.worker_sleep)



    async def run_all(self):
        # seed session_lock from bots on startup so queue/dequeue works
        try:
            names = [row[0] for row in self.bot_manager.get_active_bots()]
            await session_store.ensure_present(names, mark_ready=True)
            await session_store.refill_ready()
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Pool] seed session_lock failed: {e}")
        max_workers = int(self.config.get("max_workers", 5))
        asyncio.create_task(self._queue_refiller())
        tasks = [asyncio.create_task(self.worker_loop(i)) for i in range(max_workers)]
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]✅ Workers started: {max_workers}")
        await asyncio.gather(*tasks)

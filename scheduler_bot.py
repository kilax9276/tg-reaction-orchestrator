# -----------------------------------------------------------------------------
# Scheduler / Planner (scheduler_bot.py)
#
# Despite the name, this module is an *offline planner*:
#   - it does NOT connect to Telegram
#   - it periodically inspects local DB state (posts.db + bots.db) and generates
#     work items in bots.db/jobs for workers to execute.
#
# Planning rules (see job_store.py):
#   - ensure_collect_jobs(): create collect_posts jobs if cache is stale
#   - rebuild_reaction_plan(): create react jobs based on:
#        - recent posts in posts.db
#        - current reaction counters
#        - per-channel target reactions (reaction_targets)
#        - probability weighting by message "age" (hyperbola params)
#        - cooldown between reactions per post (react_cooldown_sec)
#        - operator overrides (NO-REACT / forced emoji) from posts.db
#
# Sleep window:
#   If config.sleep.enabled is True, the planner pauses between sleep.start and
#   sleep.end in the configured timezone (sleep_window.py).
# -----------------------------------------------------------------------------

# File: scheduler_bot.py

import asyncio
import socks
import random
from sleep_window import is_sleep_time, seconds_until_wake
from datetime import datetime
from urllib.parse import urlparse

from telethon.tl.types import PeerChannel
from telethon.tl.functions.messages import ImportChatInviteRequest
from BotManager import BotManager
from PostManager import PostManager

from session_lock import acquire_session, release_session
from frozen_checker import check_frozen_without_messages

class SchedulerBot:
    def __init__(self, api_id, api_hash, config, proxy_manager, proxy_ids, lock=None):
        self.api_id = api_id
        self.api_hash = api_hash
        self.config = config
        self.proxy_manager = proxy_manager
        self.proxy_ids = proxy_ids
        self.lock = lock  # lock создадим в run(), чтобы не требовался активный event loop
        self.bot_manager = BotManager(api_id, api_hash)
        self._needs_more_bots = False
        self.purchase_task = None
        self._load_config()

    def _load_config(self):
        self.k = self.config.get("hyperbola_k", 1.0)
        self.c = self.config.get("hyperbola_c", 1.0)
        self.d = self.config.get("hyperbola_d", 1.0)
        self.channel_ids = self.config.get("channel_ids", [])
        self.targets = self.config.get("reaction_targets", {})
        self.dev = self.config.get("target_deviation", 0)
        self.msg_limit = self.config.get("message_limit", 5)
        self.total_limit = self.config.get("total_reactions_per_run", 5)
        self.threshold = self.config.get("reaction_threshold", 5)
        self.purchase_delay = self.config.get("purchase_delay", 1)


    async def purchase_single_bot(self, idx: int) -> bool:
        # заглушка: эмулируем задержку покупки одного бота
#        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ [Purchase] старт покупки бота #{idx+1}")
        await asyncio.sleep(self.purchase_delay)
#        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]✅ [Purchase] бот #{idx+1} куплен")
        # здесь нужно вызывать self.bot_manager.add_bot(...) для реальной интеграции
        return True

    async def purchase_bots_stub_global(self, needed_count: int) -> bool:
        """
        Параллельно «покупаем» нужное количество ботов.
        Возвращаем True, если все задачи завершились успешно.
        """
        tasks = [
            asyncio.create_task(self.purchase_single_bot(i))
            for i in range(needed_count)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successes = sum(1 for r in results if r is True)
        ok = (successes == needed_count)
        # if not ok:
        #     print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ [Purchase] из {needed_count} ботов куплено только {successes}")
        return ok

    
async def fetch_messages(self):
    """
    ОФЛАЙН-ПЛАНИРОВЩИК.
    Никаких подключений к Telegram: только формирование задач в jobs.
    - collect_posts: не чаще posts_refresh_interval_sec на канал (priority=0.0)
    - react: формируется по posts.db/targets + react_cooldown_sec (ставит not_before)
    """
    from job_store import connect as jobs_connect, ensure_collect_jobs, rebuild_reaction_plan
    from PostManager import PostManager

    posts_refresh = int(self.config.get("posts_refresh_interval_sec", 300))
    db_path_posts = self.config.get("posts_db_path", "posts.db")
    db_path_bots  = self.config.get("bots_db_path", "bots.db")

    pm = PostManager(client=None, db_path=db_path_posts)

    conn = jobs_connect(db_path_bots)
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_collect_jobs(conn, self.channel_ids, posts_refresh)
        rebuild_reaction_plan(conn, pm, self.bot_manager, self.config)
        conn.commit()
    finally:
        conn.close()


# --- Monkey-patch: ensure SchedulerBot has an async run(self) method bound to class ---
# async def _scheduler_run(self):
#     # create lock lazily under a running loop
#     if getattr(self, "lock", None) is None:
#         self.lock = asyncio.Lock()
#     await asyncio.sleep(self.config.get("initial_delay", 0))
#     while True:
#         async with self.lock:
#             # call module-level planner which operates on self
#             await fetch_messages(self)

#         delay = self.config.get("interval", 15)
#         if getattr(self, "_needs_more_bots", False):
#             delay = 10
#         await asyncio.sleep(delay)

async def _scheduler_run(self):
    # create lock lazily under a running loop
    if getattr(self, "lock", None) is None:
        self.lock = asyncio.Lock()

    await asyncio.sleep(self.config.get("initial_delay", 0))

    while True:
        # 👇 СНАЧАЛА проверяем "сон" — до захвата локов и любой работы
        if is_sleep_time(self.config):
            wait = seconds_until_wake(self.config)
            # необязательно, но полезно видеть в логах
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}][Scheduler] 😴 sleep mode: pausing for {wait}s")
            await asyncio.sleep(wait)
            # после пробуждения цикл начнется заново и уже перейдет к работе
            continue

        async with self.lock:
            # call module-level planner which operates on self
            await fetch_messages(self)

        # обычный интервал (с логикой "нужно больше ботов")
        delay = self.config.get("interval", 15)
        if getattr(self, "_needs_more_bots", False):
            delay = 10

        # микроджиттер, чтобы несколько инстансов не тикали синхронно
        delay += random.uniform(0, 0.5)

        await asyncio.sleep(delay)


# Bind as a method
try:
    SchedulerBot.run = _scheduler_run
except NameError:
    pass

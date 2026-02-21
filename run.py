# -----------------------------------------------------------------------------
# Project entrypoint (run.py)
#
# This script boots the whole system as a multi-process application:
#   1) Controller process: a Telegram *admin bot* (controller_bot.py) that
#      - manages admin permissions,
#      - accepts ZIP uploads with .session files,
#      - updates config.json,
#      - requests/receives SMS login codes from admins and forwards them to workers.
#   2) Reaction worker pool process: executes jobs from the SQLite `jobs` queue
#      (reaction_worker_pool.py + job_store.py). Jobs include:
#        - collect_posts: refresh cached posts/reaction stats into posts.db
#        - react: put one reaction emoji on a specific message
#        - validate_session: log in a single session (asks SMS code via controller)
#   3) Scheduler loop (in the main process): an *offline planner* that does not
#      touch Telegram. It periodically creates/refreshes jobs in bots.db based on:
#        - channels list + refresh cadence (collect_posts)
#        - cached posts + reaction targets (react)
#      See scheduler_bot.py and job_store.rebuild_reaction_plan().
#
# Inter-process communication:
#   - session_queue: (currently reserved for legacy validation flow)
#   - code_request_queue / code_response_queue: SMS code requests & responses.
#
# State:
#   - bots.db: bots registry + actions log + jobs queue (SQLite WAL)
#   - posts.db: cached posts + reaction counters + operator overrides
#   - sessions_state.db: IPC session locks/queue (SQLite WAL) used by session_store.py
#
# NOTE: hard_reset_session_store() wipes sessions_state.db* files on every start.
#       That prevents stale locks after crashes, but also resets "cooldown" state.
# -----------------------------------------------------------------------------

# run.py

import multiprocessing
import asyncio

from session_store import hard_reset_session_store

# 🧹 сброс локов при рестарте
hard_reset_session_store()  # 🧹 сбрасываем перед запуском воркеров/планировщика


from telethon.sessions import StringSession

from controller_bot import run_controller_process
#from session_validator import run_validator_process
from scheduler_bot import SchedulerBot
from reaction_worker_pool import ReactionWorkerPool

from proxy_manager import AsyncProxyManager
from mobileproxy_api import MobileProxyAPI
from code_manager import set_code_queues
from datetime import datetime

import json



        
def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

def start_scheduler(api_id, api_hash, config, proxy_manager, proxy_ids):
    scheduler = SchedulerBot(
        api_id=api_id,
        api_hash=api_hash,
        config=config,
        proxy_manager=proxy_manager,
        proxy_ids=proxy_ids
    )
    asyncio.run(scheduler.run())

def start_reaction_pool(api_id, api_hash, config, proxy_ids, code_request_queue, code_response_queue):
    # Initialise code_manager queues INSIDE this child process
    from code_manager import set_code_queues as _set_code_queues_in_child
    _set_code_queues_in_child(code_request_queue, code_response_queue)

    pool = ReactionWorkerPool(api_id=api_id, api_hash=api_hash, proxy_ids=proxy_ids, config=config)
    asyncio.run(pool.run_all())

def main():
        
    config = load_config()

    api_id = config["api_id"]
    api_hash = config["api_hash"]
    proxy_ids = config.get("proxy_ids", [])

    proxy_api = MobileProxyAPI(config["mobileproxy_token"])
    proxy_manager = AsyncProxyManager(proxy_api, ip_db_path=config.get("ip_db_path", "ip_data.db"), max_total_bots_per_ip=config.get("max_bots_per_ip", 2))

    # очередь для заданий валидатору (check_sessions)
    session_queue = multiprocessing.Queue()

    # новые очереди для кода подтверждения
    code_request_queue  = multiprocessing.Queue()
    code_response_queue = multiprocessing.Queue()

    # инициализируем код-менеджер
    set_code_queues(code_request_queue, code_response_queue)


    # контроллер
    ctrl = multiprocessing.Process(
        target=run_controller_process,
        args=(session_queue, code_request_queue, code_response_queue, config)
    )
    ctrl.start()

    # валидатор
#    val = multiprocessing.Process(
#        target=run_validator_process,
#        args=(session_queue, code_request_queue, code_response_queue, api_id, api_hash, proxy_api, proxy_ids[0], config)
#    )
#    val.start()

    # ✅ Реакционный воркер-пул
    react = multiprocessing.Process(
        target=start_reaction_pool,
        args=(api_id, api_hash, config, proxy_ids, code_request_queue, code_response_queue)
    )
    react.start()

    # ⏱ Запуск планировщика
    try:
        start_scheduler(api_id, api_hash, config, proxy_manager, proxy_ids)
    except KeyboardInterrupt:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⛔ Завершение по Ctrl+C — все процессы остановлены")
        ctrl.terminate()
#        val.terminate()
        react.terminate()

if __name__ == "__main__":
    main()

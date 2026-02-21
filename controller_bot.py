# -----------------------------------------------------------------------------
# Controller bot (controller_bot.py)
#
# A Telegram bot (Telethon) intended to be used by operators/admins.
# It is the "human interface" for:
#   - uploading ZIP archives with Telethon .session files
#   - managing admin permissions (stored in SQLite: DB_PATH/admins table)
#   - providing SMS login codes during account validation (2FA via SMS)
#   - viewing runtime stats and cached posts, and applying per-post overrides
#     (NO-REACT / forced emoji)
#   - pushing new config.json (hot swap on next restart)
#
# How SMS codes flow:
#   worker/validator -> code_request_queue.put({"session": ..., "phone": ...})
#   controller thread reads queue and sends Telegram DM to admins with upload_zip permission
#   admin replies: "<session_name>: <code>" (or "<session_name>: esc" to cancel)
#   controller -> code_response_queue.put({"session": ..., "code": ...})
#   worker/validator awaits code_manager.wait_for_code() and continues sign_in().
#
# How session uploads are scheduled:
#   - ZIP unpacked into TEMP_DIR
#   - each .session is moved into sessions_dir
#   - for each session_name a jobs row is inserted:
#       type='validate_session', status='queued', session_name=<...>
#   - workers pick those jobs and validate/login accounts (ReactionWorkerPool)
#
# IMPORTANT:
#   - This module keeps a long-running Telethon client loop in a dedicated process.
#   - It writes operational logs into DB_PATH (session_log.db by default).
# -----------------------------------------------------------------------------

# controller_bot.py
# Управляющий Telegram-бот для работы с сессиями, конфигами и статистикой
# ──────────────────────────────────────────────────────────────────────
# Основные возможности:
#   • права админов (upload_zip / view_stats / edit_config / add_admins)
#   • /upload_mode  → загрузка ZIP-архива с сессиями
#   • /add_admin    → добавление нового админа с правами
#   • /stats        → статистика реакций ботов
#   • загрузка config.json и «горячая» подмена конфига
#   • рассылка запросов 2FA-кодов только администраторам с upload_zip
# ──────────────────────────────────────────────────────────────────────

import os, signal
import shutil
import zipfile
import sqlite3
import threading
import time
import asyncio
import json
from typing import Optional, Dict  
from datetime import datetime, timedelta
from multiprocessing import Queue

from BotManager import BotManager

from telethon import TelegramClient, events
from telethon.errors.rpcerrorlist import EntityBoundsInvalidError

from code_manager import set_code_queues
from job_store import connect as jobs_connect
from job_store import set_validation_code

def _md_escape(s: str) -> str:
    return (s or "").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[")
from PostManager import PostManager

# ──────────────────────────────────────────────────────────────────────
# Глобальные переменные
# ──────────────────────────────────────────────────────────────────────
config: Optional[Dict] = None
client: Optional[TelegramClient] = None

session_queue:   Optional[Queue] = None
code_request_queue:  Optional[Queue] = None
code_response_queue: Optional[Queue] = None

CODE_REQUESTS: dict[str, str | None] = {}   # session_name -> code/None (ожидаем)
admin_flags:   dict[int, dict]        = {}   # user_id -> {"awaiting_zip": bool}

SESSION_NAME = "controller_bot"

DB_PATH   = ""
TEMP_DIR  = ""
ADMIN_IDS: list[int] = []

WAIT_TIMEOUT = 10  # сек до ready()

# ──────────────────────────────────────────────────────────────────────
# Настройка / конфиг / очереди
# ──────────────────────────────────────────────────────────────────────
def load_config(cfg: dict):
    global config
    config = cfg


def set_session_queue(q: Queue):
    global session_queue
    session_queue = q


def _ensure_admins_table() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS admins (
               user_id INTEGER PRIMARY KEY,
               permissions TEXT
        )"""
    )
    conn.commit()
    conn.close()


ADMIN_DEFAULT_PERMS = {
    "upload_zip": True,
    "view_stats": True,
    "edit_config": True,
    "add_admins": True,
}


def get_admin_permissions(user_id: int) -> dict:
    """Возвращает словарь прав админа."""
    _ensure_admins_table()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT permissions FROM admins WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()

    if row and row[0]:
        try:
            return json.loads(row[0])
        except Exception:
            return {}
    # если пользователь в исходном списке admin_ids, даём права по умолчанию
    return ADMIN_DEFAULT_PERMS.copy() if user_id in ADMIN_IDS else {}


def has_permission(user_id: int, perm: str) -> bool:
    return get_admin_permissions(user_id).get(perm, False)


# ──────────────────────────────────────────────────────────────────────
# Логирование статуса проверок сессий
# ──────────────────────────────────────────────────────────────────────
def log_session_status(phone: str,
                       session_name: str,
                       status: str,
                       error_message: str | None = None) -> None:
    now = datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS session_checks (
               phone TEXT,
               session_name TEXT PRIMARY KEY,
               status TEXT,
               error_message TEXT,
               timestamp TEXT
        )"""
    )
    cur.execute(
        """INSERT OR REPLACE INTO session_checks
             (phone, session_name, status, error_message, timestamp)
           VALUES (?, ?, ?, ?, ?)""",
        (phone, session_name, status, error_message, now),
    )
    conn.commit()
    conn.close()


# ──────────────────────────────────────────────────────────────────────
# Вспомогательные
# ──────────────────────────────────────────────────────────────────────
async def wait_for_controller_ready(timeout: int = WAIT_TIMEOUT) -> bool:
    """Ждёт, пока TelegramClient будет готов и в БД есть хотя бы один админ."""
    loop = asyncio.get_event_loop()
    start = loop.time()
    while True:
        if client and get_all_admin_ids():
            return True
        if loop.time() - start > timeout:
            return False
        await asyncio.sleep(0.2)


# ──────────────────────────────────────────────────────────────────────
#  Telegram-хэндлеры
# ──────────────────────────────────────────────────────────────────────
# /start
async def start_handler(event):
    await event.respond(f"👋 Ваш Telegram ID: `{event.sender_id}`",
                        parse_mode="markdown")
    perms = get_admin_permissions(event.sender_id)
    if not perms:
        return

    cmds = ["⚙️ Команды:"]
    if perms.get("upload_zip"):  cmds.append("• /upload_mode – включить загрузку ZIP")
    if perms.get("add_admins"):  cmds.append("• /add_admin <id> <json_права>")
    if perms.get("view_stats"):  cmds.append("• /stats – статистика")
    if perms.get("edit_config"): cmds.append("• Отправьте `config.json` для обновления конфигурации.")

    _text = "\n".join(cmds)
    try:
        await event.respond(_text, parse_mode="markdown")
    except EntityBoundsInvalidError:
        await event.respond(_text, parse_mode=None)


# /add_admin
async def add_admin_handler(event):
#    if event.sender_id not in ADMIN_IDS or not has_permission(event.sender_id, "add_admins"):
    if not has_permission(event.sender_id, "add_admins"):
        return
    parts = event.raw_text.strip().split(" ", 2)
    if len(parts) != 3:
        await event.reply("⚠ Формат: `/add_admin <user_id> <JSON_права>`",
                          parse_mode="markdown")
        return
    try:
        uid = int(parts[1])
        perms = json.loads(parts[2])
    except Exception as err:
        await event.reply(f"❌ Ошибка разбора аргументов: {err}")
        return

    _ensure_admins_table()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("REPLACE INTO admins (user_id, permissions) VALUES (?, ?)",
                (uid, json.dumps(perms, ensure_ascii=False)))
    conn.commit()
    conn.close()

    await event.reply(f"✅ Админ `{uid}` добавлен/обновлён.\nПрава: `{perms}`",
                      parse_mode="markdown")


# /upload_mode
async def upload_mode_handler(event):
    if not has_permission(event.sender_id, "upload_zip"):
        return
    admin_flags[event.sender_id] = {"awaiting_zip": True}
    await event.reply("📥 Режим загрузки ZIP включён. Теперь отправьте архив.")

# /restart
async def restart_handler(event):
    if not has_permission(event.sender_id, "edit_config"):
        return
    await event.respond("♻️ Перезапуск всей системы...", parse_mode="markdown")
    os.kill(os.getppid(), signal.SIGINT)  # завершаем run.py


# /stats
async def stats_handler(event):
    if not has_permission(event.sender_id, "view_stats"):
        return
    try:
        posts_db = config.get("posts_db_path", "posts.db")
        bots_db  = config.get("bots_db_path",  "bots.db")

        bm = BotManager(config["api_id"], config["api_hash"], db_path=bots_db)
        active_count = len(bm.get_active_bots())
        bm.close()

        conn_p = sqlite3.connect(posts_db)
        cur_p  = conn_p.cursor()
        cur_p.execute("SELECT chat_id, msg_id FROM posts ORDER BY msg_id DESC LIMIT 10")
        posts = cur_p.fetchall()
        conn_p.close()

        lines = [f"🤖 Активных ботов: *{active_count}*\n"]
        if not posts:
            lines.append("🧐 Нет данных о постах.")
            _text = "\n".join(lines)  # ← тут было "\\n"
            try:
                await event.reply(_text, parse_mode="markdown")
            except EntityBoundsInvalidError:
                await event.reply(_text, parse_mode=None)
            return

        conn_a = sqlite3.connect(bots_db)
        cur_a  = conn_a.cursor()
        hour_ago = (datetime.utcnow() - timedelta(hours=1)).isoformat()

        lines.append("📝 *Статистика реакций (10 последних постов)*:\n")
        for chat_id, msg_id in posts:
            cur_a.execute("SELECT COUNT(*) FROM actions "
                          "WHERE chat_id=? AND target_msg_id=? AND action_type='reaction'",
                          (chat_id, msg_id))
            total = cur_a.fetchone()[0]

            cur_a.execute("SELECT COUNT(*) FROM actions "
                          "WHERE chat_id=? AND target_msg_id=? AND action_type='reaction' "
                          "AND timestamp>=?",
                          (chat_id, msg_id, hour_ago))
            last_hour = cur_a.fetchone()[0]
            lines.append(f"• {chat_id}/{msg_id}: +{total} всего, +{last_hour} за час")
        conn_a.close()

        _text = "\n".join(lines)  # ← тут было "\\n"
        try:
            await event.reply(_text, parse_mode="markdown")
        except EntityBoundsInvalidError:
            await event.reply(_text, parse_mode=None)
    except Exception as e:
        await event.reply(f"❌ Ошибка статистики: {e}")




# Приём файлов (ZIP + config.json)
async def file_handler(event):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]ZIP received")

    if event.sender_id not in ADMIN_IDS or not event.document:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]Not admin")
        return

    filename = (event.message.file.name or "").lower()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]filename = {filename}")

    # ── ZIP ────────────────────────────────────────────────
    if filename.endswith(".zip"):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]ZIP received from admin_id={event.sender_id}: filename={filename}")

        if not has_permission(event.sender_id, "upload_zip"):
            await event.reply("⛔ У вас нет прав на загрузку ZIP.")
            return
        if not admin_flags.get(event.sender_id, {}).get("awaiting_zip"):
            await event.reply("⚠ Сначала выполните /upload_mode, затем отправьте архив.")
            return

        admin_flags[event.sender_id]["awaiting_zip"] = False

        os.makedirs(TEMP_DIR, exist_ok=True)
        archive_path = os.path.join(TEMP_DIR, filename)
        await event.download_media(file=archive_path)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]ZIP saved to {archive_path} (size={os.path.getsize(archive_path)} bytes)")

        extract_dir = os.path.join(TEMP_DIR, f"unpacked_{int(time.time())}")
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(extract_dir)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]ZIP extracted to {extract_dir}; files={os.listdir(extract_dir)}")

        # Переносим .session в рабочую папку и ставим validate_session в очередь jobs
        sessions_dir = config.get("sessions_dir", "sessions")
        os.makedirs(sessions_dir, exist_ok=True)
        bots_db = config.get("bots_db_path", "bots.db")
        conn = jobs_connect(bots_db)
        cnt = 0
        try:
            for cur, dirs, files in os.walk(extract_dir):
                for fname in files:
                    if not fname.endswith(".session"):
                        continue

                    session_name = fname[:-8]
                    phone = session_name.split("_")[0] if "_" in session_name else "unknown"
                    src = os.path.join(cur, fname)
                    dst = os.path.join(sessions_dir, fname)

                    try:
                        if os.path.exists(dst):
                            os.remove(dst)
                        shutil.move(src, dst)
                    except Exception as e:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Не удалось переместить {fname}: {e}")
                        continue

                    log_session_status(phone, session_name, "ready")
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]Session ready → {session_name} ({phone})")

                    # ВАЖНО: используем datetime.utcnow(), т.к. импорт: from datetime import datetime
                    conn.execute(
                        "INSERT OR IGNORE INTO jobs (type, chat_id, priority, status, created_at, session_name) "
                        "VALUES (?, ?, ?, 'queued', ?, ?)",
                        ("validate_session", 0, 0.0, datetime.utcnow().isoformat(), session_name)
                    )
                    cnt += 1

            await event.reply(f"📦 Архив принят. Сессий: {cnt}. Поставил задачи на валидацию.")
        finally:
            conn.close()

        return  # ZIP обработан

    # ── config.json ────────────────────────────────────────
    if filename == "config.json":
        if not has_permission(event.sender_id, "edit_config"):
            await event.reply("⛔ У вас нет прав на изменение конфига.")
            return

        tmp_path = os.path.join(TEMP_DIR, f"config_{int(time.time())}.json")
        await event.download_media(file=tmp_path)

        try:
            with open(tmp_path, "r", encoding="utf-8") as f:
                new_cfg = json.load(f)
            with open("config.json", "w", encoding="utf-8") as f:
                json.dump(new_cfg, f, indent=2, ensure_ascii=False)
            await event.reply("✅ Конфигурация обновлена. Новые процессы подхватят её при следующем старте.")
        except Exception as e:
            await event.reply(f"❌ Ошибка в новом config.json: {e}")
        return


# Обработка кода подтверждения: <session_name>: <12345>
async def handle_code_response(event):
    if not has_permission(event.sender_id, "upload_zip"):
        return

    text = event.raw_text.strip()
    if ":" not in text:
        return
    name, code = [x.strip() for x in text.split(":", 1)]
    if name not in CODE_REQUESTS:
        return

    # отмена
    if code.lower() == "esc":
        CODE_REQUESTS.pop(name, None)
        await event.reply(f"❌ Ожидание для `{name}` отменено.")
        phone = name.split("_")[0] if "_" in name else "unknown"
        log_session_status(phone, name, "cancelled", "Отменено администратором")
        # Разбудим воркера, если он ждёт кода
        try:
            code_response_queue.put({"session": name, "cancel": True})
        except Exception:
            pass
        # Mark job dead in DB вместо сигнала через очередь
        try:
            bots_db = config.get("bots_db_path", "bots.db")
            conn = jobs_connect(bots_db)
            conn.execute(
                "UPDATE jobs SET status='dead', reserved_at=? "
                "WHERE type='validate_session' AND session_name=? AND status IN ('queued', 'reserved')",
                (datetime.utcnow().isoformat(), name)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Controller] cancel mark-dead failed for {name}: {e}")

    else:
        CODE_REQUESTS[name] = code
        await event.reply(f"✅ Код для `{name}` принят.")
        # Persist code to jobs.payload so workers can read it
        try:
            bots_db = config.get("bots_db_path", "bots.db")
            conn = jobs_connect(bots_db)
            set_validation_code(conn, name, code)
            conn.close()
        except Exception as db_err:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Controller] set_validation_code error for {name}: {db_err}")
        code_response_queue.put({"session": name, "code": code})

def get_all_admin_ids() -> list[int]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM admins")
    rows = cur.fetchall()
    conn.close()
    return [row[0] for row in rows]


# Рассылка запроса 2FA-кода администраторам с правом upload_zip
async def send_request_to_admin(session_name: str, phone: str):
    ok = await wait_for_controller_ready()
    if not ok:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Бот ещё не готов — не могу запросить код.")
        return

    msg = (f"🔐 Введите код подтверждения для номера `{phone}` "
           f"(сессия `{session_name}`):\n"
           f"Формат: `{session_name}: <код>`")

    for admin_id in get_all_admin_ids():
        if not has_permission(admin_id, "upload_zip"):
            continue
        try:
            await client.send_message(admin_id, msg)
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Ошибка отправки админу {admin_id}: {e}")

    CODE_REQUESTS[session_name] = None


# Очистка старых ZIP-архивов
def start_zip_cleanup_thread():
    ttl_hours = config.get("uploaded_zip_ttl_hours", 6)
    ttl_seconds = ttl_hours * 3600

    def cleaner():
        while True:
            try:
                now = time.time()
                for fn in os.listdir(TEMP_DIR):
                    if fn.endswith(".zip"):
                        fp = os.path.join(TEMP_DIR, fn)
                        if now - os.path.getmtime(fp) > ttl_seconds:
                            os.remove(fp)
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][Cleanup] 🗑 Удалён архив {fn}")
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [Cleanup] Ошибка: {e}")
            time.sleep(3600)

    threading.Thread(target=cleaner, daemon=True).start()


# ──────────────────────────────────────────────────────────────────────
# Запуск процесса-контроллера (вызывается из run.py)
# ──────────────────────────────────────────────────────────────────────
def run_controller_process(session_q: Queue,
                           code_req_q: Queue,
                           code_res_q: Queue,
                           cfg: dict):
    global session_queue, code_request_queue, code_response_queue
    global client, ADMIN_IDS, TEMP_DIR, DB_PATH

    set_session_queue(session_q)
    set_code_queues(code_req_q, code_res_q)
    code_request_queue  = code_req_q
    code_response_queue = code_res_q

    load_config(cfg)
    ADMIN_IDS = config.get("admin_ids", [])
    TEMP_DIR  = config.get("session_unpack_dir", "/tmp/telethon_sessions")
    DB_PATH   = config.get("session_log_path",  "session_log.db")

    # в БД пропишем стартовых админов с правами по умолчанию
    _ensure_admins_table()
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    for aid in ADMIN_IDS:
        cur.execute("INSERT OR IGNORE INTO admins (user_id, permissions) VALUES (?, ?)",
                    (aid, json.dumps(ADMIN_DEFAULT_PERMS)))
    conn.commit()
    conn.close()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def main():
        global client
        client = TelegramClient(SESSION_NAME,
                                config["api_id"],
                                config["api_hash"])
        await client.start(bot_token=config["admin_bot_token"])

        # команды
        client.add_event_handler(start_handler,      events.NewMessage(pattern="/start"))
        client.add_event_handler(add_admin_handler,  events.NewMessage(pattern="/add_admin"))
        client.add_event_handler(upload_mode_handler,events.NewMessage(pattern="/upload_mode"))
        client.add_event_handler(stats_handler,      events.NewMessage(pattern="/stats"))
        client.add_event_handler(listchats_handler,  events.NewMessage(pattern="/chats"))  # >>> add: /chats

        client.add_event_handler(noreact_handler,    events.NewMessage(pattern="/noreact"))
        client.add_event_handler(allowreact_handler, events.NewMessage(pattern="/allowreact"))
        client.add_event_handler(forcerxn_handler,   events.NewMessage(pattern="/forcerxn"))
        client.add_event_handler(lastposts_handler,  events.NewMessage(pattern="/lastposts"))
        client.add_event_handler(help_handler,       events.NewMessage(pattern="/help"))
        # >>> add: регистрация /post
        client.add_event_handler(post_handler,       events.NewMessage(pattern="/post"))

        client.add_event_handler(restart_handler,    events.NewMessage(pattern="/restart"))

        # текстовые коды / файлы
        client.add_event_handler(handle_code_response, events.NewMessage(incoming=True, func=lambda e: ":" in e.raw_text))
        client.add_event_handler(file_handler,         events.NewMessage(incoming=True, func=lambda e: bool(e.document)))

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][ControllerBot] 🚀 запущен")
        start_zip_cleanup_thread()

        # отдельный поток, слушающий очередь запросов кодов от валидатора
        def _process_code_requests():
            while True:
                req = code_request_queue.get()
                asyncio.run_coroutine_threadsafe(
                    send_request_to_admin(req["session"], req["phone"]),
                    loop
                )
        threading.Thread(target=_process_code_requests, daemon=True).start()

        await client.run_until_disconnected()

    loop.run_until_complete(main())


async def lastposts_handler(event):
    if not has_permission(event.sender_id, "edit_config") and not has_permission(event.sender_id, "view_stats"):
        return
    parts = event.raw_text.strip().split()
    if len(parts) < 2:
        await event.reply("Формат: `/lastposts <chat_id> [limit]`", parse_mode="markdown")
        return
    try:
        chat_id = int(parts[1])
        limit = int(parts[2]) if len(parts) >= 3 else 10
    except Exception:
        await event.reply("Формат: `/lastposts <chat_id> [limit]`", parse_mode="markdown")
        return
    pm = PostManager(None, config.get("posts_db_path", "posts.db"))
    rows = pm.list_recent_posts(chat_id, limit)
    if not rows:
        await event.reply(f"Посты для канала `{chat_id}` не найдены.", parse_mode="markdown")
        return
    lines = [f"🧾 Последние {len(rows)} постов для `{chat_id}`:"]
    for r in rows:
        mid = r.get("msg_id")
        flags = []
        if int(r.get("blocked", 0)):
            flags.append("🛑 NO-REACT")
        if r.get("forced_emoji"):
            flags.append(f"🎯 {r['forced_emoji']}")
        snippet = (r.get("text") or "").replace("\n", " ")[:120]
        flag_str = (" — " + " · ".join(flags)) if flags else ""
        lines.append(f"• ID: `{mid}`{flag_str}\n  {snippet}")
    _text = "\n".join(lines)
    try:
        await event.reply(_text, parse_mode="markdown")
    except EntityBoundsInvalidError:
        await event.reply(_text, parse_mode=None)


async def noreact_handler(event):
    if not has_permission(event.sender_id, "edit_config"):
        return
    parts = event.raw_text.strip().split()
    if len(parts) != 3:
        await event.reply("Формат: `/noreact <chat_id> <msg_id>`", parse_mode="markdown")
        return
    try:
        _, chat_id_s, msg_id_s = parts
        chat_id = int(chat_id_s); msg_id = int(msg_id_s)
    except Exception:
        await event.reply("Формат: `/noreact <chat_id> <msg_id>`", parse_mode="markdown")
        return
    pm = PostManager(None, config.get("posts_db_path", "posts.db"))
    pm.set_block(chat_id, msg_id, True)
    conn = jobs_connect(config.get("bots_db_path", "bots.db"))
    conn.execute("UPDATE jobs SET status='dead' WHERE type='react' AND chat_id=? AND msg_id=? AND status IN ('queued','reserved')", (chat_id, msg_id))
    await event.reply(f"🛑 Пост `{chat_id}/{msg_id}` помечен как NO-REACT. Очередь очищена.", parse_mode="markdown")


async def allowreact_handler(event):
    if not has_permission(event.sender_id, "edit_config"):
        return
    parts = event.raw_text.strip().split()
    if len(parts) != 3:
        await event.reply("Формат: `/allowreact <chat_id> <msg_id>`", parse_mode="markdown")
        return
    try:
        _, chat_id_s, msg_id_s = parts
        chat_id = int(chat_id_s); msg_id = int(msg_id_s)
    except Exception:
        await event.reply("Формат: `/allowreact <chat_id> <msg_id>`", parse_mode="markdown")
        return
    pm = PostManager(None, config.get("posts_db_path", "posts.db"))
    pm.set_block(chat_id, msg_id, False)
    await event.reply(f"✅ Пост `{chat_id}/{msg_id}` снова допускает реакции.", parse_mode="markdown")


async def forcerxn_handler(event):
    if not has_permission(event.sender_id, "edit_config"):
        return
    parts = event.raw_text.strip().split(maxsplit=3)
    if len(parts) < 4:
        await event.reply("Формат: `/forcerxn <chat_id> <msg_id> <emoji|clear>`", parse_mode="markdown")
        return
    _, chat_id_s, msg_id_s, emo = parts
    try:
        chat_id = int(chat_id_s); msg_id = int(msg_id_s)
    except Exception:
        await event.reply("chat_id и msg_id должны быть числами.", parse_mode="markdown")
        return
    emo = None if emo.lower() in ("clear", "none") else emo
    pm = PostManager(None, config.get("posts_db_path", "posts.db"))
    pm.set_forced_emoji(chat_id, msg_id, emo)
    conn = jobs_connect(config.get("bots_db_path", "bots.db"))
    conn.execute("UPDATE jobs SET status='dead' WHERE type='react' AND chat_id=? AND msg_id=? AND status IN ('queued','reserved')", (chat_id, msg_id))
    await event.reply(f"🎯 Подсказка реакции для `{chat_id}/{msg_id}`: `{emo or 'снята'}`. Очередь обновится.", parse_mode="markdown")


# >>> add: /chats
async def listchats_handler(event):
    # Доступ: просмотр списка каналов — как /stats
    if not has_permission(event.sender_id, "view_stats") and not has_permission(event.sender_id, "edit_config"):
        return
    ch_ids = config.get("channel_ids", []) or []
    links = config.get("channel_invite_links", {}) or {}
    targets = config.get("reaction_targets", {}) or {}
    if not ch_ids:
        await event.reply("Список каналов пуст (channel_ids не задан).")
        return
    lines = ["📋 Список настроенных каналов:"]
    for cid in ch_ids:
        cid_str = str(cid)
        tgt = targets.get(cid_str)
        if tgt is None and isinstance(targets, dict):
            try:
                tgt = targets.get(int(cid))
            except Exception:
                pass
        lnk = links.get(cid_str) if isinstance(links, dict) else None
        parts = [f"`{cid}`"]
        if tgt is not None:
            parts.append(f"цель: {tgt}")
        if lnk:
            parts.append(lnk)
        lines.append("• " + " — ".join(parts))
    _text = "\n".join(lines)
    try:
        await event.reply(_text, parse_mode="markdown")
    except EntityBoundsInvalidError:
        await event.reply(_text, parse_mode=None)


# >>> add: полный текст поста
async def post_handler(event):
    if not has_permission(event.sender_id, "view_stats") and not has_permission(event.sender_id, "edit_config"):
        return

    parts = event.raw_text.strip().split()
    if len(parts) != 3:
        await event.reply("Формат: /post <chat_id> <msg_id>")
        return
    try:
        chat_id = int(parts[1])
        msg_id  = int(parts[2])
    except Exception:
        await event.reply("Формат: /post <chat_id> <msg_id>")
        return

    posts_db = config.get("posts_db_path", "posts.db")
    conn = sqlite3.connect(posts_db)
    try:
        cur = conn.cursor()
        cur.execute("SELECT text, blocked, forced_emoji FROM posts WHERE chat_id=? AND msg_id=?", (chat_id, msg_id))
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        await event.reply(f"Пост {chat_id}/{msg_id} не найден")
        return

    text, blocked, forced = row[0] or "", int(row[1] or 0), row[2]
    flags = []
    if blocked: flags.append("🛑 NO-REACT")
    if forced:  flags.append(f"🎯 {forced}")
    flag_str = (" — " + " · ".join(flags)) if flags else ""
    # Без parse_mode — отдаем текст поста «как есть»
    await event.reply(f"Пост {chat_id}/{msg_id}{flag_str}\n\n{text}")


def _build_help(perms: dict) -> str:
    lines = ["🤖 *Доступные команды*"]
    lines.append("• /help — показать эту справку")
    if perms.get("view_stats"):  lines.append("• /stats — краткая статистика/состояние")
    if perms.get("view_stats"):  lines.append("• /chats — список настроенных chat_id")  # >>> add: /chats в help
    if perms.get("upload_zip"):  lines.append("• /upload_mode — включить режим приёма ZIP сессий")
    if perms.get("add_admins"):  lines.append("• /add_admin <id> <json_права> — добавить/обновить права")
    if perms.get("edit_config"):
        lines += [
            "• Отправьте `config.json` — обновить конфигурацию",
            "• /lastposts <chat_id> [limit] — показать последние посты и их ID",
            "• /noreact <chat_id> <msg_id> — запретить реакции на пост",
            "• /allowreact <chat_id> <msg_id> — разрешить реакции на пост",
            "• /forcerxn <chat_id> <msg_id> <emoji|clear> — задать/снять подсказку реакции",
            "• /post <chat_id> <msg_id> — показать полный текст поста",  # >>> add: help
        ]
    return "\n".join(lines)


async def help_handler(event):
    perms = get_admin_permissions(event.sender_id)
    if not perms:
        return
    _text = _build_help(perms)
    try:
        await event.respond(_text, parse_mode="markdown")
    except EntityBoundsInvalidError:
        await event.respond(_text, parse_mode=None)

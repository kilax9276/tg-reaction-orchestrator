# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# BotManager.py
#
# SQLite-backed registry of Telegram sessions ("bots") and their actions.
#
# Tables in bots.db:
#   - bots: session_name, phone, last_used, is_banned, is_frozen, revoked
#   - actions: per-bot action log (used to avoid double reactions)
#   - bot_chat_access: per-bot access status for each chat/channel
#
# WAL mode is enabled to support multiple processes without "database is locked"
# errors under moderate concurrency.
# -----------------------------------------------------------------------------

"""BotManager — переработанный для многопроцессной работы.

Главные изменения:
* SQLite открыт с `journal_mode=WAL` и `busy_timeout=30000` (30 с),
  что устраняет ошибки `database is locked` при параллельной работе
  нескольких процессов.
* `check_same_thread=False` — позволяет использовать одно соединение
  в разных `asyncio`‑трейдах внутри процесса.
* Добавлен метод `close()` (вызывать при завершении процесса).
"""

import os
import shutil
import sqlite3
from datetime import datetime
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from code_manager import wait_for_code

class BotManager:
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        db_path: str = "bots.db",
        sessions_dir: str = "sessions",
    ):
        self.api_id = api_id
        self.api_hash = api_hash
        self.db_path = db_path
        self.sessions_dir = sessions_dir
        os.makedirs(sessions_dir, exist_ok=True)

        # 🔑 Открываем SQLite соединение в WAL‑режиме + busy_timeout 30 с
        self.conn = sqlite3.connect(
            self.db_path,
            timeout=30,
            check_same_thread=False,
            isolation_level=None,  # autocommit‑режим
        )
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout = 30000")
        self.conn.row_factory = sqlite3.Row

        self._init_db()
        self._migrate_schema()

    # ------------------------------------------------------------------
    #  Схема БД
    # ------------------------------------------------------------------
    def _init_db(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bots (
                session_name TEXT PRIMARY KEY,
                phone        TEXT,
                last_used    TEXT,
                is_banned    INTEGER DEFAULT 0,
                is_frozen    INTEGER DEFAULT 0,
                revoked      INTEGER DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS actions (
                session_name  TEXT,
                action_type   TEXT,
                target_msg_id INTEGER,
                chat_id       INTEGER,
                timestamp     TEXT,
                details       TEXT
            )
            """
        )


        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_chat_access (
                session_name TEXT NOT NULL,
                chat_id      INTEGER NOT NULL,
                status       TEXT NOT NULL,
                last_error   TEXT,
                updated_at   TEXT NOT NULL,
                PRIMARY KEY (session_name, chat_id)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bot_chat_access_chat ON bot_chat_access(chat_id)")
    def _migrate_schema(self):
        cur = self.conn.cursor()

        def _alter(table: str, col_sql: str):
            # Простой способ: пытаемся выполнить и игнорируем только "duplicate column name"
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_sql}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e).lower():
                    return
                raise

        # ---- bots ----
        # Нам не важно, успел ли кто-то другой: если колонка уже есть — поймаем и проигнорируем
        try:
            cur.execute("PRAGMA table_info(bots)")
            cols = [r[1] for r in cur.fetchall()]
        except Exception:
            cols = []

        if "last_used" not in cols:
            _alter("bots", "last_used TEXT")
        if "is_banned" not in cols:
            _alter("bots", "is_banned INTEGER DEFAULT 0")
        if "is_frozen" not in cols:
            _alter("bots", "is_frozen INTEGER DEFAULT 0")
        if "revoked" not in cols:
            _alter("bots", "revoked INTEGER DEFAULT 0")

        # ---- actions ----
        try:
            cur.execute("PRAGMA table_info(actions)")
            cols = [r[1] for r in cur.fetchall()]
        except Exception:
            cols = []

        if "details" not in cols:
            _alter("actions", "details TEXT")


    
    # ------------------------------------------------------------------
    #  Доступ ботов к чатам (session_name → chat_id)
    # ------------------------------------------------------------------
    def mark_chat_access(self, session_name: str, chat_id: int, status: str, last_error: str | None = None) -> None:
        """Записывает/обновляет статус доступа конкретной сессии к конкретному чату.

        status: ok | no_access | invite_invalid | kicked | left
        """
        ts = datetime.utcnow().isoformat()
        self.conn.execute(
            """
            INSERT INTO bot_chat_access(session_name, chat_id, status, last_error, updated_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(session_name, chat_id) DO UPDATE SET
                status=excluded.status,
                last_error=excluded.last_error,
                updated_at=excluded.updated_at
            """,
            (session_name, int(chat_id), str(status), last_error, ts),
        )

    def can_access_chat(self, session_name: str, chat_id: int) -> bool:
        """True если бот можно использовать для этого chat_id.

        По умолчанию (если записи нет) возвращает True.
        """
        row = self.conn.execute(
            "SELECT status FROM bot_chat_access WHERE session_name=? AND chat_id=?",
            (session_name, int(chat_id)),
        ).fetchone()
        if not row:
            return True
        try:
            st = row["status"]
        except Exception:
            st = row[0]
        return st == 'ok'

    def eligible_bots_for_post(self, chat_id: int, msg_id: int):
        rows = self.conn.execute(
            """
            SELECT b.session_name
            FROM bots b
            WHERE COALESCE(b.is_banned,0)=0
            AND COALESCE(b.is_frozen,0)=0
            AND COALESCE(b.revoked,0)=0
            AND NOT EXISTS (
                    SELECT 1 FROM actions a
                    WHERE a.session_name = b.session_name
                    AND a.chat_id = ?
                    AND a.target_msg_id = ?
                    AND a.action_type = 'reaction'
            )
            
            AND NOT EXISTS (
                    SELECT 1 FROM bot_chat_access x
                    WHERE x.session_name = b.session_name
                    AND x.chat_id = ?
                    AND x.status IN ('no_access','invite_invalid','kicked','left')
            )
            """,
            (chat_id, msg_id, chat_id),
        ).fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    #  CRUD‑операции с ботами
    # ------------------------------------------------------------------
    def add_bot(self, session_name: str, phone: str, source_path: str | None = None):
        # Не сбрасываем revoked при повторной регистрации
        self.conn.execute(
            "INSERT OR IGNORE INTO bots (session_name, phone, last_used, is_banned, is_frozen, revoked) "
            "VALUES (?, ?, NULL, 0, 0, 0)",
            (session_name, phone),
        )
        self.conn.execute(
            "UPDATE bots SET phone=? WHERE session_name=?",
            (phone, session_name),
        )
        # # перенос session‑файла
        # if source_path:
        #     target_path = os.path.join(self.sessions_dir, f"{session_name}.session")
        #     if os.path.exists(target_path):
        #         os.remove(target_path)
        #     if os.path.exists(source_path):
        #         shutil.move(source_path, target_path)

    def mark_banned(self, session_name: str):
        self.conn.execute(
            "UPDATE bots SET is_banned = 1 WHERE session_name = ?",
            (session_name,),
        )

    def mark_frozen(self, session_name: str, frozen: int = 1):
        self.conn.execute(
            "UPDATE bots SET is_frozen = ? WHERE session_name = ?",
            (frozen, session_name),
        )

    def unfreeze(self, session_name: str):
        self.mark_frozen(session_name, 0)

    def mark_revoked(self, session_name: str):
        self.conn.execute(
            "UPDATE bots SET revoked=1 WHERE session_name=?",
            (session_name,),
        )
#        self.conn.commit()

    def clear_revoked(self, session_name: str):
        self.conn.execute(
            "UPDATE bots SET revoked=0 WHERE session_name=?",
            (session_name,),
        )
#        self.conn.commit()

    def update_last_used(self, session_name: str, timestamp: str | None = None):
        ts = timestamp or datetime.utcnow().isoformat()
        self.conn.execute(
            "UPDATE bots SET last_used = ? WHERE session_name = ?",
            (ts, session_name),
        )

    # ------------------------------------------------------------------
    #  Чтение
    # ------------------------------------------------------------------
    def list_bots(self):
        return self.conn.execute(
            "SELECT session_name, phone, last_used, is_banned, is_frozen, revoked FROM bots"
        ).fetchall()

    def get_active_bots(self):
        return self.conn.execute(
            "SELECT session_name, last_used FROM bots "
            "WHERE COALESCE(is_banned,0)=0 AND COALESCE(is_frozen,0)=0 AND COALESCE(revoked,0)=0"
        ).fetchall()

    # совместимость
    def list_active_bots(self):
        return self.get_active_bots()
    # ------------------------------------------------------------------
    #  Логи действий
    # ------------------------------------------------------------------
    def log_action(
        self,
        session_name: str,
        action_type: str,
        target_msg_id: int,
        chat_id: int,
        details: str | None = None,
    ):
        self.conn.execute(
            """
            INSERT INTO actions (session_name, action_type, target_msg_id, chat_id, timestamp, details)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                session_name,
                action_type,
                target_msg_id,
                chat_id,
                datetime.utcnow().isoformat(),
                details,
            ),
        )

    def count_reactions_for_post(self, chat_id: int, msg_id: int) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS c FROM actions
            WHERE chat_id = ? AND target_msg_id = ? AND action_type = 'reaction'
            """,
            (chat_id, msg_id),
        ).fetchone()
        return row["c"] if row else 0

    def has_bot_reacted(self, session_name: str, chat_id: int, msg_id: int) -> bool:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS c FROM actions
            WHERE session_name = ? AND chat_id = ? AND target_msg_id = ? AND action_type = 'reaction'
            """,
            (session_name, chat_id, msg_id),
        ).fetchone()
        return (row["c"] or 0) > 0

    # ------------------------------------------------------------------
    #  Авторизация
    # ------------------------------------------------------------------
    async def get_client(
        self,
        session_name: str,
        ensure_auth: bool = False,
        proxy=None,
        api_id: int | None = None,
        api_hash: str | None = None,
    ):
        """Возвращает подключённый TelegramClient."""
        session_path = os.path.join(self.sessions_dir, session_name)
        client = TelegramClient(
            session_path,
            api_id or self.api_id,
            api_hash or self.api_hash,
            proxy=proxy,
        )
        await client.connect()

        # если уже авторизован — возвращаем
        if await client.is_user_authorized():
            return client
        # если не нужно пытаться авторизоваться — кидаем ошибку
        if not ensure_auth:
            raise RuntimeError(f"Bot {session_name} is not authorized")

        # пытаемся получить SMS-код и залогиниться
        phone = session_name.split("_")[0]
        try:
            print(f"🔐 Переавторизация {session_name}…")
            code = await wait_for_code(session_name, phone)
            await client.sign_in(phone=phone, code=code)
            if await client.is_user_authorized():
                print(f"✅ {session_name} авторизован.")
                self.update_last_used(session_name)
                self.add_bot(session_name, phone)
                return client
            print(f"❌ Авторизация {session_name} не удалась.")
        except PhoneCodeInvalidError:
            print(f"❌ Неверный код для {session_name}")
        except SessionPasswordNeededError:
            print(f"🔐 2FA включена для {session_name} — не поддерживается.")
        except Exception as e:
            print(f"❌ Ошибка авторизации {session_name}: {e}")

        # в случае неудачи баним сессию и закрываем
        self.mark_banned(session_name)
        await client.disconnect()
        return None

    # ------------------------------------------------------------------
    #  Завершение работы
    # ------------------------------------------------------------------
    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

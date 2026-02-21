# -----------------------------------------------------------------------------
# session_store.py
# Copyright Kolobov Aleksei @kilax9276
#
# IPC session registry and locks built on SQLite (WAL) + aiosqlite.
#
# Provides:
#   - a FIFO-ish queue of available session names
#   - inter-process locks to ensure a Telegram session is used by at most one
#     worker at a time
#   - a "cooldown" window (MIN_REUSE_DELAY) after release before a session is
#     considered ready again
#
# Data model:
#   - queue(name): names available for dequeue()
#   - session_lock(name, in_use, released_at): lock state and last release timestamp
# -----------------------------------------------------------------------------

# session_store.py
# -*- coding: utf-8 -*-
"""
IPC-хранилище: глобальная очередь сессий и блокировки, основанные на SQLite (WAL).
Подходит для нескольких процессов и для асинхронных корутин в каждом процессе.

Зависимость:  pip install aiosqlite
"""

from __future__ import annotations

import os
import time
import asyncio
import sqlite3
from typing import Optional, List
from datetime import datetime

import aiosqlite

# ---------- настройки -------------------------------------------------------
DB_PATH = os.getenv("SESSIONS_DB", "sessions_state.db")
MIN_REUSE_DELAY = 300          # сек; «отдых» перед повторным использованием

_SQL_SCHEMA = """
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS queue (
    name TEXT PRIMARY KEY          -- имена .session-файлов
);
CREATE TABLE IF NOT EXISTS session_lock (
    name        TEXT PRIMARY KEY,
    in_use      INTEGER NOT NULL DEFAULT 0,   -- 1 = занята
    released_at INTEGER                       -- unix-time последнего release
);
"""


# ---------- соединение с базой (busy_timeout 30 с) --------------------------
async def _open_db(path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(path, timeout=30)
    await db.execute("PRAGMA busy_timeout = 30000")
    return db

# def reset_all_locks_sync(db_path=None):
    # db_path = db_path or os.getenv("SESSIONS_DB", "sessions_state.db")
    # db_path = os.path.abspath(db_path)

    # try:
        # conn = sqlite3.connect(db_path, timeout=0)  # без ожидания
        # conn.execute("PRAGMA busy_timeout = 0")      # вообще не ждём
        # conn.execute("UPDATE session_lock SET in_use = 0")
        # conn.commit()
        # conn.close()
        # print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🧹 Сброшены все in_use в {db_path}")
    # except sqlite3.OperationalError as e:
        # print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Не удалось сбросить локи: {e}")

import os

def hard_reset_session_store():
    for suffix in ("", "-shm", "-wal"):
        path = f"sessions_state.db{suffix}" if suffix else "sessions_state.db"
        if os.path.exists(path):
            try:
                os.remove(path)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🧹 Удалён {path}")
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠ Не удалось удалить {path}: {e}")



class SessionStore:
    """Глобальный реестр: очередь сессий + межпроцессный lock."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_lock = asyncio.Lock()

    # -- инициализация ----------------------------------------------------
    async def _ensure_schema(self) -> None:
        async with self._init_lock:        # один раз на процесс
            db = await _open_db(self.db_path)
            try:
                await db.executescript(_SQL_SCHEMA)
                await db.commit()
            finally:
                await db.close()


    async def reset_all_locks(self) -> None:
        """Снять все in_use=1 при старте программы."""
        await self._ensure_schema()
        db = await _open_db(self.db_path)
        try:
            await db.execute("UPDATE session_lock SET in_use = 0")
            await db.commit()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🧹 Все session_lock.in_use сброшены")
        finally:
            await db.close()
            
    # ---------- очередь --------------------------------------------------
    async def enqueue(self, name: str) -> None:
        """Кладёт имя в очередь, если его там ещё нет."""
        await self._ensure_schema()
        db = await _open_db(self.db_path)
        try:
            await db.execute(
                "INSERT OR IGNORE INTO queue(name) VALUES (?)",
                (name,),
            )
            await db.commit()
        finally:
            await db.close()

    async def dequeue(self) -> Optional[str]:
        """Атомарно берёт верхний элемент.  Вернёт None, если очередь пуста."""
        await self._ensure_schema()
        db = await _open_db(self.db_path)
        try:
            await db.execute("BEGIN IMMEDIATE")        # эксклюзивно
            cur = await db.execute("SELECT name FROM queue LIMIT 1")
            row = await cur.fetchone()
            if not row:
                await db.execute("COMMIT")
                return None

            (name,) = row
            await db.execute("DELETE FROM queue WHERE name = ?", (name,))
            await db.commit()
            return name
        finally:
            await db.close()

    # ---------- lock -----------------------------------------------------
    async def acquire(self, name: str) -> bool:
        """
        Попытка захватить сессию.

        Возвращает True, если успех.
        False – если сессия уже занята или ещё «отдыхает».
        """
        await self._ensure_schema()
        now = int(time.time())
        db = await _open_db(self.db_path)
        try:
            await db.execute("BEGIN IMMEDIATE")
            cur = await db.execute(
                "SELECT in_use, released_at FROM session_lock WHERE name = ?",
                (name,),
            )
            row = await cur.fetchone()

            if row is None:
                # впервые видим эту сессию → ставим lock
                await db.execute(
                    "INSERT INTO session_lock(name, in_use) VALUES (?, 1)",
                    (name,),
                )
                await db.commit()
                return True

            in_use, released_at = row
            if in_use:                       # уже занята
                await db.execute("ROLLBACK")
                return False
            if released_at and now - released_at < MIN_REUSE_DELAY:
                await db.execute("ROLLBACK")  # ещё отдыхает
                return False

            await db.execute(
                "UPDATE session_lock SET in_use = 1, released_at = NULL "
                "WHERE name = ?",
                (name,),
            )
            await db.commit()
            return True
        finally:
            await db.close()

    async def release(self, name: str) -> None:
        """Освободить сессию и записать время последнего использования."""
        await self._ensure_schema()
        now = int(time.time())
        db = await _open_db(self.db_path)
        try:
            await db.execute(
                "UPDATE session_lock SET in_use = 0, released_at = ? WHERE name = ?",
                (now, name),
            )
            await db.commit()
        finally:
            await db.close()

    # ---------- вернуть «отдохнувшие» в очередь --------------------------
    async def refill_ready(self, batch: int = 50) -> None:
        """
        Перемещает до `batch` сессий, у которых вышел MIN_REUSE_DELAY,
        обратно в очередь.
        """
        await self._ensure_schema()
        now = int(time.time())
        db = await _open_db(self.db_path)
        try:
            cur = await db.execute(
                """
                SELECT name FROM session_lock
                WHERE in_use = 0
                  AND released_at IS NOT NULL
                  AND released_at <= ?
                LIMIT ?
                """,
                (now - MIN_REUSE_DELAY, batch),
            )
            rows: List[tuple] = await cur.fetchall()
            if rows:
                await db.executemany(
                    "INSERT OR IGNORE INTO queue(name) VALUES (?)",
                    [(r[0],) for r in rows],
                )
                await db.commit()
        finally:
            await db.close()


# --- глобальный экземпляр -----------------------------------------------
    async def remove_from_queue(self, name: str) -> None:
        await self._ensure_schema()
        db = await _open_db(self.db_path)
        try:
            await db.execute("DELETE FROM queue WHERE name = ?", (name,))
            await db.commit()
        finally:
            await db.close()

    async def remove_many_from_queue(self, names: List[str]) -> None:
        if not names:
            return
        await self._ensure_schema()
        db = await _open_db(self.db_path)
        try:
            await db.executemany("DELETE FROM queue WHERE name = ?", [(n,) for n in names])
            await db.commit()
        finally:
            await db.close()


    async def ensure_present(self, names: List[str], *, mark_ready: bool = False) -> None:
        """Гарантирует наличие записей в session_lock для перечисленных имён.
        Если mark_ready=True — ставит released_at=0, чтобы сессии сразу считались «отдохнувшими»
        и могли попасть в очередь через refill_ready().
        """
        if not names:
            return
        await self._ensure_schema()
        db = await _open_db(self.db_path)
        try:
            rel = 0 if mark_ready else None
            await db.executemany(
                "INSERT OR IGNORE INTO session_lock(name, in_use, released_at) VALUES (?, 0, ?)",
                [(n, rel) for n in names]
            )
            await db.commit()
        finally:
            await db.close()


store = SessionStore()

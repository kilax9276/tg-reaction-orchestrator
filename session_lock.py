# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# session_lock.py
# Copyright Kolobov Aleksei @kilax9276
#
# Compatibility wrapper around the new IPC session store (session_store.py).
#
# Older code expected acquire_session()/release_session() functions.
# We keep that API but delegate to `session_store.store` when available.
#
# Fallback mode:
#   If session_store is not importable (e.g., in unit tests), a per-process
#   in-memory set is used. Note: this does NOT protect across processes.
# -----------------------------------------------------------------------------

# session_lock.py
"""
Совместимая обёртка: оставляет старые функции acquire_session / release_session,
но, если доступен модуль session_store, использует *межпроцессный* реестр.
Если session_store не найден (например, при юнит-тестах), падает обратно
на прежний in-memory set (работает лишь внутри одного процесса).
"""

import asyncio

try:
    # основной путь: глобальный IPC-реестр (SQLite + WAL)
    from session_store import store        # type: ignore
except ImportError:                        # fallback на локальный set
    store = None

# --------------------------------------------------------------------------- #
#  Резервный механизм для single-process
# --------------------------------------------------------------------------- #
_fallback_lock   = asyncio.Lock()
_fallback_active = set()

async def _fallback_acquire(name: str) -> bool:
    async with _fallback_lock:
        if name in _fallback_active:
            return False
        _fallback_active.add(name)
        return True

async def _fallback_release(name: str) -> None:
    async with _fallback_lock:
        _fallback_active.discard(name)


# --------------------------------------------------------------------------- #
#  Публичный API — остаётся прежним
# --------------------------------------------------------------------------- #
async def acquire_session(session_name: str) -> bool:
    """
    Захватывает сессию.

    • Если подключён session_store → true IPC-lock между процессами
      (учтёт MIN_REUSE_DELAY).
    • Иначе — старый in-memory механизм (только внутри текущего процесса).
    """
    if store is not None:
        return await store.acquire(session_name)
    return await _fallback_acquire(session_name)


async def release_session(session_name: str) -> None:
    """
    Освобождает сессию и фиксирует время последнего использования
    (в режиме session_store).
    """
    if store is not None:
        await store.release(session_name)
    else:
        await _fallback_release(session_name)

# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# code_manager.py
#
# Async helper for requesting and receiving SMS login codes through the controller bot.
#
# The validator/worker puts a request into code_request_queue:
#   {"session": "<session_name>", "phone": "<phone>"}
#
# The controller bot asks admins to reply with:
#   "<session_name>: <code>"  or  "<session_name>: esc"
#
# The reply is put into code_response_queue and wait_for_code() returns it.
#
# wait_for_code() uses run_in_executor() so it can safely block on a multiprocessing.Queue
# without blocking the asyncio event loop.
# -----------------------------------------------------------------------------

# code_manager.py
"""
Единая точка общения с controller-bot’ом для получения SMS-кодов.

* set_code_queues() вызывается из run.py (или другого bootstrap-кода)
  и передаёт multiprocessing.Queue-объекты.
* wait_for_code() формирует запрос, блокирующе ждёт ответ в пуле потоков
  и возвращает строку-код.  Если время (timeout) истекло, бросает TimeoutError.
"""

from __future__ import annotations

import asyncio
import time
import queue as py_queue          # для Empty
from multiprocessing import Queue
from typing import Optional, Any, Dict

# Эти объекты инициализируются в set_code_queues()
code_request_queue:  Optional[Queue] = None
code_response_queue: Optional[Queue] = None


# --------------------------------------------------------------------------- #
#  Инициализация очередей
# --------------------------------------------------------------------------- #
def set_code_queues(request_q: Queue, response_q: Queue) -> None:
    """
    Передаёт менеджеру две multiprocessing.Queue:
    * request_q  – куда кладём {"session": ..., "phone": ...}
    * response_q – откуда читаем {"session": ..., "code": ...}

    Вызывать при запуске процесса (см. run_validator_process).
    """
    global code_request_queue, code_response_queue
    code_request_queue  = request_q
    code_response_queue = response_q


# --------------------------------------------------------------------------- #
#  Запрос и ожидание SMS-кода
# --------------------------------------------------------------------------- #
async def wait_for_code(session_name: str, phone: str, *, timeout: int = 300) -> str:
    """
    Кладёт запрос в code_request_queue и ждёт ответ в code_response_queue.

    Parameters
    ----------
    session_name : str
        Имя .session-файла, чтобы отличать ответы разных ботов.
    phone : str
        Телефон, показывается оператору в UI.
    timeout : int, optional
        Максимальное время ожидания (сек). По умолчанию 300.

    Returns
    -------
    str
        5- или 6-значный код авторизации.

    Raises
    ------
    TimeoutError
        Если код не пришёл за `timeout` секунд
    RuntimeError
        Если очереди не проинициализированы через set_code_queues()
    """
    if code_request_queue is None or code_response_queue is None:
        raise RuntimeError("Code queues are not initialised. "
                           "Call set_code_queues(request_q, response_q) first.")

    # 1) --- публикуем запрос оператору ---------------------------------
    code_request_queue.put({"session": session_name, "phone": phone})

    # 2) --- блокирующую часть выполняем в пуле потоков -----------------
    def _blocking_wait() -> str:
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise TimeoutError(f"Не дождался кода для {session_name}")

            try:
                resp: Dict[str, Any] = code_response_queue.get(timeout=remaining)
            except py_queue.Empty:
                continue

            if resp.get("session") == session_name:
                if resp.get("cancel"):
                    raise RuntimeError(f"Validation cancelled for {session_name}")
                return resp.get("code")

    # 3) Запускаем блокирующее ожидание в пуле потоков
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _blocking_wait)

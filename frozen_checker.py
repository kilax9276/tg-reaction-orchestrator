# -----------------------------------------------------------------------------
# frozen_checker.py
#
# Heuristic "account frozen / write restricted" detection without reading messages.
#
# It performs two lightweight write-like operations to 'Saved Messages':
#   1) SetTyping (typing indicator)
#   2) SaveDraft + ClearAllDrafts
#
# Based on the RPC results, it infers:
#   - not_frozen
#   - write_restricted_global (treat as frozen)
#   - temporarily_rate_limited (FloodWait)
#   - session revoked / account deactivated, etc.
# -----------------------------------------------------------------------------

# frozen_checker.py
import asyncio
from typing import Literal, TypedDict, Optional
from telethon import functions, types, errors
from telethon.tl.functions.messages import SaveDraftRequest, ClearAllDraftsRequest

class FrozenCheckResult(TypedDict, total=False):
    authorized: Optional[bool]
    typing_to_self_ok: Optional[bool]
    save_draft_ok: Optional[bool]
    flood_wait_sec: Optional[int]
    inference: Literal[
        "not_frozen",
        "write_restricted_global",
        "temporarily_rate_limited",
        "not_authorized_session",
        "session_invalid_or_revoked",
        "account_deactivated_or_banned",
        "unknown",
        "unexpected_error",
    ]
    exception: Optional[str]


async def check_frozen_without_messages(client) -> FrozenCheckResult:
    """
    Проверка «заморозки» БЕЗ чтения сообщений (через SetTyping и Draft).
    Использовать на *подключённом* клиенте.
    """
    result: FrozenCheckResult = {
        "authorized": None,
        "typing_to_self_ok": None,
        "save_draft_ok": None,
        "flood_wait_sec": None,
        "inference": None,
        "exception": None,
    }

    try:
        if not await client.is_user_authorized():
            result["authorized"] = False
            result["inference"] = "not_authorized_session"
            return result
        result["authorized"] = True

        # 1) SetTyping в "Избранное"
        try:
            await client(functions.messages.SetTypingRequest(
                peer='me',
                action=types.SendMessageTypingAction()
            ))
            result["typing_to_self_ok"] = True
        except errors.FloodWaitError as e:
            result["typing_to_self_ok"] = False
            result["flood_wait_sec"] = getattr(e, "seconds", None)
        except errors.ChatWriteForbiddenError:
            result["typing_to_self_ok"] = False
        except errors.RPCError as e:
            result["typing_to_self_ok"] = False
            result["exception"] = f"SetTyping: {type(e).__name__}"

        # 2) Черновики
        try:
            await client(SaveDraftRequest(
                peer='me',
                message="(check limits, ignore)",
                no_webpage=True
            ))
            await client(ClearAllDraftsRequest())
            result["save_draft_ok"] = True
        except errors.FloodWaitError as e:
            result["save_draft_ok"] = False
            result["flood_wait_sec"] = getattr(e, "seconds", None)
        except errors.ChatWriteForbiddenError:
            result["save_draft_ok"] = False
        except errors.RPCError as e:
            result["save_draft_ok"] = False
            prev = result.get("exception")
            result["exception"] = (prev + " | " if prev else "") + f"SaveDraft: {type(e).__name__}"

        # 3) Эвристика
        if result["typing_to_self_ok"] and result["save_draft_ok"]:
            result["inference"] = "not_frozen"
        elif result["typing_to_self_ok"] is False and result["save_draft_ok"] is False:
            result["inference"] = "write_restricted_global"  # считаем «заморожен»
        elif result["flood_wait_sec"]:
            result["inference"] = "temporarily_rate_limited"
        else:
            result["inference"] = "unknown"

        return result

    except (errors.UserDeactivatedBanError, errors.UserDeactivatedError):
        result["inference"] = "account_deactivated_or_banned"
        return result
    except (errors.AuthKeyUnregisteredError, errors.SessionRevokedError) as e:
        result["inference"] = "session_invalid_or_revoked"
        result["exception"] = type(e).__name__
        return result
    except Exception as e:
        result["inference"] = "unexpected_error"
        result["exception"] = repr(e)
        return result

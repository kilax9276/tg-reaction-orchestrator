# -----------------------------------------------------------------------------
# sleep_window.py
# Copyright Kolobov Aleksei @kilax9276
#
# Utility for "quiet hours" / sleep mode.
#
# Config structure:
#   "sleep": {
#       "enabled": true,
#       "timezone": "Europe/Moscow",
#       "start": "01:00",
#       "end": "08:30"
#   }
#
# is_sleep_time() answers whether the current local time is inside [start, end).
# seconds_until_wake() returns the number of seconds to wait until the next boundary
# (end if currently sleeping, otherwise start).
# -----------------------------------------------------------------------------

# sleep_window.py (таймзона из конфига)
from __future__ import annotations
from datetime import datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo  # stdlib 3.9+

def _parse_hhmm(s: str) -> time:
    hh, mm = s.strip().split(":")
    return time(int(hh), int(mm))

def _now_local(tz: str) -> datetime:
    return datetime.now(ZoneInfo(tz))

def is_sleep_time(cfg: dict, *, now_utc: Optional[datetime] = None) -> bool:
    sleep = (cfg or {}).get("sleep", {})
    if not sleep or not sleep.get("enabled", False):
        return False
    tz = sleep.get("timezone", "UTC")
    start = _parse_hhmm(sleep.get("start", "00:00"))
    end   = _parse_hhmm(sleep.get("end",   "00:00"))
    now_local = (_now_local(tz) if now_utc is None else now_utc.astimezone(ZoneInfo(tz)))
    cur = now_local.time()
    if start == end:
        return False  # сна нет
    if start < end:
        return start <= cur < end
    else:
        return cur >= start or cur < end  # через полночь

def seconds_until_wake(cfg: dict, *, now_utc: Optional[datetime] = None) -> int:
    sleep = (cfg or {}).get("sleep", {})
    tz = sleep.get("timezone", "UTC")
    start = _parse_hhmm(sleep.get("start", "00:00"))
    end   = _parse_hhmm(sleep.get("end",   "00:00"))
    now_local = (_now_local(tz) if now_utc is None else now_utc.astimezone(ZoneInfo(tz)))
    if not is_sleep_time(cfg, now_utc=now_utc):
        target = now_local.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
        if target <= now_local:
            target += timedelta(days=1)
        return int((target - now_local).total_seconds())
    target = now_local.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
    if start < end:
        if target <= now_local:
            target += timedelta(days=1)
    else:
        if now_local.time() >= start:
            target += timedelta(days=1)
    return max(1, int((target - now_local).total_seconds()))

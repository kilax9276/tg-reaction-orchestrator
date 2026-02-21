# -----------------------------------------------------------------------------
# job_store.py
# Copyright Kolobov Aleksei @kilax9276
#
# A tiny durable job queue stored in SQLite (bots.db by default).
#
# Why SQLite:
#   - works in multi-process mode with WAL journaling
#   - easy to inspect/debug
#   - provides atomic reservation (UPDATE ... RETURNING)
#
# Job lifecycle:
#   queued -> reserved -> done
#                 \-> queued (retry) -> ... -> dead (max attempts)
#
# Job types:
#   - collect_posts: refresh posts cache for a channel (chat_id)
#   - react: put one reaction emoji on a post (chat_id, msg_id, emoji)
#   - validate_session: login/validate a specific session_name
#
# Important constraints:
#   - unique indexes prevent duplicate jobs (same post/emoji etc.) being queued
#   - not_before enforces cooldown/backoff or scheduled execution
# -----------------------------------------------------------------------------

# job_store.py — единая очередь задач для воркеров (collect_posts, react, validate_session)
import sqlite3, json
from datetime import datetime, timedelta
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  type         TEXT    NOT NULL,           -- 'collect_posts' | 'react' | 'validate_session'
  chat_id      INTEGER NOT NULL,
  msg_id       INTEGER,                    -- NULL для collect_posts/validate
  emoji        TEXT,                       -- NULL для collect_posts/validate
  priority     REAL    NOT NULL,           -- меньше = выше приоритет
  status       TEXT    NOT NULL DEFAULT 'queued',  -- queued|reserved|done|dead
  reserved_by  TEXT,
  reserved_at  TEXT,
  attempts     INTEGER NOT NULL DEFAULT 0,
  not_before   TEXT,                       -- если задано, не выдавать до этой метки
  created_at   TEXT    NOT NULL,
  payload      TEXT,
  session_name TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_status_prio ON jobs(status, priority, not_before, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_post        ON jobs(type, chat_id, msg_id);
-- уникальности для защиты от гонок/дублей
CREATE UNIQUE INDEX IF NOT EXISTS uq_collect_on_queue ON jobs(chat_id)
  WHERE type='collect_posts' AND status IN ('queued','reserved');
CREATE UNIQUE INDEX IF NOT EXISTS uq_react_on_queue ON jobs(chat_id, msg_id, emoji)
  WHERE type='react' AND status IN ('queued','reserved');
CREATE UNIQUE INDEX IF NOT EXISTS uq_validate_on_queue ON jobs(session_name)
  WHERE type='validate_session' AND status IN ('queued','reserved');
"""

FETCH_LOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS posts_fetch_log (
  chat_id     INTEGER PRIMARY KEY,
  last_fetch  TEXT
);
"""

def _utcnow() -> str:
    return datetime.utcnow().isoformat()

def connect(db_path: str = "bots.db") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.executescript(FETCH_LOG_SCHEMA)
    return conn

# ---------- планирование (офлайн) ----------

def ensure_collect_jobs(conn: sqlite3.Connection, channel_ids, refresh_min_sec: int):
    """Создаёт collect_posts-задачи для каналов, у которых устарел кеш."""
    cur = conn.cursor()
    now = datetime.utcnow()
    for ch in channel_ids:
        row = cur.execute("SELECT last_fetch FROM posts_fetch_log WHERE chat_id=?", (ch,)).fetchone()
        due = True
        if row and row["last_fetch"]:
            try:
                last = datetime.fromisoformat(row["last_fetch"])
                due = (now - last).total_seconds() >= refresh_min_sec
            except Exception:
                due = True
        if not due:
            continue
        # уже есть queued/reserved?
        row2 = cur.execute(
            "SELECT 1 FROM jobs WHERE type='collect_posts' AND chat_id=? AND status IN ('queued','reserved') LIMIT 1",
            (ch,)
        ).fetchone()
        if row2:
            continue
        cur.execute(
            "INSERT INTO jobs (type, chat_id, priority, created_at) VALUES (?,?,?,?)",
            ("collect_posts", ch, 0.0, _utcnow())
        )

def set_fetched_now(conn: sqlite3.Connection, chat_id: int):
    conn.execute("""
        INSERT INTO posts_fetch_log(chat_id, last_fetch)
        VALUES (?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET last_fetch=excluded.last_fetch
    """, (chat_id, _utcnow()))

def rebuild_reaction_plan(conn: sqlite3.Connection, pm, bot_manager, config: dict):
    """
    Идемпотентно формирует react-задачи (queued) под актуальные цели.
    Удаляет старые queued для поста перед вставкой (чтобы не скапливались).
    Учитывает cooldown между реакциями на один и тот же пост через not_before.
    """
    import random, json as _json
    k = config.get("hyperbola_k", 1.0)
    c = config.get("hyperbola_c", 1.0)
    d = config.get("hyperbola_d", 1.0)
    msg_limit = config.get("message_limit", 10)
    targets = config.get("reaction_targets", {})
    dev = config.get("target_deviation", 0)
    cooldown = int(config.get("react_cooldown_sec", 0))

    cur = pm.conn.cursor()
    for ch in config.get("channel_ids", []):
        base = targets.get(str(ch), 0)
        if base <= 0:
            continue
        rows = cur.execute(
            "SELECT msg_id, all_reactions, blocked, forced_emoji, text FROM posts WHERE chat_id=? ORDER BY msg_id DESC LIMIT ?",
            (ch, msg_limit)
        ).fetchall()
        for idx, row in enumerate(rows):
            try:
                mid = row['msg_id']; all_reactions = row['all_reactions']; blocked = int(row.get('blocked', 0)); forced = row.get('forced_emoji'); text = row.get('text', '')
            except Exception:
                mid = row[0]; all_reactions = row[1]; blocked = int(row[2] if len(row)>2 and row[2] else 0); forced = (row[3] if len(row)>3 else None); text = (row[4] if len(row)>4 else '')

            # подчистить старые queued по этому посту
            conn.execute("DELETE FROM jobs WHERE type='react' AND status='queued' AND chat_id=? AND msg_id=?", (ch, mid))

            p = (k / (idx + c)) ** d
            if p > 1.0: p = 1.0
            if random.random() > p:
                continue

            # если пост оператором помечен как NO-REACT — пропускаем
            if blocked:
                conn.execute("DELETE FROM jobs WHERE type='react' AND chat_id=? AND msg_id=?", (ch, mid))
                continue

            done  = bot_manager.count_reactions_for_post(ch, mid)
            queued= conn.execute(
                "SELECT COUNT(*) AS c FROM jobs WHERE type='react' AND chat_id=? AND msg_id=? AND status IN ('queued','reserved')",
                (ch, mid)
            ).fetchone()["c"]

            target = pm.get_or_set_target(ch, mid, base, dev)

            # --- сколько ботов ещё могут реагировать на этот пост ---
            # сначала пробуем быстрый метод, если он есть у BotManager
            try:
                eligible_total = len(bot_manager.eligible_bots_for_post(ch, mid))  # активные, ещё не реагировавшие
            except AttributeError:
                # fallback: активные боты минус уже реагировавшие (по actions)
                active_rows = bot_manager.get_active_bots()  # [(session_name, last_used), ...]
                active_names = {r[0] if not isinstance(r, sqlite3.Row) else r["session_name"] for r in active_rows}
                reacted_rows = bot_manager.conn.execute(
                    "SELECT DISTINCT session_name FROM actions "
                    "WHERE chat_id=? AND target_msg_id=? AND action_type='reaction'",
                    (ch, mid)
                ).fetchall()
                reacted = {row[0] if not isinstance(row, sqlite3.Row) else row["session_name"] for row in reacted_rows}
                eligible_total = max(0, len(active_names - reacted))

            # учитываем уже запланированные (queued|reserved): они «съедят» часть eligible
            eligible_remaining = max(0, eligible_total - queued)

            # итоговый "зазор": не планируем больше, чем реально могут поставить
            left = max(0, min(target - done - queued, eligible_remaining))
            if left <= 0:
                # подчистим висящие queued по этому посту, чтобы не висели
                conn.execute(
                    "DELETE FROM jobs WHERE type='react' AND status='queued' AND chat_id=? AND msg_id=?",
                    (ch, mid)
                )
                continue

            try:
                summary = (_json.loads(all_reactions) if all_reactions else {})
            except Exception:
                summary = {}
            available = {e: c for e, c in summary.items() if c and c > 0}
            if not available:
                continue

            # исключим эмодзи, которые уже есть в queued/reserved для этого поста
            existing_rows = conn.execute(
                "SELECT emoji FROM jobs WHERE type='react' AND chat_id=? AND msg_id=? AND status IN ('queued','reserved')",
                (ch, mid)
            ).fetchall()
            existing = { (row[0] if isinstance(row, tuple) else (row['emoji'] if isinstance(row, sqlite3.Row) else None))
                         for row in existing_rows if row }
            pairs = [(e, w) for e, w in available.items() if e not in existing]
            if not pairs:
                continue

            emojis, weights = zip(*pairs)
            emojis = list(emojis)
            weights = list(weights)

            # === Cooldown планирование ===
            # 1) последняя фактическая реакция
            last_row = bot_manager.conn.execute(
                "SELECT MAX(timestamp) AS t FROM actions WHERE chat_id=? AND target_msg_id=? AND action_type='reaction'",
                (ch, mid)
            ).fetchone()
            last_dt = None
            if last_row and last_row["t"]:
                try:
                    last_dt = datetime.fromisoformat(last_row["t"])
                except Exception:
                    last_dt = None

            # 2) учтём будущие поставленные (queued|reserved) not_before
            nb_row = conn.execute(
                "SELECT MAX(not_before) AS nb FROM jobs WHERE type='react' AND chat_id=? AND msg_id=? AND status IN ('queued','reserved')",
                (ch, mid)
            ).fetchone()
            nb_dt = None
            if nb_row and nb_row["nb"]:
                try:
                    nb_dt = datetime.fromisoformat(nb_row["nb"])
                except Exception:
                    nb_dt = None

            cursor = max([t for t in (last_dt, nb_dt) if t is not None], default=None)
            if cooldown <= 0:
                def next_not_before(i): return None
            else:
                def next_not_before(i):
                    base = datetime.utcnow()
                    if cursor:
                        start = max(base, cursor + timedelta(seconds=cooldown))
                    else:
                        start = base
                    return (start + timedelta(seconds=cooldown*i)).isoformat()

            # вставляем не более числа доступных разных эмодзи; без повторов в рамках одного прогона
#            to_add = min(left, len(emojis))
#            for i in range(to_add):
            idx_choice = random.choices(range(len(emojis)), weights=weights, k=1)[0]
            emo = emojis.pop(idx_choice)
            weights.pop(idx_choice)
            prio = idx + 1.0
            _nb = next_not_before(0)#i)
            conn.execute(
                "INSERT OR IGNORE INTO jobs (type, chat_id, msg_id, emoji, priority, created_at, not_before) VALUES (?,?,?,?,?,?,?)",
                ("react", ch, mid, emo, prio, _utcnow(), _nb)
            )
    # commit делается снаружи

# ---------- выдача/завершение ----------

def requeue_expired(conn: sqlite3.Connection, ttl_sec: int = 180):
    conn.execute(
        "UPDATE jobs SET status='queued', reserved_by=NULL, reserved_at=NULL "
        "WHERE status='reserved' AND datetime(reserved_at) <= datetime('now', '-' || ? || ' seconds')",
        (ttl_sec,)
    )

def reserve_next(conn: sqlite3.Connection, worker_id: str) -> Optional[sqlite3.Row]:
    row = conn.execute(
        """
        WITH picked AS (
          SELECT id FROM jobs
           WHERE status='queued'
             AND (not_before IS NULL OR datetime(not_before) <= datetime('now'))
           ORDER BY priority ASC, created_at ASC
           LIMIT 1
        )
        UPDATE jobs
           SET status='reserved', reserved_by=?, reserved_at=?
         WHERE id IN picked
        RETURNING id, type, chat_id, msg_id, emoji, priority, session_name
        """,
        (worker_id, _utcnow())
    ).fetchone()
    return row

def mark_dead(conn: sqlite3.Connection, job_id: int):
    conn.execute("UPDATE jobs SET status='dead' WHERE id=?", (job_id,))


def mark_done(conn: sqlite3.Connection, job_id: int):
    conn.execute("UPDATE jobs SET status='done' WHERE id=?", (job_id,))

def fail_and_maybe_requeue(conn: sqlite3.Connection, job_id: int, *, max_attempts: int = 5, backoff_sec: int = 60):
    row = conn.execute("SELECT attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row:
        return
    att = int(row["attempts"]) + 1
    if att >= max_attempts:
        conn.execute("UPDATE jobs SET status='dead', attempts=? WHERE id=?", (att, job_id))
    else:
        conn.execute(
            "UPDATE jobs SET status='queued', attempts=?, reserved_by=NULL, reserved_at=NULL, "
            "not_before = datetime('now', '+' || ? || ' seconds') WHERE id=?",
            (att, backoff_sec, job_id)
        )

# --- валидация: полезные хелперы для контроллера/воркеров ---
def _merge_payload(row_payload: str | None, **kv) -> str:
    try:
        data = json.loads(row_payload) if row_payload else {}
    except Exception:
        data = {}
    data.update(kv)
    return json.dumps(data, ensure_ascii=False)

def set_validation_code(conn: sqlite3.Connection, session_name: str, code: str):
    conn.execute(
        "UPDATE jobs SET payload=? WHERE type='validate_session' AND session_name=? AND status IN ('queued','reserved')",
        (_merge_payload(None, code=code), session_name)
    )
    conn.commit()

def get_validation_code(conn: sqlite3.Connection, session_name: str):
    row = conn.execute(
        "SELECT payload FROM jobs WHERE type='validate_session' AND session_name=? AND status IN ('queued','reserved') ORDER BY created_at DESC LIMIT 1",
        (session_name,)
    ).fetchone()
    if not row or not row["payload"]:
        return None
    try:
        data = json.loads(row["payload"])
        return data.get("code")
    except Exception:
        return None

def clear_validation_code(conn: sqlite3.Connection, session_name: str):
    conn.execute(
        "UPDATE jobs SET payload=NULL WHERE type='validate_session' AND session_name=? AND status IN ('queued','reserved')",
        (session_name,)
    )
    conn.commit()

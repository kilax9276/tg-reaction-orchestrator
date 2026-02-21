# Telegram Reaction Orchestrator

A multi-process Telegram automation framework that **plans** and **executes** message reaction jobs using:
- Telethon sessions (`.session`) as worker identities
- a Telegram *controller bot* for operator workflows (upload sessions, provide SMS codes, view stats)
- mobile SOCKS5 proxies with IP rotation (MobileProxy.Space API)
- SQLite (WAL) for durable queues and cross-process locks

> **Legal / ToS notice:** Automating Telegram actions can violate Telegram’s Terms of Service and can lead to account restrictions. Use at your own risk and only in compliance with local laws and platform rules.

## High-level architecture

**Processes**
1. **Controller bot** (`controller_bot.py`)
   - Telegram bot used by admins/operators.
   - Accepts ZIP uploads of `.session` files, updates config, shows stats.
   - Requests SMS login codes from admins and forwards them to workers.

2. **Worker pool** (`reaction_worker_pool.py`)
   - Reserves jobs from the `jobs` table in `bots.db` and executes them.
   - Uses **IPC session locks** (`session_store.py`) to prevent concurrent session use.
   - Uses **mobile proxies** (`proxy_manager.py` + `mobileproxy_api.py`) to acquire SOCKS5 + rotate external IP.

3. **Planner / scheduler** (`scheduler_bot.py`)
   - **Offline**: does not connect to Telegram.
   - Creates/refreshes jobs in `bots.db` based on cached posts in `posts.db` and targets in `config.json`.
   - Supports a sleep window.

**Databases**
- `bots.db` — bots registry, action log, and the `jobs` queue (SQLite WAL)
- `posts.db` — cached posts and reaction counters; operator overrides (NO-REACT / forced emoji)
- `sessions_state.db` — IPC session locks/queue (SQLite WAL)

## Quick start

### 1) Install dependencies

This repository does **not** include a `requirements.txt` in the provided source snapshot.  
From imports in the code, you will typically need at least:

- `telethon`
- `aiohttp`
- `aiosqlite`
- `requests`
- `PySocks` (usually installed as `pysocks`)
- Python 3.9+ (for `zoneinfo`)

### 2) Prepare Telegram credentials

- `api_id`, `api_hash` — from https://my.telegram.org
- `admin_bot_token` — from @BotFather (a separate bot account)
- `.session` files — Telethon session files for accounts that will react

### 3) Create `config.json`

Use the reference below (or copy `config.example.json`).

### 4) Run

```bash
python run.py
```

The controller bot will show your Telegram ID via `/start`. Add yourself as admin via `admin_ids` in config.

## Operator workflow

### Upload sessions (ZIP)

1. In the controller bot, run:
   - `/upload_mode`
2. Send a `.zip` that contains Telethon `.session` files.
3. The controller moves sessions into `sessions_dir` and enqueues `validate_session` jobs.

### Provide SMS login codes

When a session needs a code, the controller will DM admins with permission `upload_zip`:

`<session_name>: <code>`

To cancel validation:

`<session_name>: esc`

### Useful controller commands

- `/start` — show your Telegram ID and available commands
- `/upload_mode` — enable ZIP upload mode
- `/add_admin <id> <json_permissions>` — add/update admin rights
- `/stats` — show bot/action stats
- `/chats` — show configured channels
- `/lastposts <chat_id> [limit]` — list recent cached posts for a channel
- `/post <chat_id> <msg_id>` — show full cached post text
- `/noreact <chat_id> <msg_id>` — mark post as NO-REACT and purge queued react jobs
- `/allowreact <chat_id> <msg_id>` — remove NO-REACT
- `/forcerxn <chat_id> <msg_id> <emoji|clear>` — force a specific emoji or clear override
- `/restart` — request a full system restart (sends SIGINT to parent)

## How reaction planning works

The scheduler periodically:
1. Ensures `collect_posts` jobs exist when cache is stale.
2. Rebuilds `react` jobs based on:
   - the most recent `message_limit` posts per channel in `posts.db`
   - probability weighting for older posts (hyperbola curve)
   - per-channel targets (`reaction_targets`) + randomized per-post target deviation
   - current progress (already reacted bots) and remaining eligible bots
   - optional cooldown between reactions on the same post (`react_cooldown_sec`)

Workers then execute one reaction per job.

## `config.json` reference

> Types below use JSON notation. Times are strings in `HH:MM`.

### Required

| Key | Type | Description |
|---|---|---|
| `api_id` | number | Telegram API ID. |
| `api_hash` | string | Telegram API hash. |
| `admin_bot_token` | string | Bot token for the controller bot. |
| `mobileproxy_token` | string | MobileProxy.Space API token. |
| `admin_ids` | number[] | Telegram user IDs that are seeded as admins on first run. |

### Proxy / IP policy

| Key | Type | Default | Description |
|---|---:|---:|---|
| `proxy_ids` | number[] | `[]` | MobileProxy.Space proxy IDs available for workers. |
| `ip_db_path` | string | `"ip_data.db"` | SQLite file used for proxy/IP cache. |
| `max_bots_per_ip` | number | `2` | Max unique `session_name` per external IP within ~1 hour. |

### Paths

| Key | Type | Default | Description |
|---|---:|---:|---|
| `sessions_dir` | string | `"sessions"` | Where validated `.session` files live for workers. |
| `session_unpack_dir` | string | `"/tmp/telethon_sessions"` | Temporary ZIP unpack directory used by controller/worker. |
| `bots_db_path` | string | `"bots.db"` | Bots DB path (also contains `jobs`). |
| `posts_db_path` | string | `"posts.db"` | Posts cache DB path. |
| `session_log_path` | string | `"session_log.db"` | Controller log DB path for validation statuses. |
| `session_unpack_dir` | string | `"/tmp/telethon_sessions"` | Where controller stores uploaded ZIPs and unpacked sessions. |
| `uploaded_zip_ttl_hours` | number | `6` | How long to keep uploaded ZIP files in temp directory. |

### Worker pool

| Key | Type | Default | Description |
|---|---:|---:|---|
| `max_workers` | number | `5` | Number of async workers in the worker pool process. |
| `worker_sleep` | number | `1.0` | Sleep when no jobs available (seconds). |
| `reaction_delay` | number | `1.5` | Delay after a successful reaction (seconds). |
| `min_bot_reuse_delay` | number | `120` | Minimum seconds between uses of the same session (worker-level). |
| `job_reserve_ttl_sec` | number | `180` | If a reserved job is not finished in time, it is re-queued. |
| `queue_refill_interval_sec` | number | `10` | How often to refill session_store queue and purge bad sessions. |
| `sms_code_timeout` | number | `120` | How long workers wait for an SMS code from admins. |

### Planner / scheduler

| Key | Type | Default | Description |
|---|---:|---:|---|
| `interval` | number | `15` | Planner loop interval (seconds). |
| `initial_delay` | number | `0` | Delay before the first planning run (seconds). |
| `posts_refresh_interval_sec` | number | `300` | How often each channel should be re-fetched (seconds). |
| `react_cooldown_sec` | number | `0` | Cooldown between reactions to the same post (seconds). |
| `message_limit` | number | `5` | How many recent posts per channel are considered for planning. |
| `reaction_targets` | object | `{}` | Mapping: `"chat_id"` → base target count. |
| `target_deviation` | number | `0` | Per-post random deviation around the base target. |
| `reaction_threshold` | number | `5` | Present in code but not currently enforced in planning. |
| `total_reactions_per_run` | number | `5` | Present in code but not currently enforced in planning. |

**Hyperbola probability curve** (older posts get lower probability):
- `hyperbola_k` (default `1.0`)
- `hyperbola_c` (default `1.0`)
- `hyperbola_d` (default `1.0`)

Probability used: `p = (k / (idx + c)) ** d` where `idx` is 0 for newest post.

### Channels

| Key | Type | Description |
|---|---|---|
| `channel_ids` | number[] | Channels to operate on (their numeric IDs). |
| `channel_invite_links` | object | Optional mapping `"chat_id"` → invite link for private channels (used to join/resolve entity). |

### Sleep window (optional)

```json
"sleep": {
  "enabled": true,
  "timezone": "Europe/Moscow",
  "start": "01:00",
  "end": "08:30"
}
```

## Example `config.json`

```json
{
  "api_id": 123456,
  "api_hash": "YOUR_API_HASH",
  "admin_bot_token": "123456:AA...",
  "admin_ids": [111111111],
  "mobileproxy_token": "YOUR_MOBILEPROXY_TOKEN",

  "proxy_ids": [10001, 10002],
  "max_bots_per_ip": 2,

  "sessions_dir": "sessions",
  "session_unpack_dir": "/tmp/telethon_sessions",
  "bots_db_path": "bots.db",
  "posts_db_path": "posts.db",
  "ip_db_path": "ip_data.db",
  "session_log_path": "session_log.db",

  "channel_ids": [-1001234567890],
  "channel_invite_links": {
    "-1001234567890": "https://t.me/+INVITE_HASH"
  },
  "reaction_targets": {
    "-1001234567890": 20
  },
  "target_deviation": 3,

  "posts_refresh_interval_sec": 300,
  "message_limit": 10,
  "react_cooldown_sec": 30,

  "max_workers": 5,
  "worker_sleep": 1.0,
  "reaction_delay": 1.5,
  "min_bot_reuse_delay": 120,
  "sms_code_timeout": 120,

  "sleep": {
    "enabled": false,
    "timezone": "UTC",
    "start": "00:00",
    "end": "00:00"
  }
}
```

## Troubleshooting

- If you see many `database is locked`, ensure SQLite WAL is enabled (this project enables it in most places) and avoid placing DB files on network filesystems.
- If private channels cannot be resolved, add `channel_invite_links` for those chat IDs.
- If sessions get marked `revoked` or `frozen`, they will be removed from the IPC queue and excluded from planning/execution.

License
This project is licensed under the Apache License, Version 2.0.
You may use, modify, and distribute this software in compliance with the License.

See the LICENSE file for full license text.

Copyright (c) 2026 Kolobov Aleksei Telegram: @kilax9276

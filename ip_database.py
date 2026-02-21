# -----------------------------------------------------------------------------
# ip_database.py
#
# SQLite storage used by proxy_manager.AsyncProxyManager.
#
# Tables:
#   - ip_history: per (external) IP address status (GOOD/BAN) and ban lift time
#   - proxy_info: cached proxy metadata (credentials, endpoints, external_ip, etc.)
#   - ip_sessions: tracking of which session_name used which external_ip recently
#     (enables 'max_total_bots_per_ip' policy)
# -----------------------------------------------------------------------------

import os
import sqlite3
from typing import Optional
from datetime import datetime, timedelta

class IPDatabase:
    def __init__(self, db_path="ip_data.db"):
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self._create_table()

    def _create_table(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS ip_history (
                    ip_address TEXT PRIMARY KEY,
                    proxy_id INTEGER,
                    login TEXT,
                    password TEXT,
                    time_acquired TEXT,
                    time_swapped TEXT,
                    status TEXT,
                    ban_lift_time TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS proxy_info (
                    proxy_id INTEGER PRIMARY KEY,
                    proxy_login TEXT,
                    proxy_pass TEXT,
                    socks5_ip TEXT,
                    socks5_port TEXT,
                    proxy_operator TEXT,
                    proxy_exp TEXT,
                    proxy_key TEXT,
                    proxy_change_ip_url TEXT,
                    eid INTEGER,
                    geoid INTEGER,
                    id_country INTEGER,
                    external_ip TEXT,
                    last_updated TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS ip_sessions (
                    ip_address TEXT,
                    session_name TEXT,
                    acquired_at TEXT,
                    is_active INTEGER DEFAULT 1,
                    PRIMARY KEY (ip_address, session_name)
                )
            """)

    def save_proxy_info(self, info: dict, external_ip: Optional[str] = None):
        now = datetime.utcnow().isoformat()
        proxy_id = info.get("proxy_id")

        # Проверяем, существует ли запись
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM proxy_info WHERE proxy_id = ?", (proxy_id,))
        exists = cur.fetchone()[0] > 0

        fields = {
            "proxy_login": info.get("proxy_login"),
            "proxy_pass": info.get("proxy_pass"),
            "socks5_ip": info.get("proxy_independent_socks5_host_ip"),
            "socks5_port": info.get("proxy_independent_port"),
            "proxy_operator": info.get("proxy_operator"),
            "proxy_exp": info.get("proxy_exp"),
            "proxy_key": info.get("proxy_key"),
            "proxy_change_ip_url": info.get("proxy_change_ip_url"),
            "eid": info.get("eid"),
            "geoid": info.get("geoid"),
            "id_country": info.get("id_country"),
            "external_ip": external_ip,
            "last_updated": now,
        }

        with self.conn:
            if exists:
                set_clause = ", ".join([f"{k} = ?" for k in fields.keys()])
                values = list(fields.values()) + [proxy_id]
                self.conn.execute(
                    f"UPDATE proxy_info SET {set_clause} WHERE proxy_id = ?", values
                )
            else:
                self.conn.execute("""
                    INSERT INTO proxy_info (
                        proxy_id, proxy_login, proxy_pass, socks5_ip, socks5_port, proxy_operator,
                        proxy_exp, proxy_key, proxy_change_ip_url, eid, geoid, id_country, external_ip, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    proxy_id,
                    fields["proxy_login"],
                    fields["proxy_pass"],
                    fields["socks5_ip"],
                    fields["socks5_port"],
                    fields["proxy_operator"],
                    fields["proxy_exp"],
                    fields["proxy_key"],
                    fields["proxy_change_ip_url"],
                    fields["eid"],
                    fields["geoid"],
                    fields["id_country"],
                    fields["external_ip"],
                    fields["last_updated"]
                ))

    def count_recent_sessions(self, ip_address, hours=1):
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        cur = self.conn.cursor()
        cur.execute("""
            SELECT COUNT(DISTINCT session_name)
            FROM ip_sessions
            WHERE ip_address = ? AND acquired_at >= ?
        """, (ip_address, cutoff))
        row = cur.fetchone()
        return row[0] if row else 0



    def add_active_session(self, ip_address, session_name):
        now = datetime.utcnow().isoformat()
        with self.conn:
            self.conn.execute("""
                INSERT INTO ip_sessions (ip_address, session_name, acquired_at, is_active)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(ip_address, session_name) DO UPDATE SET
                    acquired_at=excluded.acquired_at,
                    is_active=1
            """, (ip_address, session_name, now))


    def remove_active_session(self, ip_address, session_name):
        with self.conn:
            self.conn.execute("""
                UPDATE ip_sessions
                SET is_active = 0
                WHERE ip_address = ? AND session_name = ?
            """, (ip_address, session_name))

    def purge_old_sessions(self, max_age_hours=48):
        cutoff = (datetime.utcnow() - timedelta(hours=max_age_hours)).isoformat()
        with self.conn:
            self.conn.execute("""
                DELETE FROM ip_sessions
                WHERE datetime(acquired_at) < datetime(?)
            """, (cutoff,))

    def count_active_sessions(self, ip_address):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM ip_sessions
            WHERE ip_address = ? AND is_active = 1
        """, (ip_address,))
        row = cur.fetchone()
        return row[0] if row else 0


    def get_active_sessions(self, ip_address):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT session_name FROM ip_sessions
            WHERE ip_address = ?
        """, (ip_address,))
        return [row[0] for row in cur.fetchall()]

    def add_ip(self, proxy_id, ip, login, password):
        now = datetime.utcnow().isoformat()
        with self.conn:
            self.conn.execute("""
                INSERT OR REPLACE INTO ip_history (
                    ip_address, proxy_id, login, password, time_acquired, status
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (ip, proxy_id, login, password, now, "GOOD"))

    def mark_banned(self, ip):
        now = datetime.utcnow()
        lift_time = (now + timedelta(hours=24)).isoformat()
        with self.conn:
            self.conn.execute("""
                UPDATE ip_history
                SET status = ?, time_swapped = ?, ban_lift_time = ?
                WHERE ip_address = ?
            """, ("BAN", now.isoformat(), lift_time, ip))

    def update_external_ip(self, ip, external_ip):
        with self.conn:
            self.conn.execute("""
                UPDATE proxy_info
                SET external_ip = ?
                WHERE socks5_ip = ?
            """, (external_ip, ip))

    def get_ip_status(self, ip):
        self.remove_expired_bans()
        cur = self.conn.cursor()
        cur.execute("SELECT status FROM ip_history WHERE ip_address = ?", (ip,))
        row = cur.fetchone()
        return row[0] if row else None

    def remove_expired_bans(self):
        now = datetime.utcnow().isoformat()
        with self.conn:
            self.conn.execute("""
                UPDATE ip_history
                SET status = "GOOD", ban_lift_time = NULL
                WHERE status = "BAN" AND ban_lift_time <= ?
            """, (now,))

    def close(self):
        self.conn.close()

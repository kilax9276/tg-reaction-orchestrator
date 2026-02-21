# -----------------------------------------------------------------------------
# PostManager.py
# Copyright Kolobov Aleksei @kilax9276
#
# Local cache for channel posts and their current reaction counters.
#
# Stored in posts.db:
#   - posts(msg_id, chat_id, text, all_reactions, target, blocked, forced_emoji)
#
# Key features:
#   - fetch_posts(): downloads recent messages and builds a {emoji: count} summary
#     for each message that has reactions; stores it in SQLite.
#   - get_or_set_target(): persists per-post target reactions (randomized by deviation)
#     so that repeated planner runs remain stable.
#   - overrides:
#       blocked (NO-REACT) and forced_emoji can be edited via controller bot commands.
# -----------------------------------------------------------------------------

# File: PostManager.py
import sqlite3
import json
import random
from datetime import datetime
from telethon.tl.types import PeerChannel, ReactionEmoji, ReactionCustomEmoji
from telethon.tl.functions.messages import SendReactionRequest

class PostManager:
    def __init__(self, client, db_path='posts.db'):
        self.client = client
        self.conn = sqlite3.connect(db_path)
        self._init_db()
        self._migrate_schema()

    def _init_db(self):
        cur = self.conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                msg_id INTEGER,
                chat_id INTEGER,
                text TEXT,
                all_reactions TEXT,
                target INTEGER,
                PRIMARY KEY (msg_id, chat_id)
            )
        ''')
        self.conn.commit()

    def _migrate_schema(self):
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(posts)")
        columns = [row[1] for row in cur.fetchall()]
        legacy_fields = {'my_reaction', 'reacted_by', 'reacted_at', 'reaction_status', 'reaction_error'}

        if legacy_fields.intersection(columns):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🛠 Выполняется миграция схемы posts...")
            cur.execute('''
                CREATE TABLE IF NOT EXISTS posts_new (
                    msg_id INTEGER,
                    chat_id INTEGER,
                    text TEXT,
                    all_reactions TEXT,
                    target INTEGER,
                    PRIMARY KEY (msg_id, chat_id)
                )
            ''')
            cur.execute('''
                INSERT INTO posts_new (msg_id, chat_id, text, all_reactions)
                SELECT msg_id, chat_id, text, all_reactions FROM posts
            ''')
            cur.execute("DROP TABLE posts")
            cur.execute("ALTER TABLE posts_new RENAME TO posts")
            self.conn.commit()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]✅ Миграция posts завершена.")

            cur.execute("PRAGMA table_info(posts)")
            columns = [row[1] for row in cur.fetchall()]

        if 'target' not in columns:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🛠 Добавляется поле target в posts...")
            cur.execute("ALTER TABLE posts ADD COLUMN target INTEGER")
            self.conn.commit()

        # новые поля для управления реакциями
        if 'blocked' not in columns:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🛠 Добавляется поле blocked в posts...")
            cur.execute("ALTER TABLE posts ADD COLUMN blocked INTEGER DEFAULT 0")
            self.conn.commit()
        if 'forced_emoji' not in columns:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]🛠 Добавляется поле forced_emoji в posts...")
            cur.execute("ALTER TABLE posts ADD COLUMN forced_emoji TEXT")
            self.conn.commit()

    async def fetch_posts(self, channel_id: int, limit=10):
        try:
            peer = await self.client.get_entity(channel_id)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][DEBUG] get_entity({channel_id}) → {peer.id=} {getattr(peer, 'title', None)=} {type(peer)=}")
        except ValueError as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Не удалось найти канал {channel_id}: {e}")
            return {"status": "not_found", "messages": []}
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Ошибка при получении entity канала {channel_id}: {e}")
            return {"status": "error", "messages": []}

        try:
            messages = await self.client.get_messages(peer, limit=limit)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][DEBUG] get_messages → {len(messages)} сообщений")
            for msg in messages:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][DEBUG] msg.id={msg.id}, date={msg.date}, text={repr(msg.text)[:50]}, "
                      f"has_reactions={bool(msg.reactions)}, reactions={msg.reactions.results if msg.reactions else None}")
                summary = {}
                if msg.reactions and hasattr(msg.reactions, 'results'):
                    for r in msg.reactions.results:
                        reaction = r.reaction
                        if isinstance(reaction, ReactionEmoji):
                            key = reaction.emoticon
                        elif isinstance(reaction, ReactionCustomEmoji):
                            key = f"custom:{reaction.document_id}"
                        else:
                            continue
                        summary[key] = r.count

                self._store_post(
                    msg_id=msg.id,
                    chat_id=channel_id,
                    text=msg.text or "",
                    all_reactions=json.dumps(summary)
                )
            return {"status": "ok", "messages": messages}
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]❌ Ошибка при загрузке сообщений из канала {channel_id}: {e}")
            return {"status": "error", "messages": []}




    def _store_post(self, msg_id, chat_id, text, all_reactions):
        cur = self.conn.cursor()
        # Пытаемся вставить, если нет — обновим
        cur.execute('''
            INSERT INTO posts (msg_id, chat_id, text, all_reactions)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(msg_id, chat_id) DO UPDATE SET
                text = excluded.text,
                all_reactions = excluded.all_reactions
        ''', (msg_id, chat_id, text, all_reactions))
        self.conn.commit()


    def get_reaction_summary(self, chat_id, msg_id):
        cur = self.conn.cursor()
        cur.execute(
            'SELECT all_reactions FROM posts WHERE msg_id=? AND chat_id=?',
            (msg_id, chat_id)
        )
        row = cur.fetchone()
        if row:
            try:
                return json.loads(row[0])
            except:
                return {}
        return {}

    def get_or_set_target(self, chat_id: int, msg_id: int, base_target: int, deviation: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            'SELECT target FROM posts WHERE chat_id=? AND msg_id=?',
            (chat_id, msg_id)
        )
        row = cur.fetchone()
        if row and row[0]:
            return row[0]

        dev = random.randint(-deviation, deviation)
        tgt = max(1, base_target + dev)
        cur.execute(
            'UPDATE posts SET target=? WHERE chat_id=? AND msg_id=?',
            (tgt, chat_id, msg_id)
        )
        self.conn.commit()
        return tgt

    # async def set_reaction(self, peer, msg_id: int, emoticon='❤️'):

    #     print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]set_reaction: {msg_id}:{emoticon}")
    #     try:
    #         await self.client(SendReactionRequest(
    #             peer=peer,
    #             msg_id=msg_id,
    #             reaction=[ReactionEmoji(emoticon=emoticon)]
    #         ))
    #     except Exception as e:
    #         print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠️ Ошибка при установке реакции {emoticon} на {msg_id}: {e}")


    async def set_reaction(self, peer, msg_id: int, emoticon='❤️'):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]set_reaction: {msg_id}:{emoticon}")
        try:
            if emoticon.startswith("custom:"):
                doc_id = int(emoticon.split(":")[1])
                reaction_obj = ReactionCustomEmoji(document_id=doc_id)
            else:
                reaction_obj = ReactionEmoji(emoticon=emoticon)

            await self.client(SendReactionRequest(
                peer=peer,
                msg_id=msg_id,
                reaction=[reaction_obj]
            ))
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]⚠️ Ошибка при установке реакции {emoticon} на {msg_id}: {e}")
            raise

    def set_block(self, chat_id: int, msg_id: int, block: bool = True) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE posts SET blocked=? WHERE chat_id=? AND msg_id=?", (1 if block else 0, chat_id, msg_id))
        self.conn.commit()

    def set_forced_emoji(self, chat_id: int, msg_id: int, emoji: str | None) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE posts SET forced_emoji=? WHERE chat_id=? AND msg_id=?", (emoji, chat_id, msg_id))
        self.conn.commit()

    def get_overrides(self, chat_id: int, msg_id: int) -> dict:
        cur = self.conn.cursor()
        row = cur.execute("SELECT blocked, forced_emoji FROM posts WHERE chat_id=? AND msg_id=?", (chat_id, msg_id)).fetchone()
        if not row:
            return {"blocked": 0, "forced_emoji": None}
        try:
            blocked = int(row["blocked"]) if isinstance(row, sqlite3.Row) else int(row[0] or 0)
            forced  = row["forced_emoji"] if isinstance(row, sqlite3.Row) else (row[1] if len(row)>1 else None)
        except Exception:
            blocked = int(row[0] or 0)
            forced  = row[1] if len(row) > 1 else None
        return {"blocked": blocked, "forced_emoji": forced}

    def list_recent_posts(self, chat_id: int, limit: int = 10):
        limit = max(1, min(int(limit), 50))
        cur = self.conn.cursor()
        rows = cur.execute(
            "SELECT msg_id, text, blocked, forced_emoji FROM posts WHERE chat_id=? ORDER BY msg_id DESC LIMIT ?",
            (chat_id, limit)
        ).fetchall()
        out = []
        for r in rows:
            try:
                mid = r["msg_id"]; txt = r["text"] or ""; blocked = int(r["blocked"] or 0); forced = r["forced_emoji"]
            except Exception:
                mid, txt, blocked, forced = r[0], (r[1] or ""), int(r[2] or 0), (r[3] if len(r) > 3 else None)
            out.append({"msg_id": mid, "text": txt, "blocked": blocked, "forced_emoji": forced})
        return out

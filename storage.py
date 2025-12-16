import aiosqlite
import asyncio
import os
import csv
from datetime import datetime

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    ts TEXT NOT NULL,
    price REAL NOT NULL,
    size REAL,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts);
"""

class AsyncStorage:
    def __init__(self, path="ticks.db", csv_dir="csv_data"):
        self.path = path or "ticks.db"
        self.csv_dir = csv_dir or "csv_data"

        self._queue = asyncio.Queue()
        self._task = None
        self._db = None
        self._running = False

        os.makedirs(self.csv_dir, exist_ok=True)

    async def start(self):
        db_dir = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(db_dir, exist_ok=True)

        self._db = await aiosqlite.connect(self.path, timeout=30.0)

        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("PRAGMA synchronous=NORMAL;")
        await self._db.execute("PRAGMA busy_timeout=1000;")

        await self._db.executescript(DB_SCHEMA)
        await self._db.commit()

        self._running = True
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._writer_loop())

    async def _writer_loop(self):
        if not self._db:
            return

        while self._running:
            try:
                try:
                    first = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                batch = [first]
                for _ in range(200):
                    try:
                        batch.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                await self._write_batch_to_db(batch)

                try:
                    await asyncio.to_thread(self._append_batch_to_csv, batch)
                except Exception:
                    pass

            except Exception:
                await asyncio.sleep(0.2)

    async def _write_batch_to_db(self, batch):
        try:
            await self._db.execute("BEGIN")
            stmt = "INSERT INTO ticks (symbol, ts, price, size) VALUES (?, ?, ?, ?)"

            for t in batch:
                try:
                    await self._db.execute(
                        stmt,
                        (
                            t.get("symbol"),
                            t.get("ts"),
                            float(t.get("price")),
                            float(t.get("size", 0.0)),
                        ),
                    )
                except Exception:
                    continue

            await self._db.commit()
        except Exception:
            try:
                await self._db.rollback()
            except Exception:
                pass

    def _append_batch_to_csv(self, batch):
        header = ["symbol", "ts", "price", "size"]

        all_path = os.path.join(self.csv_dir, "ticks_all.csv")
        try:
            write_header = not os.path.exists(all_path)
            with open(all_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(header)
                for t in batch:
                    writer.writerow([
                        t.get("symbol"),
                        t.get("ts"),
                        t.get("price"),
                        t.get("size", 0.0)
                    ])
        except Exception:
            pass

        per_symbol = {}
        for t in batch:
            sym = str(t.get("symbol") or "UNKNOWN").upper()
            per_symbol.setdefault(sym, []).append(t)

        for sym, rows in per_symbol.items():
            try:
                path = os.path.join(self.csv_dir, f"{sym}.csv")
                write_header = not os.path.exists(path)
                with open(path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    if write_header:
                        writer.writerow(header)
                    for t in rows:
                        writer.writerow([
                            t.get("symbol"),
                            t.get("ts"),
                            t.get("price"),
                            t.get("size", 0.0)
                        ])
            except Exception:
                pass

    async def enqueue_tick(self, tick):
        try:
            await self._queue.put(tick)
        except Exception:
            pass

    async def close(self):
        self._running = False

        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=1.0)
            except Exception:
                try:
                    self._task.cancel()
                    await asyncio.sleep(0.05)
                except Exception:
                    pass

        if self._db:
            try:
                await self._db.commit()
            except Exception:
                pass
            try:
                await self._db.close()
            except Exception:
                pass
            self._db = None

    async def fetch_recent(self, limit=500):
        if not os.path.exists(self.path):
            return []

        if self._db:
            try:
                cur = await self._db.execute(
                    "SELECT symbol, ts, price, size FROM ticks ORDER BY id DESC LIMIT ?",
                    (limit,),
                )
                rows = await cur.fetchall()
                return [dict(r) for r in rows]
            except Exception:
                pass

        conn = await aiosqlite.connect(self.path)
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT symbol, ts, price, size FROM ticks ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        await conn.close()
        return [dict(r) for r in rows]
import sqlite3
import json
import asyncio
import os
from typing import Dict, Any, List, Optional

DB_PATH = os.getenv("DATABASE_URL", "orders.db")

def _init_db_sync():
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
        with open(schema_path, "r") as f:
            cursor.executescript(f.read())
        conn.commit()
    finally:
        conn.close()

async def init_db():
    await asyncio.to_thread(_init_db_sync)

def _execute_write(query: str, params: tuple) -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        return cursor.lastrowid or 0
    finally:
        conn.close()

def _execute_read_one(query: str, params: tuple) -> Optional[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def _execute_read_all(query: str, params: tuple) -> List[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()

async def create_order(order_id: str, state: str, address: Dict[str, Any]) -> None:
    query = """
    INSERT INTO orders (id, state, address_json, created_at, updated_at)
    VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT(id) DO UPDATE SET
        state = excluded.state,
        updated_at = CURRENT_TIMESTAMP;
    """
    await asyncio.to_thread(_execute_write, query, (order_id, state, json.dumps(address)))

async def update_order_state(order_id: str, state: str) -> None:
    query = "UPDATE orders SET state = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
    await asyncio.to_thread(_execute_write, query, (state, order_id))

async def update_order_address(order_id: str, address: Dict[str, Any]) -> None:
    query = "UPDATE orders SET address_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
    await asyncio.to_thread(_execute_write, query, (json.dumps(address), order_id))

async def get_order(order_id: str) -> Optional[Dict[str, Any]]:
    query = "SELECT * FROM orders WHERE id = ?"
    res = await asyncio.to_thread(_execute_read_one, query, (order_id,))
    if res:
        res["address"] = json.loads(res["address_json"]) if res.get("address_json") else {}
    return res

async def create_payment_idempotent(payment_id: str, order_id: str, amount: float, status: str) -> Dict[str, Any]:
    def _tx():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM payments WHERE payment_id = ?", (payment_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
            
            cursor.execute(
                "INSERT INTO payments (payment_id, order_id, status, amount, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
                (payment_id, order_id, status, amount)
            )
            conn.commit()
            return {"payment_id": payment_id, "order_id": order_id, "status": status, "amount": amount}
        finally:
            conn.close()
            
    return await asyncio.to_thread(_tx)

async def log_event(order_id: str, event_type: str, payload: Optional[Dict[str, Any]] = None) -> None:
    query = "INSERT INTO events (order_id, type, payload_json, ts) VALUES (?, ?, ?, CURRENT_TIMESTAMP)"
    payload_str = json.dumps(payload) if payload else "{}"
    await asyncio.to_thread(_execute_write, query, (order_id, event_type, payload_str))

async def get_events(order_id: str) -> List[Dict[str, Any]]:
    query = "SELECT type, payload_json, ts FROM events WHERE order_id = ? ORDER BY ts ASC"
    rows = await asyncio.to_thread(_execute_read_all, query, (order_id,))
    for r in rows:
        r["payload"] = json.loads(r["payload_json"]) if r.get("payload_json") else {}
    return rows

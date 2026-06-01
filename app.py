import os
import logging
from typing import Dict, Any, List
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from temporalio.client import Client
from contextlib import asynccontextmanager

import db
from workflows import OrderWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

temporal_client: Client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global temporal_client
    await db.init_db()
    
    temporal_host = os.getenv("TEMPORAL_HOST", "localhost:7233")
    logger.info(f"Connecting to Temporal client at {temporal_host}...")
    try:
        temporal_client = await Client.connect(temporal_host)
        logger.info("Connected to Temporal client.")
    except Exception as e:
        logger.error(f"Could not connect to Temporal server: {e}")
    yield

app = FastAPI(title="Trellis Eng Console", lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    dashboard_path = os.path.join(os.path.dirname(__file__), "templates", "dashboard.html")
    if os.path.exists(dashboard_path):
        return FileResponse(dashboard_path)
    return "Dashboard template not found."

@app.post("/orders/{order_id}/start")
async def start_order(order_id: str, payment_id: str):
    if not temporal_client:
        raise HTTPException(status_code=503, detail="Temporal client not ready")
    
    await db.create_order(order_id, "RECEIVED", {})
    await db.log_event(order_id, "API_TRIGGERED", {"payment_id": payment_id})

    try:
        await temporal_client.start_workflow(
            OrderWorkflow.run,
            args=[order_id, payment_id],
            id=order_id,
            task_queue="default",
        )
        logger.info(f"Started OrderWorkflow for ID: {order_id}")
        return {"status": "started", "order_id": order_id}
    except Exception as e:
        logger.error(f"Failed to start workflow: {e}")
        await db.update_order_state(order_id, "FAILED")
        await db.log_event(order_id, "START_FAILED", {"error": str(e)})
        raise HTTPException(status_code=500, detail=f"Failed to start workflow: {str(e)}")

@app.post("/orders/{order_id}/signals/approve")
async def approve_order(order_id: str):
    if not temporal_client:
        raise HTTPException(status_code=503, detail="Temporal client not ready")
    try:
        handle = temporal_client.get_workflow_handle(order_id)
        await handle.signal(OrderWorkflow.ApproveOrder)
        await db.log_event(order_id, "SIGNAL_SENT", {"signal": "ApproveOrder"})
        return {"status": "signal_sent", "signal": "ApproveOrder"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to signal workflow: {str(e)}")

@app.post("/orders/{order_id}/signals/cancel")
async def cancel_order(order_id: str):
    if not temporal_client:
        raise HTTPException(status_code=503, detail="Temporal client not ready")
    try:
        handle = temporal_client.get_workflow_handle(order_id)
        await handle.signal(OrderWorkflow.CancelOrder)
        await db.log_event(order_id, "SIGNAL_SENT", {"signal": "CancelOrder"})
        return {"status": "signal_sent", "signal": "CancelOrder"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to signal workflow: {str(e)}")

@app.post("/orders/{order_id}/signals/address")
async def update_address(order_id: str, address: Dict[str, Any]):
    if not temporal_client:
        raise HTTPException(status_code=503, detail="Temporal client not ready")
    try:
        handle = temporal_client.get_workflow_handle(order_id)
        await handle.signal(OrderWorkflow.UpdateAddress, address)
        await db.update_order_address(order_id, address)
        await db.log_event(order_id, "SIGNAL_SENT", {"signal": "UpdateAddress", "address": address})
        return {"status": "signal_sent", "signal": "UpdateAddress"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to signal workflow: {str(e)}")

@app.get("/orders/{order_id}/status")
async def get_order_status(order_id: str):
    db_order = await db.get_order(order_id)
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found in database")
    
    events = await db.get_events(order_id)
    db_order["events"] = events

    temporal_state = {}
    if temporal_client:
        try:
            handle = temporal_client.get_workflow_handle(order_id)
            temporal_state = await handle.query(OrderWorkflow.status)
        except Exception as e:
            temporal_state = {"error": str(e), "current_step": "FINISHED"}
            
    return {
        "db": db_order,
        "temporal": temporal_state
    }

@app.get("/api/orders")
async def get_all_orders():
    def _list_orders():
        import sqlite3
        conn = sqlite3.connect(db.DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, state, updated_at FROM orders ORDER BY updated_at DESC")
            return [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()
    return await db.asyncio.to_thread(_list_orders)

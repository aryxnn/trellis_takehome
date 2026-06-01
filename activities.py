import asyncio
import random
from typing import Dict, Any
from temporalio import activity
import db

async def flaky_call() -> None:
    rand_num = random.random()
    if rand_num < 0.33:
        raise RuntimeError("Forced failure for testing")
    if rand_num < 0.67:
        await asyncio.sleep(300)

async def order_received(order_id: str) -> Dict[str, Any]:
    await flaky_call()
    initial_address = {"street": "123 Stanford Ave", "city": "Palo Alto", "state": "CA", "zip": "94306"}
    await db.create_order(order_id, "RECEIVED", initial_address)
    await db.log_event(order_id, "ORDER_RECEIVED", {"order_id": order_id, "address": initial_address})
    return {"order_id": order_id, "items": [{"sku": "ABC", "qty": 1}], "address": initial_address}

async def order_validated(order: Dict[str, Any]) -> bool:
    await flaky_call()
    order_id = order["order_id"]
    stored_order = await db.get_order(order_id)
    if not stored_order:
        raise ValueError(f"Order {order_id} not found in DB")
    if not order.get("items"):
        raise ValueError("No items to validate")
    
    await db.update_order_state(order_id, "VALIDATED")
    await db.log_event(order_id, "ORDER_VALIDATED", {"order_id": order_id})
    return True

async def payment_charged(order: Dict[str, Any], payment_id: str) -> Dict[str, Any]:
    await flaky_call()
    order_id = order["order_id"]
    amount = sum(i.get("qty", 1) for i in order.get("items", [])) * 50.0
    
    await db.create_payment_idempotent(payment_id, order_id, amount, "SUCCESS")
    await db.update_order_state(order_id, "PAID")
    await db.log_event(order_id, "PAYMENT_CHARGED", {"payment_id": payment_id, "amount": amount, "status": "SUCCESS"})
    return {"status": "charged", "amount": amount, "payment_id": payment_id}

async def order_shipped(order: Dict[str, Any]) -> str:
    await flaky_call()
    order_id = order["order_id"]
    await db.update_order_state(order_id, "SHIPPED")
    await db.log_event(order_id, "ORDER_SHIPPED", {"order_id": order_id})
    return "Shipped"

async def package_prepared(order: Dict[str, Any]) -> str:
    await flaky_call()
    order_id = order["order_id"]
    await db.update_order_state(order_id, "PREPARED")
    await db.log_event(order_id, "PACKAGE_PREPARED", {"order_id": order_id})
    return "Package ready"

async def carrier_dispatched(order: Dict[str, Any]) -> str:
    await flaky_call()
    order_id = order["order_id"]
    await db.update_order_state(order_id, "DISPATCHED")
    await db.log_event(order_id, "CARRIER_DISPATCHED", {"order_id": order_id})
    return "Dispatched"


@activity.defn
async def receive_order_activity(order_id: str) -> Dict[str, Any]:
    return await order_received(order_id)

@activity.defn
async def validate_order_activity(order: Dict[str, Any]) -> bool:
    return await order_validated(order)

@activity.defn
async def charge_payment_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    order = payload["order"]
    payment_id = payload["payment_id"]
    return await payment_charged(order, payment_id)

@activity.defn
async def prepare_package_activity(order: Dict[str, Any]) -> str:
    return await package_prepared(order)

@activity.defn
async def dispatch_carrier_activity(order: Dict[str, Any]) -> str:
    return await carrier_dispatched(order)

@activity.defn
async def ship_order_activity(order: Dict[str, Any]) -> str:
    return await order_shipped(order)

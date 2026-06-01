import asyncio
import logging
import sys
from temporalio.client import Client
from temporalio.worker import Worker

import db
from workflows import OrderWorkflow, ShippingWorkflow
import activities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("worker")

async def main():
    logger.info("Initializing database...")
    await db.init_db()

    temporal_host = "localhost:7233"
    logger.info(f"Connecting to Temporal server at {temporal_host}...")
    try:
        client = await Client.connect(temporal_host)
    except Exception as e:
        logger.error(f"Failed to connect to Temporal: {e}")
        sys.exit(1)

    logger.info("Starting workers...")

    default_worker = Worker(
        client,
        task_queue="default",
        workflows=[OrderWorkflow],
        activities=[
            activities.receive_order_activity,
            activities.validate_order_activity,
            activities.charge_payment_activity,
        ]
    )

    shipping_worker = Worker(
        client,
        task_queue="shipping-tq",
        workflows=[ShippingWorkflow],
        activities=[
            activities.prepare_package_activity,
            activities.dispatch_carrier_activity,
            activities.ship_order_activity,
        ]
    )

    logger.info("Workers started. Running indefinitely.")
    await asyncio.gather(
        default_worker.run(),
        shipping_worker.run()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopping workers.")

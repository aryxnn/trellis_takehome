import unittest
import asyncio
from unittest.mock import AsyncMock, patch
import db
import activities
from workflows import OrderWorkflow

class TestResilientLifecycle(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        db.DB_PATH = "test_orders.db"
        await db.init_db()

    async def asyncTearDown(self):
        import os
        if os.path.exists("test_orders.db"):
            os.remove("test_orders.db")

    async def test_database_idempotency(self):
        order_id = "TEST-ORD-100"
        payment_id = "TEST-PAY-100"
        
        await db.create_order(order_id, "RECEIVED", {})
        
        res1 = await db.create_payment_idempotent(payment_id, order_id, 50.0, "SUCCESS")
        self.assertEqual(res1["status"], "SUCCESS")
        
        res2 = await db.create_payment_idempotent(payment_id, order_id, 50.0, "SUCCESS")
        self.assertEqual(res2["payment_id"], payment_id)

    @patch("activities.flaky_call", new_callable=AsyncMock)
    async def test_activities_direct(self, mock_flaky):
        mock_flaky.return_value = None
        
        order_id = "TEST-ORD-200"
        res = await activities.order_received(order_id)
        self.assertEqual(res["order_id"], order_id)
        
        order = await db.get_order(order_id)
        self.assertIsNotNone(order)
        self.assertEqual(order["state"], "RECEIVED")

    @patch("temporalio.workflow.execute_activity")
    @patch("temporalio.workflow.execute_child_workflow")
    async def test_workflows_mocked(self, mock_child_wf, mock_exec_act):
        mock_exec_act.side_effect = lambda act, *args, **kwargs: {
            activities.receive_order_activity: {"order_id": "TEST-ORD-300", "items": [{"qty": 1}]},
            activities.validate_order_activity: True,
            activities.charge_payment_activity: {"status": "charged", "amount": 50.0},
        }.get(act, "Mocked Result")

        wf = OrderWorkflow()
        wf._order_id = "TEST-ORD-300"
        wf.ApproveOrder()

        mock_child_wf.return_value = "Shipped"

        with patch("temporalio.workflow.wait_condition", new_callable=AsyncMock) as mock_wait:
            res = await wf.run("TEST-ORD-300", "TEST-PAY-300")
            self.assertEqual(res["status"], "completed")
            self.assertEqual(wf._current_step, "COMPLETED")

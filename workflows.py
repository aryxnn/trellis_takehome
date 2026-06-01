from datetime import timedelta
import asyncio
import logging
from typing import Dict, Any, Optional
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    import activities
    import db

@workflow.defn
class ShippingWorkflow:
    @workflow.run
    async def run(self, order: Dict[str, Any]) -> str:
        try:
            package_status = await workflow.execute_activity(
                activities.prepare_package_activity,
                order,
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=0.5),
                    backoff_coefficient=1.5,
                    maximum_attempts=3
                )
            )
        except Exception:
            raise

        try:
            dispatch_status = await workflow.execute_activity(
                activities.dispatch_carrier_activity,
                order,
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=0.5),
                    backoff_coefficient=1.5,
                    maximum_attempts=3
                )
            )
        except Exception as e:
            parent_info = workflow.info()
            if parent_info.parent:
                try:
                    await workflow.signal_external_workflow(
                        "DispatchFailed",
                        f"Carrier dispatch failed: {str(e)}",
                        workflow_id=parent_info.parent.workflow_id,
                        run_id=parent_info.parent.run_id
                    )
                except Exception as sig_err:
                    workflow.logger.error(f"Failed to signal parent: {sig_err}")
            raise RuntimeError(f"Carrier dispatch failed: {str(e)}")

        ship_status = await workflow.execute_activity(
            activities.ship_order_activity,
            order,
            start_to_close_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=0.5),
                backoff_coefficient=1.5,
                maximum_attempts=3
            )
        )
        return ship_status


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self._order_id: str = ""
        self._current_step: str = "CREATED"
        self._approved: bool = False
        self._cancelled: bool = False
        self._updated_address: Optional[Dict[str, Any]] = None
        self._recent_error: Optional[str] = None
        self._shipping_retries: int = 0
        self._dispatch_failed_reason: Optional[str] = None

    @workflow.run
    async def run(self, order_id: str, payment_id: str) -> Dict[str, Any]:
        self._order_id = order_id
        self._current_step = "RECEIVING"

        try:
            order_data = await workflow.execute_activity(
                activities.receive_order_activity,
                order_id,
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=0.5),
                    backoff_coefficient=1.5,
                    maximum_attempts=5
                )
            )
        except Exception as e:
            self._recent_error = f"ReceiveOrder failed: {str(e)}"
            self._current_step = "FAILED"
            raise

        self._current_step = "VALIDATING"
        try:
            await workflow.execute_activity(
                activities.validate_order_activity,
                order_data,
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=0.5),
                    backoff_coefficient=1.5,
                    maximum_attempts=5
                )
            )
        except Exception as e:
            self._recent_error = f"ValidateOrder failed: {str(e)}"
            self._current_step = "FAILED"
            raise

        self._current_step = "PENDING_MANUAL_REVIEW"
        try:
            await workflow.wait_condition(
                lambda: self._approved or self._cancelled,
                timeout=8.0
            )
        except asyncio.TimeoutError:
            self._recent_error = "Manual review timed out after 8s"
            self._current_step = "EXPIRED"
            raise TimeoutError("Approval window expired")

        if self._cancelled:
            self._current_step = "CANCELLED"
            await workflow.execute_activity(
                activities.ship_order_activity,
                payload={"order_id": order_id},
                start_to_close_timeout=timedelta(seconds=1)
            )
            return {"status": "cancelled", "order_id": order_id}

        if self._updated_address:
            order_data["address"] = self._updated_address

        self._current_step = "CHARGING_PAYMENT"
        try:
            payment_res = await workflow.execute_activity(
                activities.charge_payment_activity,
                {"order": order_data, "payment_id": payment_id},
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=0.5),
                    backoff_coefficient=1.5,
                    maximum_attempts=5
                )
            )
        except Exception as e:
            self._recent_error = f"ChargePayment failed: {str(e)}"
            self._current_step = "FAILED"
            raise

        self._current_step = "SHIPPING"
        shipping_complete = False
        
        while not shipping_complete:
            if self._cancelled:
                self._current_step = "CANCELLED"
                return {"status": "cancelled", "order_id": order_id}

            try:
                if self._updated_address:
                    order_data["address"] = self._updated_address

                await workflow.execute_child_workflow(
                    ShippingWorkflow.run,
                    order_data,
                    id=f"{order_id}-shipping",
                    task_queue="shipping-tq",
                )
                shipping_complete = True
            except Exception as e:
                self._shipping_retries += 1
                if self._dispatch_failed_reason:
                    workflow.logger.info(
                        f"Shipping attempt {self._shipping_retries} failed: {self._dispatch_failed_reason}. Retrying..."
                    )
                    self._dispatch_failed_reason = None
                    await workflow.sleep(0.5)
                else:
                    self._recent_error = f"Shipping child workflow failed: {str(e)}"
                    self._current_step = "FAILED"
                    raise

        self._current_step = "COMPLETED"
        return {
            "status": "completed",
            "order_id": order_id,
            "payment": payment_res,
            "shipping_retries": self._shipping_retries
        }

    @workflow.signal
    def ApproveOrder(self) -> None:
        if self._current_step == "PENDING_MANUAL_REVIEW":
            self._approved = True

    @workflow.signal
    def CancelOrder(self) -> None:
        self._cancelled = True

    @workflow.signal
    def UpdateAddress(self, address: Dict[str, Any]) -> None:
        self._updated_address = address

    @workflow.signal
    def DispatchFailed(self, reason: str) -> None:
        self._dispatch_failed_reason = reason

    @workflow.query
    def status(self) -> Dict[str, Any]:
        return {
            "order_id": self._order_id,
            "current_step": self._current_step,
            "approved": self._approved,
            "cancelled": self._cancelled,
            "updated_address": self._updated_address,
            "recent_error": self._recent_error,
            "shipping_retries": self._shipping_retries,
            "dispatch_failed_reason": self._dispatch_failed_reason
        }

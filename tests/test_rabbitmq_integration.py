import os
import time
from uuid import uuid4

import aio_pika
import pytest
from aio_pika import ExchangeType, Message

pytestmark = pytest.mark.integration


async def _get_message(queue, timeout_seconds: float = 5.0):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        message = await queue.get(timeout=0.5, fail=False)
        if message is not None:
            return message
    pytest.fail(f"Timed out waiting for message in {queue.name}")


async def test_rabbitmq_retry_ttl_and_dlq_flow() -> None:
    if os.getenv("RUN_RABBITMQ_INTEGRATION") != "1":
        pytest.skip("Set RUN_RABBITMQ_INTEGRATION=1 to run RabbitMQ integration tests")

    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    suffix = uuid4().hex
    main_exchange_name = f"it.payments.{suffix}"
    retry_exchange_name = f"it.payments.retry.{suffix}"
    dlx_name = f"it.payments.dlx.{suffix}"
    main_queue_name = f"it.payments.new.{suffix}"
    retry_queue_name = f"it.payments.retry.{suffix}"
    dlq_name = f"it.payments.dlq.{suffix}"
    main_routing_key = "payments.new"
    retry_routing_key = "payments.retry"
    dlq_routing_key = "payments.dlq"

    connection = await aio_pika.connect_robust(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        main_exchange = await channel.declare_exchange(
            main_exchange_name,
            ExchangeType.DIRECT,
            auto_delete=True,
        )
        retry_exchange = await channel.declare_exchange(
            retry_exchange_name,
            ExchangeType.DIRECT,
            auto_delete=True,
        )
        dlx = await channel.declare_exchange(
            dlx_name,
            ExchangeType.DIRECT,
            auto_delete=True,
        )
        main_queue = await channel.declare_queue(
            main_queue_name,
            auto_delete=True,
            arguments={
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dlq_routing_key,
            },
        )
        retry_queue = await channel.declare_queue(
            retry_queue_name,
            auto_delete=True,
            arguments={
                "x-dead-letter-exchange": main_exchange_name,
                "x-dead-letter-routing-key": main_routing_key,
            },
        )
        dlq = await channel.declare_queue(dlq_name, auto_delete=True)

        await main_queue.bind(main_exchange, routing_key=main_routing_key)
        await retry_queue.bind(retry_exchange, routing_key=retry_routing_key)
        await dlq.bind(dlx, routing_key=dlq_routing_key)

        try:
            await main_exchange.publish(Message(b"first-attempt"), routing_key=main_routing_key)
            first_attempt = await _get_message(main_queue)
            assert first_attempt.body == b"first-attempt"
            await first_attempt.ack()

            await retry_exchange.publish(
                Message(b"retry-attempt", expiration=0.2),
                routing_key=retry_routing_key,
            )
            retried = await _get_message(main_queue)
            assert retried.body == b"retry-attempt"
            await retried.reject(requeue=False)

            dead_lettered = await _get_message(dlq)
            assert dead_lettered.body == b"retry-attempt"
            await dead_lettered.ack()
        finally:
            await main_queue.delete(if_unused=False, if_empty=False)
            await retry_queue.delete(if_unused=False, if_empty=False)
            await dlq.delete(if_unused=False, if_empty=False)
            await main_exchange.delete(if_unused=False)
            await retry_exchange.delete(if_unused=False)
            await dlx.delete(if_unused=False)

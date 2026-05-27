from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import settings

PAYMENTS_EXCHANGE_NAME = "payments"
PAYMENTS_NEW_QUEUE_NAME = "payments.new"
PAYMENTS_NEW_ROUTING_KEY = "payments.new"

PAYMENTS_RETRY_EXCHANGE_NAME = "payments.retry"
PAYMENTS_RETRY_QUEUE_NAME = "payments.retry"
PAYMENTS_RETRY_ROUTING_KEY = "payments.retry"

PAYMENTS_DLX_NAME = "payments.dlx"
PAYMENTS_DLQ_QUEUE_NAME = "payments.dlq"
PAYMENTS_DLQ_ROUTING_KEY = "payments.dlq"

PAYMENTS_EXCHANGE = RabbitExchange(PAYMENTS_EXCHANGE_NAME, durable=True)
PAYMENTS_RETRY_EXCHANGE = RabbitExchange(PAYMENTS_RETRY_EXCHANGE_NAME, durable=True)
PAYMENTS_DLX = RabbitExchange(PAYMENTS_DLX_NAME, durable=True)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    PAYMENTS_NEW_QUEUE_NAME,
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": PAYMENTS_DLX_NAME,
        "x-dead-letter-routing-key": PAYMENTS_DLQ_ROUTING_KEY,
    },
)
PAYMENTS_RETRY_QUEUE = RabbitQueue(
    PAYMENTS_RETRY_QUEUE_NAME,
    durable=True,
    routing_key=PAYMENTS_RETRY_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
        "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
    },
)
PAYMENTS_DLQ_QUEUE = RabbitQueue(
    PAYMENTS_DLQ_QUEUE_NAME,
    durable=True,
    routing_key=PAYMENTS_DLQ_ROUTING_KEY,
)

broker = RabbitBroker(settings.rabbitmq_url)


async def declare_rabbitmq_topology() -> None:
    """Declare durable RabbitMQ topology.

    RabbitMQ declarations and bindings are idempotent when called with the same
    parameters, so both API and consumer processes can safely call this on
    startup.
    """
    payments_exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    payments_queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
    await payments_queue.bind(
        payments_exchange,
        routing_key=PAYMENTS_NEW_QUEUE.routing(),
        arguments=PAYMENTS_NEW_QUEUE.bind_arguments,
        timeout=PAYMENTS_NEW_QUEUE.timeout,
        robust=PAYMENTS_NEW_QUEUE.robust,
    )

    retry_exchange = await broker.declare_exchange(PAYMENTS_RETRY_EXCHANGE)
    retry_queue = await broker.declare_queue(PAYMENTS_RETRY_QUEUE)
    await retry_queue.bind(
        retry_exchange,
        routing_key=PAYMENTS_RETRY_QUEUE.routing(),
        arguments=PAYMENTS_RETRY_QUEUE.bind_arguments,
        timeout=PAYMENTS_RETRY_QUEUE.timeout,
        robust=PAYMENTS_RETRY_QUEUE.robust,
    )

    dlx = await broker.declare_exchange(PAYMENTS_DLX)
    dlq = await broker.declare_queue(PAYMENTS_DLQ_QUEUE)
    await dlq.bind(
        dlx,
        routing_key=PAYMENTS_DLQ_QUEUE.routing(),
        arguments=PAYMENTS_DLQ_QUEUE.bind_arguments,
        timeout=PAYMENTS_DLQ_QUEUE.timeout,
        robust=PAYMENTS_DLQ_QUEUE.robust,
    )

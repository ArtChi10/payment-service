from faststream.rabbit import RabbitBroker

from app.core.config import settings

PAYMENTS_NEW_ROUTING_KEY = "payments.new"
PAYMENTS_DLQ_ROUTING_KEY = "payments.dlq"

broker = RabbitBroker(settings.rabbitmq_url)

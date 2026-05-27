from faststream import FastStream

from app.core.logging import configure_logging
from app.messaging import handlers as _handlers  # noqa: F401
from app.messaging.broker import broker, declare_rabbitmq_topology

configure_logging()

app = FastStream(broker, after_startup=[declare_rabbitmq_topology])

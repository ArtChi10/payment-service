from faststream import FastStream

from app.core.logging import configure_logging
from app.messaging import handlers as _handlers  # noqa: F401
from app.messaging.broker import broker

configure_logging()

app = FastStream(broker)

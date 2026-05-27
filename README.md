# Payment Service

Асинхронный сервис процессинга платежей по тестовому заданию: FastAPI принимает платеж,
outbox гарантирует публикацию события в RabbitMQ, consumer обрабатывает платеж, обновляет
статус и отправляет webhook.

## Стек

- FastAPI + Pydantic v2
- SQLAlchemy 2.0 async + PostgreSQL
- RabbitMQ + FastStream
- Alembic
- Docker Compose

## Запуск

```bash
docker compose up --build
```

API будет доступен на `http://localhost:8000`, RabbitMQ management UI на
`http://localhost:15672` (`guest` / `guest`).

`/health` — простой liveness endpoint. `/ready` проверяет готовность зависимостей:
соединение с PostgreSQL и подключение к RabbitMQ.

## API

Создать платеж:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: change-me" \
  -H "Idempotency-Key: order-10001" \
  -d '{
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Order 10001",
    "metadata": {"customer_id": "42"},
    "webhook_url": "https://example.com/webhooks/payments"
  }'
```

Ответ: `202 Accepted`

```json
{
  "payment_id": "4e2a3e10-0d36-4d76-bb7f-d85e1de3275a",
  "status": "pending",
  "created_at": "2026-05-27T10:00:00Z"
}
```

Получить платеж:

```bash
curl http://localhost:8000/api/v1/payments/4e2a3e10-0d36-4d76-bb7f-d85e1de3275a \
  -H "X-API-Key: change-me"
```

Ответ содержит платежный статус и поля webhook delivery: `webhook_status`,
`webhook_attempts`, `webhook_delivered_at`, `webhook_last_error`.

## Поток обработки

1. `POST /api/v1/payments` требует `X-API-Key` и `Idempotency-Key`.
2. В одной транзакции создаются `payments.status=pending` и запись `outbox`.
3. Фоновый outbox publisher короткой транзакцией выбирает `pending`/retryable `failed`
   события, публикует их в RabbitMQ вне DB-транзакции и отдельной короткой транзакцией
   помечает успешные события как `published`.
4. Consumer получает `payments.new` и короткой атомарной DB-операцией переводит платеж
   из `pending` в `processing`. Duplicate-события для того же платежа не получают claim
   и не вызывают gateway повторно. Если старый `processing` claim протух по lease timeout,
   следующее сообщение может забрать его заново.
5. Consumer эмулирует gateway с задержкой 2-5 секунд и шансом успеха 90%, затем отдельной
   короткой транзакцией фиксирует `succeeded` или `failed` и `processed_at`.
6. Consumer атомарно claim-ит webhook delivery через `webhook_status=sending`; stale
   `sending` claim тоже может быть восстановлен повторным сообщением.
7. Consumer отправляет webhook через `WebhookService.send_with_retry`: HTTP-доставка имеет
   собственные 3 попытки с экспоненциальной задержкой.
8. Если gateway processing или webhook retry окончательно падает, ошибка попадает в общий
   RabbitMQ message retry flow.
9. После 3 неудачной попытки обработки сообщения consumer отклоняет исходное сообщение без
   requeue, и RabbitMQ маршрутизирует его в DLQ через dead-letter exchange.

## RabbitMQ topology

API и consumer оба объявляют durable topology на старте. Это сделано намеренно:
API запускает outbox publisher и может публиковать события до старта consumer, поэтому
exchange, queues и bindings должны существовать уже в API-процессе. Повторное объявление
безопасно, потому что RabbitMQ declarations идемпотентны при одинаковых параметрах.

- main exchange: `payments`
- main queue: `payments.new`
- main routing key: `payments.new`
- retry exchange: `payments.retry`
- retry queue: `payments.retry`
- retry routing key: `payments.retry`
- dead-letter exchange: `payments.dlx`
- DLQ queue: `payments.dlq`
- DLQ routing key: `payments.dlq`

Очередь `payments.new` объявлена с аргументами `x-dead-letter-exchange=payments.dlx`
и `x-dead-letter-routing-key=payments.dlq`. При промежуточной ошибке handler публикует
следующую попытку в `payments.retry`; retry queue после expiration dead-letter-ит сообщение
обратно в exchange `payments` с routing key `payments.new`. Если обработка падает на 3-й
попытке, handler делает `reject(requeue=False)`, после чего RabbitMQ перекладывает сообщение
в durable очередь `payments.dlq`.

## Outbox guarantees

Outbox реализован как at-least-once delivery:

- событие создается в той же DB-транзакции, что и платеж;
- publish в RabbitMQ выполняется вне DB-транзакции, поэтому row lock не держится во время сетевого I/O;
- после успешного publish событие отдельной транзакцией помечается `published`;
- если publish падает, `attempts` увеличивается, а событие остается в статусе `failed`;
- `pending` и `failed` события с `attempts < 3` повторно подхватываются publisher'ом;
- `failed` события с исчерпанными attempts остаются в БД и требуют ручного расследования/alerting.

Гарантия не является exactly-once. Если publish прошел успешно, но сервис упал до отметки
`published`, событие может быть опубликовано повторно. Consumer поэтому идемпотентен на
доменном уровне: gateway processing claim-ится атомарно, а webhook delivery имеет отдельный
статус и idempotency key.

## Claim leases and recovery

Внутренние состояния `processing` и `sending` не являются вечными lock'ами. При claim сервис
записывает timestamp:

- `processing_started_at` и `processing_attempts` для gateway processing;
- `webhook_sending_started_at` для webhook delivery.

Если consumer падает после claim, но до финального статуса, повторное RabbitMQ-сообщение
сможет восстановить работу после lease timeout. Значения по умолчанию:
`payment_processing_lease_seconds=60` и `webhook_delivery_lease_seconds=60`.

Активный non-expired claim не приводит к повторному внешнему вызову. Stale `processing`
может быть снова переведен в `processing` атомарным `UPDATE ... WHERE`, после чего gateway
будет вызван заново. Stale `sending` может быть снова claim-нут и повторить webhook delivery.
Timestamps не очищаются после финального статуса и остаются как audit-информация.

## Webhook retry

Webhook delivery отделен от RabbitMQ message retry. `WebhookService.send_with_retry`
выполняет до 3 HTTP-попыток и использует экспоненциальную задержку `1, 2` секунды между
ними. Неуспешные попытки логируются. Если webhook не доставлен после всех попыток,
`WebhookDeliveryError` пробрасывается наружу, и consumer передает ошибку в общий
RabbitMQ retry/DLQ flow.

Состояние webhook delivery хранится в `payments`:

- `webhook_status`: `pending`, `sending`, `delivered`, `failed`;
- `webhook_attempts`: суммарное число HTTP-попыток webhook delivery;
- `webhook_delivered_at`: время успешной доставки;
- `webhook_last_error`: последняя финальная ошибка доставки.

Webhook payload содержит стабильный `delivery_id` вида `payment:<payment_id>:webhook`.
Получатель webhook может дедуплицировать входящие уведомления по этому ключу. Если
`webhook_status=delivered`, повторное `payments.new` завершается без HTTP-вызова. Если
доставка упала, consumer сохраняет `failed`, attempts/error и пробрасывает ошибку в
RabbitMQ retry flow; следующая message attempt может снова claim-ить delivery.

Текущая семантика: webhook retry выполняется внутри каждой попытки обработки сообщения.
Так как RabbitMQ message retry тоже делает до 3 попыток обработки, худший случай для
недоступного webhook — до 9 HTTP-запросов на один платеж. Это осознанный компромисс
текущей версии; production-вариант обычно выносит webhook delivery в отдельную outbox/queue
с собственным budget и дедупликацией.

Layered retry оставлен намеренно: HTTP retry закрывает короткие сетевые сбои webhook endpoint,
а RabbitMQ message retry закрывает ошибки обработки, маршрутизации и окончательные падения
webhook delivery.

## Идемпотентность

`payments.idempotency_key` уникален. Повторный `POST` с тем же `Idempotency-Key`
возвращает уже созданный платеж и не создает второе событие outbox.

Consumer защищает gateway от duplicate `payments.new`: только один обработчик может
атомарно перевести платеж из `pending` в `processing`. Остальные duplicate-события видят,
что платеж уже claim-нут или обработан, и не вызывают внешний gateway. Успешно доставленный
webhook повторно не отправляется; pending/failed delivery claim-ится отдельно через
`webhook_status=sending`.

`processing` — внутренний статус consumer claim-а. Во внешнем `GET /api/v1/payments/{id}`
он скрывается как `pending`, чтобы публичный контракт оставался `pending`, `succeeded`,
`failed`.

При конкурентном создании второй запрос может столкнуться с unique constraint и после
rollback повторно прочитать уже созданный платеж. Для production-grade PostgreSQL лучше
заменить это на атомарный `INSERT ... ON CONFLICT`/upsert flow, чтобы не зависеть от
короткого retry-read окна после `IntegrityError`.

## Миграции

```bash
alembic upgrade head
```

## Тесты

```bash
pip install -e ".[dev]"
pytest
```

## Ruff

```bash
ruff check .
ruff format .
```

## Quality checks

Перед сдачей полезно прогнать:

```bash
ruff check .
ruff format --check .
pytest
docker compose config
```

Docker runtime check:

```bash
docker compose up --build -d
curl http://localhost:8000/health
curl http://localhost:8000/ready
docker compose ps
```

RabbitMQ/DLQ integration check требует запущенный RabbitMQ. При поднятом `docker compose`
можно выполнить:

```bash
$env:RUN_RABBITMQ_INTEGRATION = "1"
pytest -m integration
```

Обычный `pytest` не требует Docker: integration test пропускается без
`RUN_RABBITMQ_INTEGRATION=1`.

Manual integration checklist:

1. Запустить `docker compose up --build`.
2. Проверить API: `curl http://localhost:8000/health`.
3. Проверить readiness: `curl http://localhost:8000/ready`.
4. Создать платеж через пример `POST /api/v1/payments`.
5. Открыть RabbitMQ UI: `http://localhost:15672` (`guest` / `guest`).
6. Проверить, что существуют `payments.new`, `payments.retry`, `payments.dlq`.
7. Для DLQ-сценария искусственно сломать обработку или webhook и убедиться, что после
   исчерпания message attempts сообщение попадает в `payments.dlq`.

RabbitMQ/DLQ integration test есть в `tests/test_rabbitmq_integration.py`, но не
запускается по умолчанию, потому что требует внешний RabbitMQ. Полный сценарий с API,
PostgreSQL, RabbitMQ и consumer оставлен как manual Docker-check выше.

## Operational notes

Outbox events, которые исчерпали publish attempts, остаются в статусе `failed` для ручного
расследования:

```sql
SELECT id, event_type, routing_key, attempts, created_at
FROM outbox
WHERE status = 'failed' AND attempts >= 3
ORDER BY created_at;
```

Такие события нужно расследовать по логам и состоянию RabbitMQ. Повторная отправка может
быть оформлена отдельной maintenance-командой в будущем; сейчас автоматический retry
ограничен, чтобы бесконечно не гонять неисправные события.

Ручной recovery-сценарий: проверить причину ошибки, убедиться, что RabbitMQ доступен, затем
вернуть конкретное событие в retryable состояние SQL-операцией наподобие:

```sql
UPDATE outbox
SET status = 'failed', attempts = 0
WHERE id = '<event-id>';
```

После этого outbox publisher снова подхватит событие.

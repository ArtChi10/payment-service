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

## Поток обработки

1. `POST /api/v1/payments` требует `X-API-Key` и `Idempotency-Key`.
2. В одной транзакции создаются `payments.status=pending` и запись `outbox`.
3. Фоновый outbox publisher короткой транзакцией выбирает `pending`/retryable `failed`
   события, публикует их в RabbitMQ вне DB-транзакции и отдельной короткой транзакцией
   помечает успешные события как `published`.
4. Consumer получает `payments.new`, эмулирует gateway с задержкой 2-5 секунд и шансом успеха 90%.
5. Consumer обновляет статус платежа на `succeeded` или `failed`.
6. Consumer отправляет webhook через `WebhookService.send_with_retry`: HTTP-доставка имеет
   собственные 3 попытки с экспоненциальной задержкой.
7. Если gateway processing или webhook retry окончательно падает, ошибка попадает в общий
   RabbitMQ message retry flow.
8. После 3 неудачной попытки обработки сообщения consumer отклоняет исходное сообщение без
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
`published`, событие может быть опубликовано повторно. Consumer поэтому должен быть
идемпотентным: если `payment.processed_at` уже заполнен, gateway processing повторно не
запускается. Webhook на повторном `payments.new` сейчас может отправиться повторно; надежный
webhook deduplication может быть улучшен отдельной следующей задачей.

## Webhook retry

Webhook delivery отделен от RabbitMQ message retry. `WebhookService.send_with_retry`
выполняет до 3 HTTP-попыток и использует экспоненциальную задержку `1, 2` секунды между
ними. Неуспешные попытки логируются. Если webhook не доставлен после всех попыток,
`WebhookDeliveryError` пробрасывается наружу, и consumer передает ошибку в общий
RabbitMQ retry/DLQ flow.

Текущая семантика: webhook retry выполняется внутри каждой попытки обработки сообщения.
Так как RabbitMQ message retry тоже делает до 3 попыток обработки, худший случай для
недоступного webhook — до 9 HTTP-запросов на один платеж. Это осознанный компромисс
текущей версии; production-вариант обычно выносит webhook delivery в отдельную outbox/queue
с собственным budget и дедупликацией.

## Идемпотентность

`payments.idempotency_key` уникален. Повторный `POST` с тем же `Idempotency-Key`
возвращает уже созданный платеж и не создает второе событие outbox.

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

Опциональная интеграционная проверка с PostgreSQL, RabbitMQ, API и consumer:

```bash
docker compose up --build
```

Manual integration checklist:

1. Запустить `docker compose up --build`.
2. Проверить API: `curl http://localhost:8000/health`.
3. Создать платеж через пример `POST /api/v1/payments`.
4. Открыть RabbitMQ UI: `http://localhost:15672` (`guest` / `guest`).
5. Проверить, что существуют `payments.new`, `payments.retry`, `payments.dlq`.
6. Для DLQ-сценария искусственно сломать обработку или webhook и убедиться, что после
   исчерпания message attempts сообщение попадает в `payments.dlq`.

Автоматический integration test с настоящими PostgreSQL и RabbitMQ в этом репозитории не
запускается по умолчанию, потому что требует внешних Docker-сервисов; сценарий выше
оставлен как manual/optional проверка.

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

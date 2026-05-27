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
3. Фоновый outbox publisher читает неопубликованные события и публикует `payments.new`.
4. Consumer получает `payments.new`, эмулирует gateway с задержкой 2-5 секунд и шансом успеха 90%.
5. Consumer обновляет статус платежа на `succeeded` или `failed`.
6. Consumer отправляет webhook. Ошибки обрабатываются retry до 3 попыток с задержкой `1, 2, 4` секунды.
7. После 3 неудачных попыток сообщение публикуется в `payments.dlq`.

## Идемпотентность

`payments.idempotency_key` уникален. Повторный `POST` с тем же `Idempotency-Key`
возвращает уже созданный платеж и не создает второе событие outbox.

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

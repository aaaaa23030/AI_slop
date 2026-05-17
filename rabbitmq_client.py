from __future__ import annotations

import json
import os
import time
from typing import Any

import pika

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://kanban:kanban@localhost:5672/")
EXCHANGE = "kanban"
INCOMING_QUEUE = "incoming_tasks"
INCOMING_DEAD_QUEUE = "incoming_tasks_dead"
AUTOMATION_QUEUE = "automation_events"
AUTOMATION_DEAD_QUEUE = "automation_events_dead"
DLX = "kanban.dead"


def get_connection(retries: int = 30, delay: float = 2.0) -> pika.BlockingConnection:
    """Connect to RabbitMQ with retries because Docker services start at different speeds."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            return pika.BlockingConnection(params)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(f"[RabbitMQ] connection attempt {attempt}/{retries} failed: {exc}")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to RabbitMQ: {last_error}")


def setup_rabbitmq() -> None:
    connection = get_connection()
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=DLX, exchange_type="topic", durable=True)

    # Main queue. If worker rejects a message, RabbitMQ moves it to DLX.
    channel.queue_declare(
        queue=INCOMING_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": "incoming.task.failed",
        },
    )
    channel.queue_bind(queue=INCOMING_QUEUE, exchange=EXCHANGE, routing_key="incoming.task.created")

    # Dead-letter queue: incoming messages that could not be processed.
    channel.queue_declare(queue=INCOMING_DEAD_QUEUE, durable=True)
    channel.queue_bind(queue=INCOMING_DEAD_QUEUE, exchange=DLX, routing_key="incoming.task.failed")

    # Automation queue: all task-domain events are consumed by automation_worker.py.
    channel.queue_declare(
        queue=AUTOMATION_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": "automation.event.failed",
        },
    )
    for routing_key in ["task.created", "task.updated", "task.moved", "incoming.task.processed"]:
        channel.queue_bind(queue=AUTOMATION_QUEUE, exchange=EXCHANGE, routing_key=routing_key)

    channel.queue_declare(queue=AUTOMATION_DEAD_QUEUE, durable=True)
    channel.queue_bind(queue=AUTOMATION_DEAD_QUEUE, exchange=DLX, routing_key="automation.event.failed")

    connection.close()


def publish(routing_key: str, payload: dict[str, Any], message_id: str | None = None, exchange: str = EXCHANGE) -> None:
    setup_rabbitmq()
    connection = get_connection()
    channel = connection.channel()
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent message
            content_type="application/json",
            message_id=message_id,
        ),
    )
    connection.close()

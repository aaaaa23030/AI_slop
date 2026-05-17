from __future__ import annotations

import json

from app import (
    Event,
    ProcessedMessage,
    Task,
    app,
    check_rules,
    db,
    emit_state,
    log_event,
    send_notification,
)
from rabbitmq_client import AUTOMATION_QUEUE, get_connection, setup_rabbitmq


def on_message(channel, method, properties, body: bytes) -> None:
    payload = json.loads(body.decode("utf-8"))
    message_id = payload.get("message_id") or properties.message_id or f"automation:{method.delivery_tag}"
    trigger = payload.get("trigger")
    task_id = payload.get("task_id")

    with app.app_context():
        try:
            if ProcessedMessage.query.filter_by(message_id=message_id).first():
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            task = Task.query.get(task_id) if task_id else None
            if not task:
                log_event("automation_event_skipped", None, {"reason": "task_not_found", "payload": payload})
                db.session.add(ProcessedMessage(message_id=message_id))
                db.session.commit()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            check_rules(trigger, task)
            db.session.add(ProcessedMessage(message_id=message_id))
            log_event("automation_event_processed", task.id, {"trigger": trigger, "message_id": message_id})
            db.session.commit()
            emit_state()
            print(f"[AutomationWorker] processed {message_id} trigger={trigger} task={task.id}")
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as exc:  # noqa: BLE001
            db.session.rollback()
            print(f"[AutomationWorker] failed {message_id}: {exc}")
            send_notification(f"⚠️ Ошибка автоматизации: {exc}", "warning")
            # requeue=False sends the message to automation_events_dead.
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


if __name__ == "__main__":
    print("[AutomationWorker] starting...")
    setup_rabbitmq()
    connection = get_connection()
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=AUTOMATION_QUEUE, on_message_callback=on_message, auto_ack=False)
    print("[AutomationWorker] waiting for task-domain events...")
    channel.start_consuming()

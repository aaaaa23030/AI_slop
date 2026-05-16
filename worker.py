from __future__ import annotations

from datetime import datetime
import json

import pika

from app import (
    Column,
    Event,
    IncomingTask,
    ProcessedMessage,
    PRIORITIES,
    Task,
    TeamMember,
    app,
    check_rules,
    db,
    emit_state,
    enrich_incoming_payload,
    find_column,
    find_team,
    find_user,
    get_default_column,
    normalize_tags,
    parse_deadline,
    send_notification,
    socketio,
)
from rabbitmq_client import INCOMING_QUEUE, get_connection, setup_rabbitmq


def log_system_event(event_type: str, task_id: int | None = None, payload: dict | None = None) -> None:
    db.session.add(Event(
        type=event_type,
        task_id=task_id,
        user_id=None,
        payload=json.dumps(payload or {}, ensure_ascii=False),
    ))


def validate_worker_payload(data: dict) -> tuple[bool, str]:
    title = str(data.get("title", "")).strip()
    if not title:
        return False, "title is required"

    priority = data.get("priority", "medium")
    if priority not in PRIORITIES:
        return False, "invalid priority"

    deadline = data.get("deadline")
    if deadline and parse_deadline(deadline) is None:
        return False, "deadline must be YYYY-MM-DD"

    return True, ""


def process_incoming_task(item: IncomingTask) -> None:
    data = json.loads(item.payload)
    data = enrich_incoming_payload(data)

    ok, error = validate_worker_payload(data)
    if not ok:
        item.status = "error"
        item.error = error
        item.processed_at = datetime.utcnow()
        log_system_event("incoming_task_error", None, {"incoming_id": item.id, "error": error})
        return

    external_id = (item.external_id or "").strip()
    if external_id:
        existing_external = IncomingTask.query.filter(
            IncomingTask.id != item.id,
            IncomingTask.source == item.source,
            IncomingTask.external_id == external_id,
            IncomingTask.status == "processed",
        ).first()
        if existing_external:
            item.status = "duplicate"
            item.task_id = existing_external.task_id
            item.error = "Duplicate by source + external_id"
            item.processed_at = datetime.utcnow()
            log_system_event("incoming_task_duplicate", item.task_id, {"incoming_id": item.id, "reason": "external_id"})
            return

    title = str(data.get("title", "")).strip()
    tags = normalize_tags(data.get("tags", ""))

    existing_task = Task.query.filter_by(title=title, tags=tags).first()
    if existing_task:
        item.status = "duplicate"
        item.task_id = existing_task.id
        item.error = "Duplicate by title + tags"
        item.processed_at = datetime.utcnow()
        log_system_event("incoming_task_duplicate", existing_task.id, {"incoming_id": item.id, "reason": "title_tags"})
        return

    column = find_column(data.get("column_id")) or find_column(data.get("column")) or get_default_column()
    if not column:
        item.status = "error"
        item.error = "No kanban columns exist"
        item.processed_at = datetime.utcnow()
        log_system_event("incoming_task_error", None, {"incoming_id": item.id, "error": item.error})
        return

    team = find_team(data.get("team_id")) or find_team(data.get("team"))
    assignee = find_user(data.get("assignee_id")) or find_user(data.get("assignee"))

    if team and assignee:
        is_member = TeamMember.query.filter_by(team_id=team.id, user_id=assignee.id).first()
        if not is_member:
            item.status = "error"
            item.error = "Assignee is not a member of selected team"
            item.processed_at = datetime.utcnow()
            log_system_event("incoming_task_error", None, {"incoming_id": item.id, "error": item.error})
            return

    task = Task(
        title=title,
        description=str(data.get("description", "")).strip(),
        priority=data.get("priority", "medium"),
        tags=tags,
        deadline=parse_deadline(data.get("deadline")),
        column_id=column.id,
        creator_id=None,
        team_id=team.id if team else None,
        assignee_id=assignee.id if assignee else None,
    )
    db.session.add(task)
    db.session.flush()

    item.status = "processed"
    item.task_id = task.id
    item.processed_at = datetime.utcnow()
    item.error = ""

    log_system_event("task_created_from_queue", task.id, {"incoming_id": item.id, "source": item.source, "external_id": external_id})

    # Automation rules are still executed as Python functions, but now they are triggered by a background worker event.
    check_rules("incoming_task", task)
    check_rules("task_created", task)

    send_notification(f'📥 Из RabbitMQ создана задача: "{task.title}"', "success")


def on_message(channel, method, properties, body: bytes) -> None:
    payload = json.loads(body.decode("utf-8"))
    message_id = payload["message_id"]
    incoming_task_id = payload["incoming_task_id"]

    with app.app_context():
        try:
            if ProcessedMessage.query.filter_by(message_id=message_id).first():
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            item = IncomingTask.query.get(incoming_task_id)
            if not item:
                # Nothing to process; acknowledge so the queue is not blocked forever.
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            process_incoming_task(item)
            db.session.add(ProcessedMessage(message_id=message_id))
            db.session.commit()
            emit_state()
            channel.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[Worker] processed {message_id}")

        except Exception as exc:  # noqa: BLE001
            db.session.rollback()
            print(f"[Worker] failed {message_id}: {exc}")
            # requeue=False sends message to dead-letter queue configured in rabbitmq_client.py
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


if __name__ == "__main__":
    print("[Worker] starting...")
    setup_rabbitmq()
    connection = get_connection()
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=INCOMING_QUEUE, on_message_callback=on_message, auto_ack=False)
    print("[Worker] waiting for messages from RabbitMQ...")
    channel.start_consuming()

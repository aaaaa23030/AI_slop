from __future__ import annotations

from datetime import datetime
import json
import time

from app import OutboxMessage, app, db
from rabbitmq_client import publish, setup_rabbitmq


def publish_pending_messages() -> int:
    count = 0
    with app.app_context():
        messages = (
            OutboxMessage.query
            .filter_by(status="pending")
            .order_by(OutboxMessage.id.asc())
            .limit(20)
            .all()
        )
        for msg in messages:
            try:
                payload = json.loads(msg.payload)
                publish(
                    routing_key=msg.routing_key,
                    payload=payload,
                    message_id=str(msg.id),
                    exchange=msg.exchange,
                )
                msg.status = "published"
                msg.published_at = datetime.utcnow()
                msg.error = ""
                count += 1
                print(f"[Outbox] published message #{msg.id} -> {msg.routing_key}")
            except Exception as exc:  # noqa: BLE001
                msg.attempts += 1
                msg.error = str(exc)[:500]
                if msg.attempts >= 5:
                    msg.status = "failed"
                print(f"[Outbox] failed message #{msg.id}: {exc}")
        db.session.commit()
    return count


if __name__ == "__main__":
    print("[Outbox] starting publisher...")
    setup_rabbitmq()
    while True:
        published = publish_pending_messages()
        if not published:
            time.sleep(1)

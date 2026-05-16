from __future__ import annotations

from datetime import date, datetime
from functools import wraps
import json
import os
import re
import shutil
import sqlite3
from typing import Any

from flask import Flask, jsonify, redirect, render_template, request, url_for
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from flask_socketio import SocketIO
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "hackathon-dev-secret-change-me")

# In Docker Compose DATABASE_URL points to PostgreSQL. Locally we can still fall back to SQLite.
database_url = os.environ.get("DATABASE_URL", "sqlite:///tasks.db")
# SQLAlchemy expects postgresql://, but some providers still expose postgres://.
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Redis message_queue lets background workers/outbox processes emit Socket.IO events too.
# Without REDIS_URL it still works locally as a single Flask process.
redis_url = os.environ.get("REDIS_URL")
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
    message_queue=redis_url if redis_url else None,
)
db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.login_message = "Сначала войдите в систему."

PRIORITIES = {"low", "medium", "high", "critical"}
SYSTEM_COLUMNS = [
    {"key": "todo", "name": "📋 To Do", "position": 1},
    {"key": "in_progress", "name": "🔄 In Progress", "position": 2},
    {"key": "done", "name": "✅ Done", "position": 3},
]


# ===================== MODELS =====================

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default="user", nullable=False)  # admin/user
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)


class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(80), unique=True, nullable=False)
    name = db.Column(db.String(160), nullable=False)
    description = db.Column(db.String(500), default="")
    created_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    creator = db.relationship("User", foreign_keys=[created_by], backref="created_teams", lazy=True)


class TeamMember(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey("team.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    role = db.Column(db.String(20), default="member", nullable=False)  # admin/member
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    team = db.relationship("Team", backref=db.backref("members", cascade="all, delete-orphan"), lazy=True)
    user = db.relationship("User", backref=db.backref("team_memberships", cascade="all, delete-orphan"), lazy=True)

    __table_args__ = (db.UniqueConstraint("team_id", "user_id", name="uq_team_member"),)


class Column(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(60), unique=True, nullable=False)
    name = db.Column(db.String(120), nullable=False)
    position = db.Column(db.Integer, default=0)
    is_default = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    tasks = db.relationship("Task", backref="column_ref", lazy=True)


class Task(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.String(1000), default="")
    priority = db.Column(db.String(20), default="medium")
    tags = db.Column(db.String(500), default="")  # comma-separated tags
    deadline = db.Column(db.Date, nullable=True)
    column_id = db.Column(db.Integer, db.ForeignKey("column.id"), nullable=False)
    creator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    team_id = db.Column(db.Integer, db.ForeignKey("team.id"), nullable=True)
    assignee_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    creator = db.relationship("User", foreign_keys=[creator_id], backref="created_tasks", lazy=True)
    assignee = db.relationship("User", foreign_keys=[assignee_id], backref="assigned_tasks", lazy=True)
    team_ref = db.relationship("Team", backref="tasks", lazy=True)


class Rule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    trigger = db.Column(db.String(50), nullable=False)  # task_created/task_updated/task_moved/task_deleted/incoming_task
    condition_field = db.Column(db.String(50), default="always")  # always/priority/column/tags/title/deadline
    condition_operator = db.Column(db.String(50), default="equals")  # equals/contains/not_equals
    condition_value = db.Column(db.String(200), default="")
    action_type = db.Column(db.String(50), nullable=False)  # move_column/send_notification/set_priority/add_tag
    action_value = db.Column(db.String(200), default="")
    enabled = db.Column(db.Boolean, default=True)
    created_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Event(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.String(80), nullable=False)
    task_id = db.Column(db.Integer, nullable=True)
    user_id = db.Column(db.Integer, nullable=True)
    payload = db.Column(db.Text, default="{}")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class IncomingTask(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source = db.Column(db.String(80), default="api", nullable=False)  # api/telegram/github/support/etc.
    external_id = db.Column(db.String(120), nullable=True)
    payload = db.Column(db.Text, nullable=False)
    status = db.Column(db.String(30), default="pending")  # pending/queued/processed/duplicate/error
    error = db.Column(db.String(500), default="")
    task_id = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    processed_at = db.Column(db.DateTime, nullable=True)


class OutboxMessage(db.Model):
    """Reliable bridge from SQL transaction to RabbitMQ.

    API writes IncomingTask + OutboxMessage in one DB commit.
    outbox_publisher.py later sends this message to RabbitMQ.
    """
    id = db.Column(db.Integer, primary_key=True)
    exchange = db.Column(db.String(120), default="kanban", nullable=False)
    routing_key = db.Column(db.String(120), nullable=False)
    payload = db.Column(db.Text, nullable=False)
    status = db.Column(db.String(30), default="pending")  # pending/published/failed
    attempts = db.Column(db.Integer, default=0)
    error = db.Column(db.String(500), default="")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    published_at = db.Column(db.DateTime, nullable=True)


class ProcessedMessage(db.Model):
    """Idempotency table: protects from repeated RabbitMQ deliveries."""
    id = db.Column(db.Integer, primary_key=True)
    message_id = db.Column(db.String(160), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


@login_manager.user_loader
def load_user(user_id: str) -> User | None:
    return User.query.get(int(user_id))


# ===================== INIT =====================

def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-zа-я0-9]+", "_", text, flags=re.IGNORECASE)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "column"


def ensure_team_member(team: Team, user: User, role: str = "member") -> None:
    member = TeamMember.query.filter_by(team_id=team.id, user_id=user.id).first()
    if member:
        member.role = role
        return
    db.session.add(TeamMember(team_id=team.id, user_id=user.id, role=role))


def seed_database() -> None:
    """Create default columns, demo users and demo team on first launch."""
    for col in SYSTEM_COLUMNS:
        if not Column.query.filter_by(key=col["key"]).first():
            db.session.add(Column(key=col["key"], name=col["name"], position=col["position"], is_default=True))

    if not User.query.filter_by(username="admin").first():
        admin = User(username="admin", email="admin@example.com", role="admin")
        admin.set_password("admin123")
        db.session.add(admin)

    if not User.query.filter_by(username="user").first():
        user = User(username="user", email="user@example.com", role="user")
        user.set_password("user123")
        db.session.add(user)

    db.session.flush()

    admin = User.query.filter_by(username="admin").first()
    user = User.query.filter_by(username="user").first()
    if admin and not Team.query.filter_by(key="victory_group").first():
        team = Team(
            key="victory_group",
            name="Victory Group",
            description="Демо-команда для хакатона",
            created_by=admin.id,
        )
        db.session.add(team)
        db.session.flush()
        ensure_team_member(team, admin, "admin")
        if user:
            ensure_team_member(team, user, "member")

    db.session.commit()



def get_sqlite_db_path() -> str | None:
    """Return actual SQLite file path for sqlite:///tasks.db.

    Flask-SQLAlchemy stores relative SQLite files inside the Flask instance
    folder, so sqlite:///tasks.db becomes instance/tasks.db.
    """
    uri = app.config.get("SQLALCHEMY_DATABASE_URI", "")
    if not uri.startswith("sqlite:///"):
        return None

    raw_path = uri.replace("sqlite:///", "", 1)
    if raw_path == ":memory:":
        return None

    if os.path.isabs(raw_path):
        return raw_path
    return os.path.join(app.instance_path, raw_path)


def backup_and_import_legacy_sqlite() -> list[dict[str, Any]]:
    """Detect the previous hackathon DB schema and reset it safely.

    The first MVP had a `task` table with a text field named `column`.
    The new version uses `column_id`, users, events, queue, etc. SQLAlchemy's
    `create_all()` does not alter existing tables, so the old DB causes a
    500 error after login. This function backs up the old DB and imports old
    tasks into the new schema.
    """
    db_path = get_sqlite_db_path()
    if not db_path or not os.path.exists(db_path):
        return []

    legacy_tasks: list[dict[str, Any]] = []

    try:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        tables = {
            row["name"]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "task" not in tables:
            connection.close()
            return []

        task_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(task)").fetchall()
        }

        new_schema_columns = {"column_id", "creator_id", "created_at", "updated_at"}
        if new_schema_columns.issubset(task_columns):
            connection.close()
            return []

        select_parts = [
            "id" if "id" in task_columns else "NULL AS id",
            "title" if "title" in task_columns else "'' AS title",
            "description" if "description" in task_columns else "'' AS description",
            "priority" if "priority" in task_columns else "'medium' AS priority",
            "tags" if "tags" in task_columns else "'' AS tags",
            "deadline" if "deadline" in task_columns else "'' AS deadline",
            "\"column\" AS column_key" if "column" in task_columns else "'todo' AS column_key",
        ]
        query = "SELECT " + ", ".join(select_parts) + " FROM task"
        for row in connection.execute(query).fetchall():
            legacy_tasks.append(dict(row))

        connection.close()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = db_path.replace(".db", f"_legacy_backup_{timestamp}.db")
        shutil.copy2(db_path, backup_path)
        os.remove(db_path)
        print(f"[DB] Old incompatible database backed up to: {backup_path}")

    except Exception as exc:  # noqa: BLE001
        print(f"[DB] Could not inspect/migrate existing SQLite DB: {exc}")
        return []

    return legacy_tasks


def import_legacy_tasks(legacy_tasks: list[dict[str, Any]]) -> None:
    if not legacy_tasks:
        return

    admin = User.query.filter_by(username="admin").first()
    default_column = get_default_column()
    imported = 0

    for old in legacy_tasks:
        title = str(old.get("title") or "").strip()
        if not title:
            continue

        priority = str(old.get("priority") or "medium").strip()
        if priority not in PRIORITIES:
            priority = "medium"

        column = find_column(old.get("column_key")) or default_column
        if not column:
            continue

        task = Task(
            title=title,
            description=str(old.get("description") or ""),
            priority=priority,
            tags=normalize_tags(str(old.get("tags") or "")),
            deadline=parse_deadline(str(old.get("deadline") or "")),
            column_id=column.id,
            creator_id=admin.id if admin else None,
        )
        db.session.add(task)
        db.session.flush()
        db.session.add(Event(
            type="legacy_task_imported",
            task_id=task.id,
            user_id=admin.id if admin else None,
            payload=json.dumps({"old_id": old.get("id"), "title": title}, ensure_ascii=False),
        ))
        imported += 1

    db.session.commit()
    print(f"[DB] Imported {imported} task(s) from old database backup.")


def migrate_sqlite_schema() -> None:
    """Tiny SQLite migration for hackathon use.

    SQLAlchemy create_all() creates missing tables, but it does not add new
    columns to existing tables. This function lets you replace only app.py/html
    while keeping the current instance/tasks.db from the previous version.
    """
    db_path = get_sqlite_db_path()
    if not db_path or not os.path.exists(db_path):
        return

    connection = sqlite3.connect(db_path)
    try:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "task" in tables:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(task)").fetchall()}
            if "team_id" not in columns:
                connection.execute("ALTER TABLE task ADD COLUMN team_id INTEGER")
                print("[DB] Added task.team_id")
            if "assignee_id" not in columns:
                connection.execute("ALTER TABLE task ADD COLUMN assignee_id INTEGER")
                print("[DB] Added task.assignee_id")
        connection.commit()
    finally:
        connection.close()


# ===================== HELPERS =====================

def admin_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != "admin":
            return jsonify({"status": "error", "message": "Нужны права администратора"}), 403
        return view_func(*args, **kwargs)
    return wrapper


def is_global_admin(user: User | None = None) -> bool:
    user = user or current_user
    return bool(user and user.is_authenticated and user.role == "admin")


def get_user_team_ids(user: User | None = None) -> list[int]:
    user = user or current_user
    if not user or not user.is_authenticated:
        return []
    return [m.team_id for m in TeamMember.query.filter_by(user_id=user.id).all()]


def is_team_member(team_id: int | None, user_id: int | None = None) -> bool:
    if not team_id or not user_id:
        return False
    return TeamMember.query.filter_by(team_id=team_id, user_id=user_id).first() is not None


def is_team_admin(team_id: int | None, user: User | None = None) -> bool:
    user = user or current_user
    if not team_id or not user or not user.is_authenticated:
        return False
    if is_global_admin(user):
        return True
    return TeamMember.query.filter_by(team_id=team_id, user_id=user.id, role="admin").first() is not None


def can_manage_team(team_id: int | None) -> bool:
    return is_team_admin(team_id)


def can_edit_task(task: Task) -> bool:
    if is_global_admin():
        return True
    if task.creator_id == current_user.id or task.assignee_id == current_user.id:
        return True
    if task.team_id and is_team_admin(task.team_id):
        return True
    # Public task without a team: creator can edit; global admins can edit above.
    return False


def can_view_task(task: Task) -> bool:
    if is_global_admin():
        return True
    if task.team_id is None:
        return True
    if task.creator_id == current_user.id or task.assignee_id == current_user.id:
        return True
    return is_team_member(task.team_id, current_user.id)


def find_team(value: Any) -> Team | None:
    if value is None or value == "":
        return None
    if isinstance(value, int) or str(value).isdigit():
        return Team.query.get(int(value))
    value_str = str(value).strip()
    return Team.query.filter((Team.key == value_str) | (Team.name == value_str)).first()


def find_user(value: Any) -> User | None:
    if value is None or value == "":
        return None
    if isinstance(value, int) or str(value).isdigit():
        return User.query.get(int(value))
    value_str = str(value).strip()
    return User.query.filter((User.username == value_str) | (User.email == value_str)).first()


def validate_team_assignment(team: Team | None, assignee: User | None, changing_assignment: bool = True) -> tuple[bool, str]:
    """Validate team/assignee rules.

    Global admins can assign anywhere. Team admins can assign tasks inside their
    team to any team member. Regular members can create/edit only their own
    assignment or leave the assignee empty.
    """
    if team:
        if not is_global_admin() and not is_team_member(team.id, current_user.id):
            return False, "Вы не состоите в этой команде"
        if assignee and not is_team_member(team.id, assignee.id):
            return False, "Исполнитель должен быть участником выбранной команды"
        if assignee and assignee.id != current_user.id and not is_team_admin(team.id):
            return False, "Назначать задачи другим может только админ команды"
        return True, ""

    if assignee and assignee.id != current_user.id and not is_global_admin():
        return False, "Без команды можно назначить задачу только себе"
    return True, ""


def get_visible_teams() -> list[Team]:
    if is_global_admin():
        return Team.query.order_by(Team.created_at.desc()).all()
    ids = get_user_team_ids()
    if not ids:
        return []
    return Team.query.filter(Team.id.in_(ids)).order_by(Team.created_at.desc()).all()


def get_visible_users() -> list[User]:
    if is_global_admin():
        return User.query.order_by(User.username.asc()).all()
    ids = set([current_user.id])
    team_ids = get_user_team_ids()
    if team_ids:
        for member in TeamMember.query.filter(TeamMember.team_id.in_(team_ids)).all():
            ids.add(member.user_id)
    return User.query.filter(User.id.in_(ids)).order_by(User.username.asc()).all()


def get_visible_tasks() -> list[Task]:
    query = Task.query
    if not is_global_admin():
        team_ids = get_user_team_ids()
        filters = [Task.team_id.is_(None), Task.creator_id == current_user.id, Task.assignee_id == current_user.id]
        if team_ids:
            filters.append(Task.team_id.in_(team_ids))
        query = query.filter(db.or_(*filters))
    return query.order_by(Task.created_at.desc()).all()


def parse_deadline(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def get_default_column() -> Column:
    return Column.query.order_by(Column.position.asc()).first()


def find_column(value: Any) -> Column | None:
    if value is None or value == "":
        return None
    if isinstance(value, int) or str(value).isdigit():
        return Column.query.get(int(value))
    value_str = str(value).strip()
    return Column.query.filter((Column.key == value_str) | (Column.name == value_str)).first()


def normalize_tags(tags: str | None) -> str:
    if not tags:
        return ""
    cleaned = []
    seen = set()
    for tag in tags.split(","):
        item = tag.strip()
        if item and item.lower() not in seen:
            seen.add(item.lower())
            cleaned.append(item)
    return ", ".join(cleaned)


def serialize_user(user: User | None) -> dict[str, Any] | None:
    if not user:
        return None
    return {"id": user.id, "username": user.username, "email": user.email or "", "role": user.role}


def serialize_team_member(member: TeamMember) -> dict[str, Any]:
    return {
        "user_id": member.user_id,
        "username": member.user.username if member.user else "unknown",
        "role": member.role,
    }


def serialize_team(team: Team) -> dict[str, Any]:
    return {
        "id": team.id,
        "key": team.key,
        "name": team.name,
        "description": team.description or "",
        "created_by": team.creator.username if team.creator else "system",
        "created_at": team.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "current_user_can_manage": can_manage_team(team.id),
        "members": [serialize_team_member(m) for m in TeamMember.query.filter_by(team_id=team.id).order_by(TeamMember.role.asc()).all()],
    }


def serialize_column(column: Column) -> dict[str, Any]:
    return {
        "id": column.id,
        "key": column.key,
        "name": column.name,
        "position": column.position,
        "is_default": column.is_default,
    }


def serialize_task(task: Task) -> dict[str, Any]:
    column = task.column_ref
    team = task.team_ref
    assignee = task.assignee
    return {
        "id": task.id,
        "title": task.title,
        "description": task.description or "",
        "priority": task.priority,
        "tags": task.tags or "",
        "deadline": task.deadline.isoformat() if task.deadline else "",
        "column_id": task.column_id,
        "column_key": column.key if column else "",
        "column_name": column.name if column else "",
        "creator_id": task.creator_id,
        "creator": task.creator.username if task.creator else "system",
        "team_id": task.team_id,
        "team_name": team.name if team else "",
        "assignee_id": task.assignee_id,
        "assignee": assignee.username if assignee else "",
        "can_edit": can_edit_task(task) if current_user.is_authenticated else False,
        "created_at": task.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": task.updated_at.strftime("%Y-%m-%d %H:%M:%S") if task.updated_at else "",
    }


def serialize_rule(rule: Rule) -> dict[str, Any]:
    return {
        "id": rule.id,
        "name": rule.name,
        "trigger": rule.trigger,
        "condition_field": rule.condition_field,
        "condition_operator": rule.condition_operator,
        "condition_value": rule.condition_value,
        "action_type": rule.action_type,
        "action_value": rule.action_value,
        "enabled": rule.enabled,
    }


def serialize_event(event: Event) -> dict[str, Any]:
    try:
        payload = json.loads(event.payload or "{}")
    except json.JSONDecodeError:
        payload = {}
    user = User.query.get(event.user_id) if event.user_id else None
    return {
        "id": event.id,
        "type": event.type,
        "task_id": event.task_id,
        "user": user.username if user else "system",
        "payload": payload,
        "created_at": event.created_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


def serialize_incoming(item: IncomingTask) -> dict[str, Any]:
    return {
        "id": item.id,
        "source": item.source or "api",
        "external_id": item.external_id or "",
        "status": item.status,
        "error": item.error or "",
        "task_id": item.task_id,
        "created_at": item.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "processed_at": item.processed_at.strftime("%Y-%m-%d %H:%M:%S") if item.processed_at else "",
    }


def get_state() -> dict[str, Any]:
    return {
        "columns": [serialize_column(c) for c in Column.query.order_by(Column.position.asc()).all()],
        "tasks": [serialize_task(t) for t in get_visible_tasks()] if current_user.is_authenticated else [],
        "teams": [serialize_team(t) for t in get_visible_teams()] if current_user.is_authenticated else [],
        "users": [serialize_user(u) for u in get_visible_users()] if current_user.is_authenticated else [],
        "rules": [serialize_rule(r) for r in Rule.query.order_by(Rule.created_at.desc()).all()] if is_global_admin() else [],
        "events": [serialize_event(e) for e in Event.query.order_by(Event.created_at.desc()).limit(50).all()],
        "incoming": [serialize_incoming(i) for i in IncomingTask.query.order_by(IncomingTask.created_at.desc()).limit(20).all()],
        "current_user": serialize_user(current_user if current_user.is_authenticated else None),
    }


def emit_state() -> None:
    # Do not broadcast a ready-made state because every user has different
    # permissions and team visibility. We only broadcast a lightweight signal.
    # Each browser then calls /api/state and receives its own filtered state.
    socketio.emit("state_changed", {"time": datetime.now().strftime("%H:%M:%S")})


def log_event(event_type: str, task_id: int | None = None, payload: dict[str, Any] | None = None) -> None:
    try:
        user_id = current_user.id if current_user.is_authenticated else None
    except RuntimeError:
        # Background workers have app context, but not a browser request context.
        user_id = None
    event = Event(
        type=event_type,
        task_id=task_id,
        user_id=user_id,
        payload=json.dumps(payload or {}, ensure_ascii=False),
    )
    db.session.add(event)


def send_notification(message: str, notif_type: str = "info") -> None:
    socketio.emit("notification", {
        "message": message,
        "type": notif_type,
        "time": datetime.now().strftime("%H:%M:%S"),
    })


def validate_task_payload(data: dict[str, Any], partial: bool = False) -> tuple[bool, str]:
    if not partial and not str(data.get("title", "")).strip():
        return False, "Название задачи обязательно"
    if "priority" in data and data.get("priority") not in PRIORITIES:
        return False, "Недопустимый приоритет"
    if "deadline" in data and data.get("deadline"):
        if parse_deadline(data.get("deadline")) is None:
            return False, "Дедлайн должен быть в формате YYYY-MM-DD"
    return True, ""


def task_matches_rule(rule: Rule, task: Task) -> bool:
    if rule.condition_field == "always":
        return True

    expected = (rule.condition_value or "").strip().lower()
    operator = rule.condition_operator or "equals"

    if rule.condition_field == "priority":
        actual = task.priority.lower()
    elif rule.condition_field == "column":
        actual = (task.column_ref.key if task.column_ref else "").lower()
    elif rule.condition_field == "tags":
        actual = ",".join([t.strip().lower() for t in (task.tags or "").split(",")])
    elif rule.condition_field == "title":
        actual = task.title.lower()
    elif rule.condition_field == "deadline":
        if not task.deadline:
            actual = ""
        elif task.deadline < date.today():
            actual = "overdue"
        elif task.deadline == date.today():
            actual = "today"
        else:
            actual = task.deadline.isoformat()
    else:
        actual = ""

    if operator == "contains":
        return expected in actual
    if operator == "not_equals":
        return actual != expected
    return actual == expected


def apply_rule_action(rule: Rule, task: Task) -> None:
    if rule.action_type == "move_column":
        target = find_column(rule.action_value)
        if target and task.column_id != target.id:
            old_column = task.column_ref.name if task.column_ref else ""
            task.column_id = target.id
            log_event("rule_moved_task", task.id, {"rule": rule.name, "from": old_column, "to": target.name})
            send_notification(f'⚡ Правило "{rule.name}": "{task.title}" → {target.name}', "info")

    elif rule.action_type == "send_notification":
        log_event("rule_notification", task.id, {"rule": rule.name, "message": rule.action_value})
        send_notification(f'📢 {rule.action_value} — "{task.title}"', "success")

    elif rule.action_type == "set_priority":
        value = (rule.action_value or "").strip()
        if value in PRIORITIES and task.priority != value:
            old_priority = task.priority
            task.priority = value
            log_event("rule_changed_priority", task.id, {"rule": rule.name, "from": old_priority, "to": value})
            send_notification(f'⚡ Правило "{rule.name}": приоритет "{task.title}" → {value}', "info")

    elif rule.action_type == "add_tag":
        new_tag = (rule.action_value or "").strip()
        current_tags = [t.strip() for t in (task.tags or "").split(",") if t.strip()]
        if new_tag and new_tag.lower() not in [t.lower() for t in current_tags]:
            current_tags.append(new_tag)
            task.tags = normalize_tags(",".join(current_tags))
            log_event("rule_added_tag", task.id, {"rule": rule.name, "tag": new_tag})
            send_notification(f'⚡ Правило "{rule.name}": тег {new_tag} добавлен к "{task.title}"', "info")


def check_rules(trigger: str, task: Task) -> None:
    rules = Rule.query.filter_by(trigger=trigger, enabled=True).all()
    for rule in rules:
        if task_matches_rule(rule, task):
            apply_rule_action(rule, task)


def enrich_incoming_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Small demo enrichment: urgent/bug tags can raise priority."""
    result = dict(data)
    tags = normalize_tags(result.get("tags", ""))
    title = str(result.get("title", ""))
    combined = f"{title} {tags}".lower()

    if not result.get("priority"):
        result["priority"] = "medium"
    if any(word in combined for word in ["critical", "крит", "срочно", "urgent", "blocker"]):
        result["priority"] = "critical"
    elif any(word in combined for word in ["bug", "баг", "ошибка"]):
        result["priority"] = "high"

    result["tags"] = tags
    return result


def process_incoming_task(item: IncomingTask) -> None:
    try:
        data = json.loads(item.payload)
        data = enrich_incoming_payload(data)

        ok, error = validate_task_payload(data)
        if not ok:
            item.status = "error"
            item.error = error
            item.processed_at = datetime.utcnow()
            log_event("incoming_task_error", None, {"incoming_id": item.id, "error": error})
            return

        external_id = (item.external_id or "").strip()
        if external_id:
            existing_external = IncomingTask.query.filter(
                IncomingTask.id != item.id,
                IncomingTask.external_id == external_id,
                IncomingTask.status == "processed",
            ).first()
            if existing_external:
                item.status = "duplicate"
                item.task_id = existing_external.task_id
                item.error = "Дубликат по external_id"
                item.processed_at = datetime.utcnow()
                log_event("incoming_task_duplicate", item.task_id, {"incoming_id": item.id, "reason": "external_id"})
                return

        title = str(data.get("title", "")).strip()
        tags = normalize_tags(data.get("tags", ""))
        existing_task = Task.query.filter_by(title=title, tags=tags).first()
        if existing_task:
            item.status = "duplicate"
            item.task_id = existing_task.id
            item.error = "Дубликат по title + tags"
            item.processed_at = datetime.utcnow()
            log_event("incoming_task_duplicate", existing_task.id, {"incoming_id": item.id, "reason": "title_tags"})
            return

        column = find_column(data.get("column_id")) or find_column(data.get("column")) or get_default_column()
        team = find_team(data.get("team_id")) or find_team(data.get("team"))
        assignee = find_user(data.get("assignee_id")) or find_user(data.get("assignee"))
        if current_user.is_authenticated:
            ok, error = validate_team_assignment(team, assignee)
            if not ok:
                item.status = "error"
                item.error = error
                item.processed_at = datetime.utcnow()
                log_event("incoming_task_error", None, {"incoming_id": item.id, "error": error})
                return
        task = Task(
            title=title,
            description=data.get("description", ""),
            priority=data.get("priority", "medium"),
            tags=tags,
            deadline=parse_deadline(data.get("deadline")),
            column_id=column.id,
            creator_id=current_user.id if current_user.is_authenticated else None,
            team_id=team.id if team else None,
            assignee_id=assignee.id if assignee else None,
        )
        db.session.add(task)
        db.session.flush()

        item.status = "processed"
        item.task_id = task.id
        item.processed_at = datetime.utcnow()
        log_event("incoming_task_processed", task.id, {"incoming_id": item.id, "external_id": external_id})
        send_notification(f'📥 Из очереди создана задача: "{task.title}"', "success")
        check_rules("incoming_task", task)
        check_rules("task_created", task)

    except Exception as exc:  # noqa: BLE001 - hackathon demo: preserve error in queue
        item.status = "error"
        item.error = str(exc)
        item.processed_at = datetime.utcnow()
        log_event("incoming_task_error", None, {"incoming_id": item.id, "error": str(exc)})


legacy_tasks_to_import = backup_and_import_legacy_sqlite()

with app.app_context():
    db.create_all()
    migrate_sqlite_schema()
    seed_database()
    import_legacy_tasks(legacy_tasks_to_import)


# ===================== PAGES =====================

@app.route("/")
@login_required
def index():
    return render_template("index.html", initial_state=get_state())


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    error = ""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = User.query.filter_by(username=username).first()
        if user and user.check_password(password):
            login_user(user)
            return redirect(url_for("index"))
        error = "Неверный логин или пароль"

    return render_template("login.html", error=error)


@app.route("/register", methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    error = ""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip() or None
        password = request.form.get("password", "")

        if len(username) < 3:
            error = "Логин должен быть не короче 3 символов"
        elif len(password) < 6:
            error = "Пароль должен быть не короче 6 символов"
        elif User.query.filter_by(username=username).first():
            error = "Такой логин уже занят"
        elif email and User.query.filter_by(email=email).first():
            error = "Такой email уже занят"
        else:
            user = User(username=username, email=email, role="user")
            user.set_password(password)
            db.session.add(user)
            db.session.commit()
            login_user(user)
            return redirect(url_for("index"))

    return render_template("register.html", error=error)


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


# ===================== API: STATE =====================

@app.route("/api/state")
@login_required
def api_state():
    return jsonify(get_state())


@app.route("/api/events")
@login_required
def api_events():
    return jsonify({"events": get_state()["events"]})


# ===================== API: TASKS =====================

@app.route("/add_task", methods=["POST"])
@login_required
def add_task():
    data = request.get_json() or {}
    ok, error = validate_task_payload(data)
    if not ok:
        return jsonify({"status": "error", "message": error}), 400

    column = find_column(data.get("column_id")) or find_column(data.get("column")) or get_default_column()
    if not column:
        return jsonify({"status": "error", "message": "Нет ни одной колонки"}), 400

    team = find_team(data.get("team_id")) or find_team(data.get("team"))
    assignee = find_user(data.get("assignee_id")) or find_user(data.get("assignee"))
    ok, error = validate_team_assignment(team, assignee)
    if not ok:
        return jsonify({"status": "error", "message": error}), 403

    task = Task(
        title=data["title"].strip(),
        description=data.get("description", "").strip(),
        priority=data.get("priority", "medium"),
        tags=normalize_tags(data.get("tags", "")),
        deadline=parse_deadline(data.get("deadline")),
        column_id=column.id,
        creator_id=current_user.id,
        team_id=team.id if team else None,
        assignee_id=assignee.id if assignee else None,
    )
    db.session.add(task)
    db.session.flush()
    log_event("task_created", task.id, {"title": task.title, "column": column.name})
    check_rules("task_created", task)
    db.session.commit()

    send_notification(f'📝 Создана задача: "{task.title}"', "success")
    emit_state()
    return jsonify({"status": "ok", "task": serialize_task(task)})


@app.route("/update_task/<int:task_id>", methods=["POST"])
@login_required
def update_task(task_id: int):
    task = Task.query.get_or_404(task_id)
    if not can_edit_task(task):
        return jsonify({"status": "error", "message": "Нет доступа к редактированию этой задачи"}), 403
    data = request.get_json() or {}
    ok, error = validate_task_payload(data, partial=True)
    if not ok:
        return jsonify({"status": "error", "message": error}), 400

    old_data = serialize_task(task)

    if "title" in data and str(data.get("title", "")).strip():
        task.title = str(data["title"]).strip()
    if "description" in data:
        task.description = str(data.get("description", "")).strip()
    if "priority" in data:
        task.priority = data.get("priority")
    if "tags" in data:
        task.tags = normalize_tags(data.get("tags", ""))
    if "deadline" in data:
        task.deadline = parse_deadline(data.get("deadline"))
    if "column_id" in data or "column" in data:
        new_column = find_column(data.get("column_id")) or find_column(data.get("column"))
        if new_column:
            task.column_id = new_column.id

    if "team_id" in data or "team" in data or "assignee_id" in data or "assignee" in data:
        new_team = find_team(data.get("team_id")) or find_team(data.get("team"))
        new_assignee = find_user(data.get("assignee_id")) or find_user(data.get("assignee"))
        if not ("team_id" in data or "team" in data):
            new_team = task.team_ref
        if not ("assignee_id" in data or "assignee" in data):
            new_assignee = task.assignee
        ok, error = validate_team_assignment(new_team, new_assignee)
        if not ok:
            return jsonify({"status": "error", "message": error}), 403
        task.team_id = new_team.id if new_team else None
        task.assignee_id = new_assignee.id if new_assignee else None

    log_event("task_updated", task.id, {"before": old_data, "after": serialize_task(task)})
    check_rules("task_updated", task)
    db.session.commit()

    send_notification(f'✏️ Обновлена задача: "{task.title}"', "info")
    emit_state()
    return jsonify({"status": "ok", "task": serialize_task(task)})


@app.route("/move_task/<int:task_id>", methods=["POST"])
@login_required
def move_task(task_id: int):
    task = Task.query.get_or_404(task_id)
    if not can_edit_task(task):
        return jsonify({"status": "error", "message": "Нет доступа к перемещению этой задачи"}), 403
    data = request.get_json() or {}
    new_column = find_column(data.get("column_id")) or find_column(data.get("column"))
    if not new_column:
        return jsonify({"status": "error", "message": "Колонка не найдена"}), 404

    old_column = task.column_ref.name if task.column_ref else ""
    task.column_id = new_column.id
    log_event("task_moved", task.id, {"from": old_column, "to": new_column.name})
    check_rules("task_moved", task)
    db.session.commit()

    send_notification(f'🔄 "{task.title}" → {new_column.name}', "info")
    emit_state()
    return jsonify({"status": "ok"})


@app.route("/delete_task/<int:task_id>", methods=["POST"])
@login_required
def delete_task(task_id: int):
    task = Task.query.get_or_404(task_id)
    if not can_edit_task(task):
        return jsonify({"status": "error", "message": "Нет доступа к удалению этой задачи"}), 403
    title = task.title
    log_event("task_deleted", task.id, {"title": title})
    db.session.delete(task)
    db.session.commit()

    send_notification(f'🗑️ Удалена задача: "{title}"', "warning")
    emit_state()
    return jsonify({"status": "ok"})


@app.route("/api/check_deadlines", methods=["POST"])
@login_required
def check_deadlines():
    done = Column.query.filter_by(key="done").first()
    done_id = done.id if done else None
    tasks = Task.query.filter(Task.deadline.isnot(None)).all()

    count = 0
    for task in tasks:
        if done_id and task.column_id == done_id:
            continue
        if task.deadline < date.today():
            count += 1
            log_event("deadline_overdue", task.id, {"deadline": task.deadline.isoformat(), "title": task.title})
            send_notification(f'⏰ Просрочена задача: "{task.title}" ({task.deadline.isoformat()})', "warning")
        elif task.deadline == date.today():
            count += 1
            log_event("deadline_today", task.id, {"deadline": task.deadline.isoformat(), "title": task.title})
            send_notification(f'⏰ Дедлайн сегодня: "{task.title}"', "info")

    db.session.commit()
    emit_state()
    return jsonify({"status": "ok", "count": count})


# ===================== API: TEAMS =====================

@app.route("/add_team", methods=["POST"])
@login_required
def add_team():
    data = request.get_json() or {}
    name = str(data.get("name", "")).strip()
    description = str(data.get("description", "")).strip()
    if not name:
        return jsonify({"status": "error", "message": "Название команды обязательно"}), 400

    base_key = slugify(name)
    key = base_key
    suffix = 2
    while Team.query.filter_by(key=key).first():
        key = f"{base_key}_{suffix}"
        suffix += 1

    team = Team(key=key, name=name, description=description, created_by=current_user.id)
    db.session.add(team)
    db.session.flush()
    ensure_team_member(team, current_user, "admin")
    log_event("team_created", None, {"team_id": team.id, "name": team.name})
    db.session.commit()

    send_notification(f'👥 Создана команда: "{team.name}"', "success")
    emit_state()
    return jsonify({"status": "ok", "team": serialize_team(team)})


@app.route("/update_team/<int:team_id>", methods=["POST"])
@login_required
def update_team(team_id: int):
    team = Team.query.get_or_404(team_id)
    if not can_manage_team(team.id):
        return jsonify({"status": "error", "message": "Нужны права админа команды"}), 403

    data = request.get_json() or {}
    name = str(data.get("name", "")).strip()
    description = str(data.get("description", team.description or "")).strip()
    if not name:
        return jsonify({"status": "error", "message": "Название команды обязательно"}), 400

    old_name = team.name
    team.name = name
    team.description = description
    log_event("team_updated", None, {"team_id": team.id, "from": old_name, "to": name})
    db.session.commit()
    emit_state()
    return jsonify({"status": "ok"})


@app.route("/delete_team/<int:team_id>", methods=["POST"])
@login_required
def delete_team(team_id: int):
    team = Team.query.get_or_404(team_id)
    if not can_manage_team(team.id):
        return jsonify({"status": "error", "message": "Нужны права админа команды"}), 403

    for task in Task.query.filter_by(team_id=team.id).all():
        task.team_id = None
        task.assignee_id = None
    name = team.name
    db.session.delete(team)
    log_event("team_deleted", None, {"team_id": team_id, "name": name})
    db.session.commit()

    send_notification(f'👥 Команда удалена: "{name}"', "warning")
    emit_state()
    return jsonify({"status": "ok"})


@app.route("/add_team_member/<int:team_id>", methods=["POST"])
@login_required
def add_team_member(team_id: int):
    team = Team.query.get_or_404(team_id)
    if not can_manage_team(team.id):
        return jsonify({"status": "error", "message": "Нужны права админа команды"}), 403

    data = request.get_json() or {}
    user = find_user(data.get("user_id")) or find_user(data.get("username"))
    role = data.get("role", "member")
    if role not in {"admin", "member"}:
        return jsonify({"status": "error", "message": "Роль должна быть admin или member"}), 400
    if not user:
        return jsonify({"status": "error", "message": "Пользователь не найден"}), 404

    ensure_team_member(team, user, role)
    log_event("team_member_added", None, {"team": team.name, "user": user.username, "role": role})
    db.session.commit()

    send_notification(f'👥 {user.username} добавлен(а) в {team.name} как {role}', "success")
    emit_state()
    return jsonify({"status": "ok"})


@app.route("/update_team_member/<int:team_id>/<int:user_id>", methods=["POST"])
@login_required
def update_team_member(team_id: int, user_id: int):
    team = Team.query.get_or_404(team_id)
    if not can_manage_team(team.id):
        return jsonify({"status": "error", "message": "Нужны права админа команды"}), 403

    member = TeamMember.query.filter_by(team_id=team_id, user_id=user_id).first_or_404()
    data = request.get_json() or {}
    role = data.get("role", "member")
    if role not in {"admin", "member"}:
        return jsonify({"status": "error", "message": "Роль должна быть admin или member"}), 400

    if member.role == "admin" and role != "admin":
        admins_count = TeamMember.query.filter_by(team_id=team_id, role="admin").count()
        if admins_count <= 1:
            return jsonify({"status": "error", "message": "В команде должен остаться хотя бы один админ"}), 400

    member.role = role
    log_event("team_member_role_changed", None, {"team": team.name, "user_id": user_id, "role": role})
    db.session.commit()
    emit_state()
    return jsonify({"status": "ok"})


@app.route("/remove_team_member/<int:team_id>/<int:user_id>", methods=["POST"])
@login_required
def remove_team_member(team_id: int, user_id: int):
    team = Team.query.get_or_404(team_id)
    if not can_manage_team(team.id):
        return jsonify({"status": "error", "message": "Нужны права админа команды"}), 403

    member = TeamMember.query.filter_by(team_id=team_id, user_id=user_id).first_or_404()
    if member.role == "admin":
        admins_count = TeamMember.query.filter_by(team_id=team_id, role="admin").count()
        if admins_count <= 1:
            return jsonify({"status": "error", "message": "Нельзя удалить последнего админа команды"}), 400

    for task in Task.query.filter_by(team_id=team_id, assignee_id=user_id).all():
        task.assignee_id = None
    username = member.user.username if member.user else str(user_id)
    db.session.delete(member)
    log_event("team_member_removed", None, {"team": team.name, "user": username})
    db.session.commit()

    send_notification(f'👥 {username} удалён(а) из {team.name}', "warning")
    emit_state()
    return jsonify({"status": "ok"})


# ===================== API: COLUMNS =====================

@app.route("/add_column", methods=["POST"])
@login_required
@admin_required
def add_column():
    data = request.get_json() or {}
    name = str(data.get("name", "")).strip()
    if not name:
        return jsonify({"status": "error", "message": "Название колонки обязательно"}), 400

    base_key = slugify(name)
    key = base_key
    suffix = 2
    while Column.query.filter_by(key=key).first():
        key = f"{base_key}_{suffix}"
        suffix += 1

    max_position = db.session.query(db.func.max(Column.position)).scalar() or 0
    column = Column(key=key, name=name, position=max_position + 1)
    db.session.add(column)
    db.session.flush()
    log_event("column_created", None, {"name": name, "key": key})
    db.session.commit()

    send_notification(f'🧱 Создана колонка: "{name}"', "success")
    emit_state()
    return jsonify({"status": "ok", "column": serialize_column(column)})


@app.route("/update_column/<int:column_id>", methods=["POST"])
@login_required
@admin_required
def update_column(column_id: int):
    column = Column.query.get_or_404(column_id)
    data = request.get_json() or {}
    name = str(data.get("name", "")).strip()
    if not name:
        return jsonify({"status": "error", "message": "Название колонки обязательно"}), 400

    old_name = column.name
    column.name = name
    log_event("column_updated", None, {"from": old_name, "to": name})
    db.session.commit()
    emit_state()
    return jsonify({"status": "ok"})


@app.route("/delete_column/<int:column_id>", methods=["POST"])
@login_required
@admin_required
def delete_column(column_id: int):
    column = Column.query.get_or_404(column_id)
    if Column.query.count() <= 1:
        return jsonify({"status": "error", "message": "Нельзя удалить последнюю колонку"}), 400

    fallback = Column.query.filter(Column.id != column.id).order_by(Column.position.asc()).first()
    for task in Task.query.filter_by(column_id=column.id).all():
        task.column_id = fallback.id

    name = column.name
    db.session.delete(column)
    log_event("column_deleted", None, {"name": name, "moved_tasks_to": fallback.name})
    db.session.commit()

    send_notification(f'🧱 Колонка удалена: "{name}"', "warning")
    emit_state()
    return jsonify({"status": "ok"})


@app.route("/reorder_columns", methods=["POST"])
@login_required
@admin_required
def reorder_columns():
    data = request.get_json() or {}
    ids = data.get("ids", [])
    for idx, column_id in enumerate(ids, start=1):
        column = Column.query.get(int(column_id))
        if column:
            column.position = idx
    log_event("columns_reordered", None, {"ids": ids})
    db.session.commit()
    emit_state()
    return jsonify({"status": "ok"})


# ===================== API: RULES =====================

@app.route("/add_rule", methods=["POST"])
@login_required
@admin_required
def add_rule():
    data = request.get_json() or {}
    required = ["name", "trigger", "action_type"]
    if any(not str(data.get(field, "")).strip() for field in required):
        return jsonify({"status": "error", "message": "Заполните название, событие и действие"}), 400

    rule = Rule(
        name=data["name"].strip(),
        trigger=data.get("trigger", "task_created"),
        condition_field=data.get("condition_field", "always"),
        condition_operator=data.get("condition_operator", "equals"),
        condition_value=str(data.get("condition_value", "")).strip(),
        action_type=data.get("action_type", "send_notification"),
        action_value=str(data.get("action_value", "")).strip(),
        created_by=current_user.id,
    )
    db.session.add(rule)
    db.session.flush()
    log_event("rule_created", None, serialize_rule(rule))
    db.session.commit()

    send_notification(f'✅ Правило создано: "{rule.name}"', "success")
    emit_state()
    return jsonify({"status": "ok", "rule": serialize_rule(rule)})


@app.route("/toggle_rule/<int:rule_id>", methods=["POST"])
@login_required
@admin_required
def toggle_rule(rule_id: int):
    rule = Rule.query.get_or_404(rule_id)
    rule.enabled = not rule.enabled
    log_event("rule_toggled", None, {"name": rule.name, "enabled": rule.enabled})
    db.session.commit()
    emit_state()
    return jsonify({"status": "ok"})


@app.route("/delete_rule/<int:rule_id>", methods=["POST"])
@login_required
@admin_required
def delete_rule(rule_id: int):
    rule = Rule.query.get_or_404(rule_id)
    name = rule.name
    db.session.delete(rule)
    log_event("rule_deleted", None, {"name": name})
    db.session.commit()

    send_notification(f'🗑️ Правило удалено: "{name}"', "warning")
    emit_state()
    return jsonify({"status": "ok"})


# ===================== API: INCOMING QUEUE =====================

def api_key_is_valid() -> bool:
    expected = os.environ.get("INCOMING_API_KEY", "dev-incoming-token")
    provided = request.headers.get("X-API-Key", "")
    return bool(expected and provided == expected)


@app.route("/api/incoming_tasks", methods=["POST"])
def incoming_tasks():
    """External API endpoint: accepts raw tasks and puts them into RabbitMQ via outbox.

    This endpoint intentionally does NOT create Task immediately. It returns 202 Accepted:
    the message was accepted and will be processed asynchronously by worker.py.
    """
    if not api_key_is_valid():
        return jsonify({"status": "error", "message": "Invalid or missing X-API-Key"}), 401

    data = request.get_json() or {}
    title = str(data.get("title", "")).strip()
    if not title:
        return jsonify({"status": "error", "message": "title is required"}), 400

    source = str(data.get("source", "api")).strip() or "api"
    external_id = str(data.get("external_id", "")).strip() or None

    # Light duplicate guard before queueing. Worker also repeats deduplication safely.
    if external_id:
        existing = IncomingTask.query.filter_by(source=source, external_id=external_id, status="processed").first()
        if existing:
            return jsonify({
                "status": "duplicate",
                "message": "This external_id was already processed",
                "incoming": serialize_incoming(existing),
            }), 200

    item = IncomingTask(
        source=source,
        external_id=external_id,
        payload=json.dumps(data, ensure_ascii=False),
        status="queued",
    )
    db.session.add(item)
    db.session.flush()

    message_payload = {
        "message_id": f"incoming-task:{item.id}",
        "incoming_task_id": item.id,
    }
    outbox = OutboxMessage(
        exchange="kanban",
        routing_key="incoming.task.created",
        payload=json.dumps(message_payload, ensure_ascii=False),
        status="pending",
    )
    db.session.add(outbox)
    log_event("incoming_task_queued", None, {"incoming_id": item.id, "source": source, "external_id": external_id})
    db.session.commit()
    emit_state()

    return jsonify({
        "status": "queued",
        "incoming_task_id": item.id,
        "outbox_message_id": outbox.id,
    }), 202


# ===================== RUN =====================

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=True, allow_unsafe_werkzeug=True)

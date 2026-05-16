# Kanban Event System для хакатона

MVP канбан-системы уровня Jira/Trello: real-time доска, авторизация, команды, назначение задач, роли, правила автоматизации, входящий поток задач, дедубликация и история событий.

## Что есть

- Flask backend
- SQLite база данных
- Flask-Login: вход, регистрация, сессии
- Роли пользователей: `admin` и `user`
- Команды: создание, участники, роли внутри команды `admin/member`
- Назначение задач исполнителям
- Канбан-доска с кастомными колонками
- Drag-and-drop задач между колонками
- Real-time синхронизация через Socket.IO
- Правила автоматизации
- Входящий поток задач через API
- Дедубликация входящих задач
- Audit log / история событий
- Dockerfile для развёртывания

## Запуск локально

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Откройте в браузере:

```text
http://127.0.0.1:5000
```

Демо-аккаунты создаются автоматически:

```text
admin / admin123
user / user123
```

Также автоматически создаётся демо-команда:

```text
Victory Group
admin — admin команды
user — member команды
```

## Где база данных

Flask-SQLAlchemy хранит относительный SQLite-файл здесь:

```text
instance/tasks.db
```

Открыть можно через:

- PyCharm → Database → + → Data Source → SQLite → выбрать `instance/tasks.db`
- DB Browser for SQLite
- терминал:

```bash
sqlite3 instance/tasks.db
```

Полезные SQL-команды:

```sql
.tables
SELECT * FROM user;
SELECT * FROM team;
SELECT * FROM team_member;
SELECT * FROM task;
SELECT * FROM event ORDER BY id DESC LIMIT 20;
SELECT * FROM incoming_task;
SELECT * FROM rule;
SELECT * FROM "column";
```

## Как работает архитектура

```text
Browser
  ↓ HTTP fetch / WebSocket
Flask app.py
  ↓ SQLAlchemy
SQLite tasks.db
  ↓ Socket.IO signal
Browser refreshes /api/state
```

Важно: Socket.IO не рассылает всем пользователям готовую копию всех данных. Он рассылает сигнал `state_changed`, после чего каждый браузер сам делает запрос `/api/state` и получает только те задачи и команды, которые ему разрешены.

## Команды и роли

Есть два уровня ролей.

### Глобальная роль пользователя

Хранится в таблице `user`, поле `role`:

- `admin` — глобальный админ системы;
- `user` — обычный пользователь.

Глобальный `admin` может управлять колонками, правилами и видеть все команды.

### Роль внутри команды

Хранится в таблице `team_member`, поле `role`:

- `admin` — админ конкретной команды;
- `member` — обычный участник команды.

Админ команды может:

- добавлять участников в команду;
- удалять участников;
- менять роль `member/admin`;
- назначать задачи участникам своей команды.

Обычный участник может создавать задачи в своей команде, но назначать их другим не может. Он может назначить задачу себе или оставить задачу без исполнителя.

## Поток задачи

Когда пользователь создаёт задачу:

```text
POST /add_task
  ↓
проверка названия, приоритета, дедлайна
  ↓
проверка команды и исполнителя
  ↓
запись Task в SQLite
  ↓
создание события task_created
  ↓
проверка правил автоматизации
  ↓
Socket.IO отправляет state_changed
  ↓
каждый клиент обновляет /api/state
```

## Правила автоматизации

Правило — это простая связка:

```text
событие → условие → действие
```

Пример:

```text
Когда задача создана
Если priority equals critical
То move_column in_progress
```

Поддерживаются события:

- `task_created`
- `task_updated`
- `task_moved`
- `incoming_task`

Поля условия:

- `always`
- `priority`
- `column`
- `tags`
- `title`
- `deadline`

Операторы:

- `equals`
- `contains`
- `not_equals`

Действия:

- `move_column`
- `send_notification`
- `set_priority`
- `add_tag`

## Входящий поток задач

Это имитация очереди RabbitMQ/Kafka внутри SQLite.

Endpoint:

```http
POST /api/incoming_tasks
Content-Type: application/json
```

Пример тела:

```json
{
  "external_id": "CRM-42",
  "title": "Срочно исправить баг оплаты",
  "tags": "bug, urgent",
  "team_id": 1,
  "assignee_id": 2
}
```

Что происходит:

```text
1. JSON сохраняется в incoming_task со статусом pending.
2. Система валидирует данные.
3. Система обогащает задачу: bug → high, urgent/critical → critical.
4. Система проверяет дубликаты:
   - по external_id;
   - по title + tags.
5. Если дубль не найден, создаётся Task.
6. Статус входящей записи становится processed.
7. Создаётся событие incoming_task_processed.
8. Проверяются правила автоматизации.
```

Это можно презентовать как MVP очереди. В production вместо немедленной обработки можно подключить Celery/RabbitMQ/Kafka.

## Развёртывание через Docker

Собрать образ:

```bash
docker build -t kanban-event-system .
```

Запустить контейнер:

```bash
docker run -p 5000:5000 kanban-event-system
```

Открыть:

```text
http://localhost:5000
```

Для реального сервера обычно добавляют:

- домен;
- HTTPS;
- gunicorn/eventlet/gevent вместо debug-сервера;
- PostgreSQL вместо SQLite;
- переменную `SECRET_KEY`;
- volume для хранения базы;
- reverse proxy nginx.

## Что говорить на защите

> Мы сделали event-driven канбан-систему. Каждое действие превращается в событие: создание, изменение, перемещение, удаление задачи, входящая задача из очереди, срабатывание правила. События сохраняются в audit log, запускают автоматизацию и синхронизируют клиентов через Socket.IO. Также реализованы команды, админы команд и назначение задач исполнителям.

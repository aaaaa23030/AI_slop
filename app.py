from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///tasks.db'
db = SQLAlchemy(app)
socketio = SocketIO(app)

# ====== МОДЕЛИ ======

class Task(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200))
    description = db.Column(db.String(1000), default="")
    priority = db.Column(db.String(20), default="medium")
    column = db.Column(db.String(20))
    tags = db.Column(db.String(500), default="")  # теги через запятую
    deadline = db.Column(db.String(20), default="")  # дата дедлайна

class Rule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200))
    trigger = db.Column(db.String(50))  # task_created, task_moved, task_deleted
    condition_field = db.Column(db.String(50))  # priority, column, tags
    condition_value = db.Column(db.String(200))  # critical, done, bug
    action_type = db.Column(db.String(50))  # move_column, send_notification
    action_value = db.Column(db.String(200))  # in_progress, "Задача завершена!"
    enabled = db.Column(db.Boolean, default=True)

with app.app_context():
    db.create_all()

# ====== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======

def get_tasks_list():
    tasks = Task.query.all()
    return [{"id": t.id, "title": t.title, "description": t.description,
             "priority": t.priority, "column": t.column, 
             "tags": t.tags, "deadline": t.deadline} for t in tasks]

def send_notification(message, notif_type="info"):
    socketio.emit("notification", {
        "message": message,
        "type": notif_type,
        "time": datetime.now().strftime("%H:%M:%S")
    })

def check_rules(trigger, task_data):
    """Проверяет все правила и выполняет подходящие"""
    rules = Rule.query.filter_by(trigger=trigger, enabled=True).all()
    
    for rule in rules:
        # Проверяем условие
        condition_met = False
        
        if rule.condition_field == "priority":
            condition_met = (task_data.get("priority") == rule.condition_value)
        elif rule.condition_field == "column":
            condition_met = (task_data.get("column") == rule.condition_value)
        elif rule.condition_field == "tags":
            task_tags = task_data.get("tags", "").split(",")
            condition_met = (rule.condition_value in task_tags)
        
        if condition_met:
            # Выполняем действие
            task = Task.query.get(task_data["id"])
            if not task:
                continue
                
            if rule.action_type == "move_column":
                task.column = rule.action_value
                db.session.commit()
                socketio.emit("update_tasks", get_tasks_list())
                send_notification(f'⚡ Авто: "{task.title}" → {rule.action_value}', "info")
                
            elif rule.action_type == "send_notification":
                send_notification(f'📢 {rule.action_value} — "{task.title}"', "success")

# ====== МАРШРУТЫ ======

@app.route("/")
def index():
    tasks = Task.query.all()
    rules = Rule.query.all()
    return render_template("index.html", tasks=tasks, rules=rules)

@app.route("/add_task", methods=["POST"])
def add_task():
    data = request.get_json()
    if data and data.get("title"):
        new_task = Task(
            title=data["title"],
            description=data.get("description", ""),
            priority=data.get("priority", "medium"),
            tags=data.get("tags", ""),
            deadline=data.get("deadline", ""),
            column="todo"
        )
        db.session.add(new_task)
        db.session.commit()
        
        socketio.emit("update_tasks", get_tasks_list())
        send_notification(f'📝 Создана: "{new_task.title}"', "success")
        
        # Проверяем правила для нового задания
        task_data = {"id": new_task.id, "priority": new_task.priority, 
                     "tags": new_task.tags, "column": new_task.column}
        check_rules("task_created", task_data)
        
        return {"status": "ok"}
    return {"status": "error"}, 400

@app.route("/move_task/<int:task_id>", methods=["POST"])
def move_task(task_id):
    data = request.get_json()
    task = Task.query.get(task_id)
    if task and data and "column" in data:
        task.column = data["column"]
        db.session.commit()
        
        column_names = {"todo": "📋 To Do", "in_progress": "🔄 In Progress", "done": "✅ Done"}
        
        socketio.emit("update_tasks", get_tasks_list())
        send_notification(f'🔄 "{task.title}" → {column_names.get(task.column, task.column)}', "info")
        
        # Проверяем правила
        task_data = {"id": task.id, "priority": task.priority, 
                     "tags": task.tags, "column": task.column}
        check_rules("task_moved", task_data)
        
        return {"status": "ok"}
    return {"status": "error"}, 404

@app.route("/delete_task/<int:task_id>", methods=["POST"])
def delete_task(task_id):
    task = Task.query.get(task_id)
    if task:
        title = task.title
        db.session.delete(task)
        db.session.commit()
        
        socketio.emit("update_tasks", get_tasks_list())
        send_notification(f'🗑️ Удалена: "{title}"', "warning")
        return {"status": "ok"}
    return {"status": "error"}, 404

# ====== УПРАВЛЕНИЕ ПРАВИЛАМИ ======

@app.route("/add_rule", methods=["POST"])
def add_rule():
    data = request.get_json()
    if data:
        new_rule = Rule(
            name=data["name"],
            trigger=data["trigger"],
            condition_field=data["condition_field"],
            condition_value=data["condition_value"],
            action_type=data["action_type"],
            action_value=data["action_value"]
        )
        db.session.add(new_rule)
        db.session.commit()
        
        # Отправляем обновлённый список правил
        rules = Rule.query.all()
        rules_list = [{"id": r.id, "name": r.name, "trigger": r.trigger,
                       "condition_field": r.condition_field, "condition_value": r.condition_value,
                       "action_type": r.action_type, "action_value": r.action_value,
                       "enabled": r.enabled} for r in rules]
        socketio.emit("update_rules", rules_list)
        send_notification(f'✅ Правило создано: "{new_rule.name}"', "success")
        return {"status": "ok"}
    return {"status": "error"}, 400

@app.route("/toggle_rule/<int:rule_id>", methods=["POST"])
def toggle_rule(rule_id):
    rule = Rule.query.get(rule_id)
    if rule:
        rule.enabled = not rule.enabled
        db.session.commit()
        
        rules = Rule.query.all()
        rules_list = [{"id": r.id, "name": r.name, "trigger": r.trigger,
                       "condition_field": r.condition_field, "condition_value": r.condition_value,
                       "action_type": r.action_type, "action_value": r.action_value,
                       "enabled": r.enabled} for r in rules]
        socketio.emit("update_rules", rules_list)
        return {"status": "ok"}
    return {"status": "error"}, 404

@app.route("/delete_rule/<int:rule_id>", methods=["POST"])
def delete_rule(rule_id):
    rule = Rule.query.get(rule_id)
    if rule:
        db.session.delete(rule)
        db.session.commit()
        
        rules = Rule.query.all()
        rules_list = [{"id": r.id, "name": r.name, "trigger": r.trigger,
                       "condition_field": r.condition_field, "condition_value": r.condition_value,
                       "action_type": r.action_type, "action_value": r.action_value,
                       "enabled": r.enabled} for r in rules]
        socketio.emit("update_rules", rules_list)
        send_notification(f'🗑️ Правило удалено: "{rule.name}"', "warning")
        return {"status": "ok"}
    return {"status": "error"}, 404

if __name__ == "__main__":
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True)
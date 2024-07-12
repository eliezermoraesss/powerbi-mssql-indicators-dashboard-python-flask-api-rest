from flask import current_app as app
from .queries import get_qp_data

def scheduled_task():
    with app.app_context():
        data = get_qp_data()
        print("Tarefa agendada executada:", data)

app.apscheduler.add_job(id='Scheduled Task', func=scheduled_task, trigger='interval', minute=1)

from apscheduler.schedulers.blocking import BlockingScheduler
import requests

scheduler = BlockingScheduler()

def run_task():
    response = requests.get("http://localhost:5000/update-indicators")
    print(response.json())

scheduler.add_job(run_task, 'interval', minutes=2)

scheduler.start()
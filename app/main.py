from flask import request, jsonify
from app import create_app
from app.queries import get_all_indicators, get_all_totvs_indicators
from apscheduler.schedulers.background import BackgroundScheduler
import requests

app = create_app()

@app.route('/')
def home():
    return "API de Indicadores de Dashboard está online!"

@app.route('/indicators', methods=['GET'])
def all_indicators():
    data = get_all_indicators()
    return jsonify(data)

@app.route('/totvs-indicators', methods=['GET'])
def all_totvs_indicators():
    data = get_all_totvs_indicators()
    return jsonify(data)

def run_task():
    try:
        response = requests.get('http://127.0.0.1:5000/indicators')
        print(response.json())
    except requests.exceptions.ConnectionError as e:
        print(f"Erro de conexão: {e}")

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_task, 'interval', seconds=5)
    scheduler.start()

    app.run(host='0.0.0.0', port=5000, use_reloader=False)



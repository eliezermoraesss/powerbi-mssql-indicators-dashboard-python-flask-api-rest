from flask import request, jsonify
from app import create_app
from app.controllers.indicator_controller import get_all_indicators, get_all_totvs_indicators, save_totvs_indicator
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

@app.route('/refresh-totvs-indicators', methods=['GET'])
def save_totvs_indicators():
    save_totvs_indicator()
    return f"Atualização de indicadores realizada com sucesso!", 200

def scheduled_task_insert_totvs_indicators():
    try:
        requests.get('http://localhost:5000/refresh-totvs-indicators')
        print("Atualização de indicadores realizada com sucesso!")
    except requests.exceptions.ConnectionError as e:
        print(f"Erro de conexão: {e}")

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_task_insert_totvs_indicators, 'interval', seconds=5)
    scheduler.start()

    app.run(host='0.0.0.0', port=5000, use_reloader=False)



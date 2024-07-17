from flask import request, jsonify
from app import create_app
from app.controllers.indicator_controller import get_all_indicators, get_all_totvs_indicators, save_totvs_indicator, get_project_indicators
from apscheduler.schedulers.background import BackgroundScheduler
import requests

app = create_app()

@app.route('/')
def home():
    return "API de Indicadores de Dashboard está online!"

@app.route('/indicators', methods=['GET'])
def all_indicators():
    try:
        project_indicators_dataframe = get_project_indicators()
        data = get_all_indicators(project_indicators_dataframe)
        return jsonify(data)
    except Exception as e:
        print(f"Erro ao consultar todos os indicadores: {e}")

@app.route('/totvs-indicators', methods=['GET'])
def all_totvs_indicators():
    try:
        data = get_all_totvs_indicators()
        return jsonify(data)
    except Exception as e:
        print(f"Erro ao consultar os indicadores do TOTVS: {e}")

@app.route('/refresh-totvs-indicators', methods=['GET', 'POST'])
def save_totvs_indicators():
    save_totvs_indicator()
    return f"Atualização de indicadores realizada com sucesso!", 200

def scheduled_task_insert_totvs_indicators():
    try:
        requests.post('http://localhost:5000/refresh-totvs-indicators')
        print("Atualização de indicadores realizada com sucesso!")
    except requests.exceptions.ConnectionError as ex:
        print(f"Erro de conexão: {ex}")

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_task_insert_totvs_indicators, 'interval', days=5)
    scheduler.start()

    app.run(host='0.0.0.0', port=5000, use_reloader=False, debug=True)



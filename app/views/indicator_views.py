from flask import jsonify, abort
from app import create_app
from app.controllers.indicator_controller import (
    get_all_indicators,
    get_all_totvs_indicators,
    save_indicators
)
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import logging

logging.basicConfig(level=logging.DEBUG)

app = create_app()


@app.route('/')
def home():
    return "API de Indicadores de Dashboard está online!"


@app.route('/indicators', methods=['GET'])
def all_indicators():
    try:
        response = get_all_indicators()
        return jsonify(response)
    except Exception as e:
        logging.error(f"Erro ao consultar todos os indicadores: {e}")
        abort(500, description="Internal Server Error")


@app.route('/indicators/totvs', methods=['GET'])
def all_totvs_indicators():
    try:
        response = get_all_totvs_indicators()
        return jsonify(response)
    except Exception as e:
        logging.error(f"Erro ao consultar os indicadores do TOTVS: {e}")
        abort(500, description="Internal Server Error")


@app.route('/indicators/save', methods=['GET', 'POST'])
def save_all_indicators():
    try:
        logging.info("request: Atualização de todos Indicadores em andamento...")
        save_indicators()
        logging.info("response: Atualização e salvamento dos Indicadores realizada com sucesso!")
        return "Atualização e salvamento dos Indicadores realizada com sucesso!", 201
    except Exception as e:
        logging.error(f"Erro ao salvar os indicadores: {e}")
        abort(500, description="Internal Server Error")


def scheduled_task_save_all_indicators():
    try:
        logging.info("scheduled: Atualização de todos Indicadores em andamento...")
        requests.post('http://localhost:5000/indicators/save', timeout=600)  # 600 seconds or 10 minutes
        logging.info("scheduled: Atualização e salvamento dos Indicadores realizada com sucesso!")
    except requests.exceptions.ConnectionError as ex:
        logging.error(f"Erro de conexão: {ex}")


if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_task_save_all_indicators, 'interval', days=1)
    scheduler.start()
    app.run(host='0.0.0.0', port=5000, use_reloader=False, debug=True)

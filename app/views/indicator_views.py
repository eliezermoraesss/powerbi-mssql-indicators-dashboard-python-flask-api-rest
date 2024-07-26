from flask import jsonify, abort, render_template
from app import create_app
from app.controllers.indicator_controller import (
    get_all_indicators,
    get_all_totvs_indicators,
    save_indicators
)
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import logging
from app.extensions.email_service import send_email

logging.basicConfig(level=logging.DEBUG)

app = create_app()


@app.route('/')
def home():
    send_email("Eureka® Systems NOTIFICATION", "API de Indicadores de Dashboard está online!\n\nEureka® BOT")
    return render_template('index.html')


@app.route('/indicators', methods=['GET'])
def all_indicators():
    try:
        response = get_all_indicators()
        return jsonify(response)
    except Exception as e:
        error_message = f"Erro ao consultar todos os indicadores: {e}"
        logging.error(error_message)
        send_email("API Error - /indicators", error_message)
        abort(500, description="Internal Server Error")


@app.route('/indicators/totvs', methods=['GET'])
def all_totvs_indicators():
    try:
        response = get_all_totvs_indicators()
        return jsonify(response)
    except Exception as e:
        error_message = f"Erro ao consultar os indicadores do TOTVS: {e}"
        logging.error(error_message)
        send_email("API Error - /indicators/totvs", error_message)
        abort(500, description="Internal Server Error")


@app.route('/indicators/save', methods=['GET', 'POST'])
def save_all_indicators():
    try:
        logging.info("request: Atualização de todos Indicadores em andamento...")
        save_indicators()
        success_message = "response: Atualização e salvamento dos Indicadores realizada com sucesso!"
        logging.info(success_message)
        send_email("API Success - /indicators/save", success_message)
        return success_message, 201
    except Exception as e:
        error_message = f"Erro ao salvar os indicadores: {e}"
        logging.error(error_message)
        send_email("API Error - /indicators/save", error_message)
        abort(500, description="Internal Server Error")


def scheduled_task_save_all_indicators():
    try:
        logging.info("scheduled: Atualização de todos Indicadores em andamento...")
        requests.post('http://localhost:5000/indicators/save', timeout=600)  # 600 seconds or 10 minutes
        success_message = "scheduled: Atualização e salvamento dos Indicadores realizada com sucesso!"
        logging.info(success_message)
        send_email("Scheduled Task - Success", success_message)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"Erro de conexão: {ex}"
        logging.error(error_message)
        send_email("Scheduled Task - Error", error_message)


if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_task_save_all_indicators, 'interval', days=1)
    scheduler.start()
    app.run(host='0.0.0.0', port=5000, use_reloader=False, debug=True)

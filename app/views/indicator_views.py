import pytz
from flask import jsonify, abort, render_template, request
from app import create_app
from app.controllers.indicator_controller import (
    get_all_indicators,
    get_all_totvs_indicators,
    save_indicators, find_all_sharepoint_indicators
)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import requests
import logging
from app.extensions.email_service import send_email
from waitress import serve

logging.basicConfig(level=logging.DEBUG)

app = create_app()


@app.route('/')
def home():
    send_email("Eureka® Systems NOTIFICATION", "API de Indicadores de Dashboard está online!\n\nEureka® BOT")
    return render_template('index.html')


@app.route('/indicators', methods=['GET'])
def all_indicators():
    try:
        logging.info("request: Consulta de todos os Indicadores em andamento...")
        response = get_all_indicators()
        return jsonify(response), 200
    except Exception as e:
        error_message = f"Erro ao consultar todos os indicadores: {e}"
        logging.error(error_message)
        send_email("API Error - /indicators", error_message)
        abort(500, description="Internal Server Error")


@app.route('/indicators/totvs', methods=['GET'])
def all_totvs_indicators():
    try:
        logging.info("request: Consulta dos Indicadores TOTVS em andamento...")
        response = get_all_totvs_indicators()
        return jsonify(response), 200
    except Exception as e:
        error_message = f"Erro ao consultar os indicadores do TOTVS: {e}"
        logging.error(error_message)
        send_email("API Error - /indicators/totvs", error_message)
        abort(500, description="Internal Server Error")


@app.route('/indicators/save', methods=['GET', 'POST'])
def save_all_indicators():
    try:
        status_qp = request.args.get('qp')

        if status_qp is None:
            return abort(400, description="Parameter 'qp' is required")
        if status_qp == 'open' or status_qp == 'test':
            logging.info("request: Atualização de todos Indicadores em andamento...")
            project_data = find_all_sharepoint_indicators(status_qp)
            totvs_indicators = get_all_totvs_indicators()
            save_indicators(project_data, totvs_indicators)

            success_message = "response: Atualização e salvamento dos Indicadores realizada com sucesso!"
            logging.info(success_message)
            send_email("API Success - /indicators/save?qp=open", success_message)
            return success_message, 201
        elif status_qp == 'closed':
            logging.info("request: Atualização da tabela de QP CONCLUÍDA em andamento...")
            find_all_sharepoint_indicators(status_qp)

            sucess_message = "response: Atualização da tabela de QP CONCLUÍDA realizada com sucesso!"
            logging.info(sucess_message)
            send_email("API Success - /indicators/save?qp=closed", sucess_message)
            return sucess_message, 201
        else:
            return abort(400, description="Unknown value for 'qp'")

    except Exception as e:
        error_message = f"Erro ao salvar os indicadores: {e}"
        logging.error(error_message)
        send_email("API Error - /indicators/save", error_message)
        abort(500, description="Internal Server Error")


def scheduled_task_save_all_indicators():
    try:
        logging.info("scheduled: Atualização de todos Indicadores em andamento...")
        requests.post('http://localhost:5000/indicators/save?qp=open', timeout=600)  # 600 seconds or 10 minutes
        success_message = "scheduled: Atualização e salvamento dos Indicadores realizada com sucesso!"
        logging.info(success_message)
        send_email("Scheduled Task - Success", success_message)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"Erro de conexão: {ex}"
        logging.error(error_message)
        send_email("Scheduled Task - Error", error_message)


def scheduled_task_update_end_qps_table():
    try:
        logging.info("scheduled: Atualização da tabela de QP CONCLUÍDA em andamento...")
        requests.post('http://localhost:5000/indicators/save?qp=closed', timeout=1200)  # 1200 seconds or 20 minutes
        success_message = "scheduled: Atualização da tabela de QP CONCLUÍDA realizada com sucesso!"
        logging.info(success_message)
        send_email("Scheduled Task - Success", success_message)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"Erro de conexão: {ex}"
        logging.error(error_message)
        send_email("Scheduled Task - Error", error_message)


if __name__ == '__main__':
    timezone = pytz.timezone('America/Sao_Paulo')

    scheduler = BackgroundScheduler(timezone=timezone)
    scheduler.add_job(scheduled_task_save_all_indicators, CronTrigger(hour=7, minute=0, timezone=timezone))
    scheduler.add_job(scheduled_task_update_end_qps_table, 'interval', weeks=1)
    logging.info(f"Job agendado para executar no fuso horário {timezone}")
    scheduler.start()
    logging.info("Scheduler iniciado")

    serve(app, host='0.0.0.0', port=5000)

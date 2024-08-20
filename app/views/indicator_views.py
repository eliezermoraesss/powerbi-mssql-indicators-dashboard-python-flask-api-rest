import pytz
from flask import jsonify, abort, render_template, request
from app import create_app
from app.controllers.indicator_controller import *
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import requests
import logging
from app.extensions.email_service import send_email
from waitress import serve

logging.basicConfig(level=logging.INFO)

app = create_app()


@app.route('/')
def home():
    send_email("🤖 Eureka® BOT", "✔️ API de Indicadores de Dashboard está online!\n\n🦾🤖 Eureka® BOT")
    return render_template('index.html')


@app.route('/indicators', methods=['GET'])
def all_indicators():
    try:
        logging.info("request: Consulta de todos os Indicadores em andamento...")
        response = get_all_indicators()
        send_email("🤖 Eureka® BOT - /indicators", f"✔️ Consulta de todos Indicadores realizada com "
                                                      f"sucesso!\n\n{response}\n\n🦾🤖 Eureka® BOT")
        return jsonify(response), 200
    except Exception as e:
        error_message = f"❌ Erro ao consultar todos os indicadores: {e}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email("❌ API Error - /indicators", error_message)
        abort(500, description="Internal Server Error")


@app.route('/indicators/totvs', methods=['GET'])
def all_totvs_indicators():
    try:
        logging.info("request: Consulta dos Indicadores TOTVS em andamento...")
        response = get_all_totvs_indicators()
        send_email("🤖 Eureka® BOT INFO - /indicators/totvs", f"✔️ Consulta de Indicadores TOTVS realizada com "
                                                                 f"sucesso!\n\n{response}\n\n🦾🤖 Eureka® BOT ")
        return jsonify(response), 200
    except Exception as e:
        error_message = f"❌ Erro ao consultar os indicadores do TOTVS: {e}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email("❌ API Error - /indicators/totvs", error_message)
        abort(500, description="Internal Server Error")


@app.route('/indicators/save', methods=['GET', 'POST'])
def save_all_indicators():
    try:
        status_qp = request.args.get('qp')

        if status_qp is None:
            return abort(400, description="Parameter 'qp' is required")
        if status_qp == 'open' or status_qp == 'test':

            logging.info("request: 🤖 Atualização dos Indicadores e QPS ABERTAS em andamento...")
            project_data = find_all_sharepoint_indicators(status_qp)
            totvs_indicators = get_all_totvs_indicators()
            save_indicators(project_data, totvs_indicators)
            success_message = ("✔️ Atualização dos Indicadores e QPS ABERTAS realizada com sucesso!\n\n🦾🤖 "
                               "Eureka® BOT")
            logging.info(success_message)
            send_email("🤖 Eureka® BOT INFO - /indicators/save?qp=open - Success ✔️", success_message)
            requests.post('http://localhost:5000/indicators/save?qp=closed', timeout=1200)
            return success_message, 201

        elif status_qp == 'closed':
            logging.info("request: 🤖 Atualização das QPS CONCLUÍDAS em andamento...")
            find_all_sharepoint_indicators(status_qp)

            sucess_message = "✔️ Atualização das QPS CONCLUÍDAS realizada com sucesso!\n\n🦾🤖 Eureka® BOT"
            logging.info(sucess_message)
            send_email("🤖 Eureka® BOT INFO - /indicators/save?qp=closed - Success ✔️", sucess_message)
            return sucess_message, 201
        else:
            return abort(400, description="Unknown value for 'qp'")

    except Exception as e:
        error_message = f"❌ Erro ao salvar os indicadores: {e}\n\n🦾 Eureka® BOT"
        logging.error(error_message)
        send_email("❌ API Error - /indicators/save", error_message)
        abort(500, description=error_message)


@app.route('/indicators/qp/closed', methods=['GET'])
def find_all_end_qps():
    try:
        logging.info("request: 🤖 Consultando QPS CONCLUÍDAS...")
        response = find_qp_by_status_qp("closed")
        send_email("🤖 Eureka® BOT - /qp/closed", f"✔️ Requisição de QPS CONCLUÍDAS realizada com "
                                                     f"sucesso!\n\n{response}\n\n🦾🤖 Eureka® BOT")
        return "✔️ Requisição de QPS CONCLUÍDAS realizada com sucesso!", 200
    except Exception as e:
        error_message = f"❌ Erro ao consultar QPS CONCLUÍDAS: {e}\n\n🦾 Eureka® BOT"
        logging.error(error_message)
        send_email("❌ API Error - indicators/qp/closed", error_message)
        abort(500, description="Internal Server Error")


@app.route("/indicators/qp/send-email", methods=['GET'])
def send_email_qp():
    operation = request.args.get('type')

    if operation not in ["open_late", "open_up_to_date", "closed_no_date"]:
        error_message = f"❌ Tipo de operação inválido: {operation}"
        logging.error(error_message)
        return jsonify({"error": error_message}), 400

    try:
        sent_email, message = send_email_notification(operation)
        if not sent_email:
            raise Exception(f"{message}")
        logging.info(message)
        return jsonify({"message": message}), 200
    except Exception as ex:
        error_message = f"{ex}"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - /indicators/qp/send-email?type={operation} - Error ❌", error_message)
        return jsonify({"error": error_message}), 500


def scheduled_task_save_all_indicators():
    try:
        logging.info("🕗 scheduled: Atualização de todos Indicadores em andamento...")
        requests.post('http://localhost:5000/indicators/save?qp=open', timeout=600)  # 600 seconds or 10 minutes
        success_message = "🕗 scheduled: ✔️ Atualização dos Indicadores realizada com sucesso!\n\n🦾🤖 Eureka® BOT"
        logging.info(success_message)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {ex}\n\n🦾 Eureka® BOT"
        logging.error(error_message)
        send_email("🤖 Eureka® BOT INFO - Salvar Indicadores - Error ❌", error_message)


def scheduled_task_send_email_qp_open_late():
    url = 'http://localhost:5000/indicators/qp/send-email?type=open_late'
    try:
        logging.info('🕗 scheduled: Relatório de notificação por e-mail dos status das QPS abertas e atrasadas.')
        requests.get(url)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {url}\n\n{ex}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - Request error {url} ❌", error_message)


def scheduled_task_send_email_qp_open_up_to_date():
    url = 'http://localhost:5000/indicators/qp/send-email?type=open_up_to_date'
    try:
        logging.info('🕗 scheduled: Relatório de notificação por e-mail dos status das QPS abertas e em dia.')
        requests.get(url)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {url}\n\n{ex}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - Request error {url} ❌", error_message)


def scheduled_task_send_email_qp_closed_no_date():
    url = 'http://localhost:5000/indicators/qp/send-email?type=closed_no_date'
    try:
        logging.info('🕗 scheduled: Relatório de notificação por e-mail dos status das QPS fechadas e sem data.')
        requests.get(url)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {url}\n\n{ex}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - Request error {url} ❌", error_message)


if __name__ == '__main__':
    america_sp_timezone = pytz.timezone('America/Sao_Paulo')
    scheduler = BackgroundScheduler(timezone=america_sp_timezone)
    scheduler.add_job(scheduled_task_save_all_indicators, CronTrigger(day_of_week='1-5', hour=7, minute=0, timezone=america_sp_timezone))
    scheduler.add_job(scheduled_task_send_email_qp_open_late, CronTrigger(day_of_week='mon', hour=7, minute=0, timezone=america_sp_timezone))
    scheduler.add_job(scheduled_task_send_email_qp_open_up_to_date, CronTrigger(day_of_week='1-5', hour=8, minute=0, timezone=america_sp_timezone))
    scheduler.add_job(scheduled_task_send_email_qp_closed_no_date, CronTrigger(day_of_week='tue', hour=7, minute=0, timezone=america_sp_timezone))
    logging.info(f"Job agendado para executar no fuso horário {america_sp_timezone}")
    scheduler.start()
    logging.info("Scheduler iniciado!")

    serve(app, host='0.0.0.0', port=5000)

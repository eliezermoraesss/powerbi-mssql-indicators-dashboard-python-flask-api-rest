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

            logging.info("request: Atualização de todos Indicadores em andamento...")
            project_data = find_all_sharepoint_indicators(status_qp)
            totvs_indicators = get_all_totvs_indicators()
            save_indicators(project_data, totvs_indicators)
            success_message = (" ✔️ Atualização dos Indicadores e QPS ABERTAS realizada com sucesso!\n\n🦾🤖 "
                               "Eureka® BOT")
            logging.info(success_message)
            send_email("🤖 Eureka® BOT INFO - /indicators/save?qp=open - Success ✔️", success_message)
            requests.post('http://localhost:5000/indicators/save?qp=closed', timeout=1200)
            success_message = (" ✔️ Atualização das QPS CONCLUÍDAS realizada com sucesso!\n\n🦾🤖 "
                               "Eureka® BOT")
            logging.info(success_message)
            send_email("🤖 Eureka® BOT INFO - /indicators/save?qp=open - Success ✔️", success_message)
            return success_message, 201

        elif status_qp == 'closed':
            logging.info("request: Atualização da tabela de QP CONCLUÍDA em andamento...")
            find_all_sharepoint_indicators(status_qp)

            sucess_message = " ✔️ Atualização da tabela de QP CONCLUÍDA realizada com sucesso!\n\n🦾🤖 Eureka® BOT"
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
        logging.info("request: Consultando QPS CONCLUÍDAS...")
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
def send_notifications_emails():
    try:
        sent_email, message = send_all_notifications_emails()
        if not sent_email:
            raise Exception(f"Falha ao executar serviço de notificação por email: {message}")
        logging.info(message)
        return message, 200
    except Exception as ex:
        error_message = f"❌ Error: {ex}\n\n🦾 Eureka® BOT"
        logging.error(error_message)
        send_email("🤖 Eureka® BOT INFO - /indicators/qp/send-email - Error ❌", error_message)


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


def scheduled_task_update_end_qps_table():
    try:
        logging.info("🕗 scheduled: Atualização da tabela de QP CONCLUÍDA em andamento...")
        requests.post('http://localhost:5000/indicators/save?qp=closed', timeout=1200)  # 1200 seconds or 20 minutes
        success_message = ("🕗 scheduled: ✔️ Atualização da tabela de QP CONCLUÍDA realizada com sucesso!️\n\n🦾🤖 "
                           "Eureka® BOT")
        logging.info(success_message)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {ex}\n\n🦾 Eureka® BOT"
        logging.error(error_message)
        send_email("🤖 Eureka® BOT INFO - QP CONCLUÍDA - Error ❌", error_message)


def scheduled_task_send_notifications_emails():
    url = 'http://localhost:5000/indicators/qp/send-email'
    try:
        logging.info('🕗 scheduled: Relatório de notificação por e-mail dos status das QPS iniciado.')
        requests.get(url)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {url}\n\n{ex}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - Request error {url} ❌", error_message)


if __name__ == '__main__':
    timezone = pytz.timezone('America/Sao_Paulo')
    scheduler = BackgroundScheduler(timezone=timezone)
    scheduler.add_job(scheduled_task_save_all_indicators, CronTrigger(hour=6, minute=0, timezone=timezone))
    scheduler.add_job(scheduled_task_update_end_qps_table, CronTrigger(hour=6, minute=30, timezone=timezone))
    # scheduler.add_job(scheduled_task_send_notifications_emails, CronTrigger(hour=7, minute=0, timezone=timezone))
    logging.info(f"Job agendado para executar no fuso horário {timezone}")
    scheduler.start()
    logging.info("Scheduler iniciado!")

    serve(app, host='0.0.0.0', port=5000)

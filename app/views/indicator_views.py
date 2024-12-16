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
        status_qp = request.args.get('qp')
        if status_qp is None:
            return abort(400, description="Parameter 'qp' is required")
        if status_qp == 'open':
            logging.info("request: Consulta dos Indicadores TOTVS de QPS ABERTAS em andamento...")
            response = get_all_totvs_indicators(status_qp)
            send_email("🤖 Eureka® BOT INFO - /indicators/totvs",
                       f"✔️ Consulta de Indicadores TOTVS de QPS ABERTAS realizada com "
                       f"sucesso!\n\n{response}\n\n🦾🤖 Eureka® BOT ")
            return jsonify(response), 200
        elif status_qp == 'closed':
            logging.info("request: Consulta dos Indicadores TOTVS de QPS FECHADAS em andamento...")
            response = get_all_totvs_indicators(status_qp)
            send_email("🤖 Eureka® BOT INFO - /indicators/totvs",
                       f"✔️ Consulta de Indicadores TOTVS de QPS FECHADAS realizada com "
                       f"sucesso!\n\n{response}\n\n🦾🤖 Eureka® BOT ")
            return jsonify(response), 200
        else:
            return abort(400, description="Unknown value for 'qp'")
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
            logging.info("request: 🤖 Atualização dos Indicadores das QPS ABERTAS em andamento...")
            project_data = find_all_sharepoint_indicators(status_qp)
            totvs_indicators = get_all_totvs_indicators(status_qp)
            save_indicators(project_data, totvs_indicators, status_qp)
            success_message = ("✔️ Atualização dos Indicadores das QPS ABERTAS realizada com sucesso!\n\n🦾🤖 "
                               "Eureka® BOT")
            logging.info(success_message)
            send_email("🤖 Eureka® BOT INFO - /indicators/save?qp=open - Success ✔️", success_message)
            requests.post('http://localhost:5000/indicators/save?qp=closed', timeout=1200)
            return success_message, 201

        elif status_qp == 'closed':
            logging.info("request: 🤖 Atualização dos Indicadores das QPS FECHADAS em andamento...")
            project_data = find_all_sharepoint_indicators(status_qp)
            totvs_indicators = get_all_totvs_indicators(status_qp)
            save_indicators(project_data, totvs_indicators, status_qp, False)
            sucess_message = "✔️ Atualização dos Indicadores das QPS FECHADAS realizada com sucesso!\n\n🦾🤖 Eureka® BOT"
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
        logging.info("request: 🤖 Consultando QPS FECHADAS...")
        response = find_qp_by_status_qp("closed")
        send_email("🤖 Eureka® BOT - /qp/closed", f"✔️ Requisição de QPS FECHADAS realizada com "
                                                 f"sucesso!\n\n{response}\n\n🦾🤖 Eureka® BOT")
        return "✔️ Requisição de QPS FECHADAS realizada com sucesso!", 200
    except Exception as e:
        error_message = f"❌ Erro ao consultar QPS FECHADAS: {e}\n\n🦾 Eureka® BOT"
        logging.error(error_message)
        send_email("❌ API Error - indicators/qp/closed", error_message)
        abort(500, description="Internal Server Error")


@app.route("/indicators/qp/send-email", methods=['GET'])
def send_email_qp():
    request_param = request.args.get('type')
    if request_param not in ["open_late", "open_up_to_date", "closed_no_date"]:
        error_message = f"❌ Tipo de operação inválido: {request_param}"
        logging.error(error_message)
        return jsonify({"error": error_message}), 400

    try:
        sent_email, message = send_email_notification_qp(request_param)
        if not sent_email:
            raise Exception(f"{message}")
        logging.info(message)
        return jsonify({"message": message}), 200
    except Exception as ex:
        error_message = f"{ex}"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - /indicators/qp/send-email?type={request_param} - Error ❌", error_message)
        return jsonify({"error": error_message}), 500


@app.route("/indicators/qr/send-email", methods=['GET'])
def send_email_qr():
    request_param = request.args.get('status')
    if request_param not in ['open']:
        error_message = f"❌ Tipo de operação inválido: {request_param}"
        logging.error(error_message)
        return jsonify({"error": error_message}), 400

    try:
        sent_email, message = send_email_notification_qr(request_param)
        if not sent_email:
            raise Exception(f"{message}")
        logging.info(message)
        return jsonify({"message": message}), 200
    except Exception as ex:
        error_message = f"{ex}"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - /indicators/qr/send-email?status={request_param} - Error ❌", error_message)
        return jsonify({"error": error_message}), 500


@app.route("/indicators/solic-compras/send-email", methods=['GET'])
def send_email_sc():
    request_param = request.args.get('status')
    if request_param not in ['open']:
        error_message = f"❌ Tipo de operação inválido: {request_param}"
        logging.error(error_message)
        return jsonify({"error": error_message}), 400

    try:
        sent_email, message = send_email_notification_sc(request_param)
        if not sent_email:
            raise Exception(f"{message}")
        logging.info(message)
        return jsonify({"message": message}), 200
    except Exception as ex:
        error_message = f"{ex}"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - /indicators/solic-compras/send-email?status={request_param} - "
                   f"Error ❌", error_message)
        return jsonify({"error": error_message}), 500


def scheduled_task_save_all_indicators():
    try:
        logging.info("🕗 scheduled: Atualização de todos Indicadores em andamento...")
        requests.post('http://localhost:5000/indicators/save?qp=open', timeout=1800)  # 1800 seconds or 30 minutes
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
        requests.get(url, timeout=300)  # 300 seconds or 5 minutes
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {url}\n\n{ex}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - Request error {url} ❌", error_message)


def scheduled_task_send_email_qp_open_up_to_date():
    url = 'http://localhost:5000/indicators/qp/send-email?type=open_up_to_date'
    try:
        logging.info('🕗 scheduled: Relatório de notificação por e-mail dos status das QPS abertas e em dia.')
        requests.get(url, timeout=300)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {url}\n\n{ex}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - Request error {url} ❌", error_message)


def scheduled_task_send_email_qp_closed_no_date():
    url = 'http://localhost:5000/indicators/qp/send-email?type=closed_no_date'
    try:
        logging.info('🕗 scheduled: Relatório de notificação por e-mail dos status das QPS fechadas e sem data.')
        requests.get(url, timeout=300)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {url}\n\n{ex}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - Request error {url} ❌", error_message)


def scheduled_task_send_email_open_qr():
    url = 'http://localhost:5000/indicators/qr/send-email?status=open'
    try:
        logging.info('🕗 scheduled: Relatório de notificação por e-mail dos status das QRS abertas.')
        requests.get(url, timeout=300)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {url}\n\n{ex}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - Request error {url} ❌", error_message)


def scheduled_task_send_email_open_sc():
    url = 'http://localhost:5000/indicators/solic-compras/send-email?status=open'
    try:
        logging.info('🕗 scheduled: Relatório de notificação por e-mail dos status das SCs abertas.')
        requests.get(url, timeout=300)
    except requests.exceptions.ConnectionError as ex:
        error_message = f"❌ Erro de conexão: {url}\n\n{ex}\n\n🦾🤖 Eureka® BOT"
        logging.error(error_message)
        send_email(f"🤖 Eureka® BOT INFO - Request error {url} ❌", error_message)


if __name__ == '__main__':
    america_sp_timezone = pytz.timezone('America/Sao_Paulo')
    scheduler = BackgroundScheduler(timezone=america_sp_timezone)
    scheduler.add_job(scheduled_task_save_all_indicators,
                      CronTrigger(day_of_week='0-4', hour=8, minute=0, timezone=america_sp_timezone))
    scheduler.add_job(scheduled_task_send_email_open_qr,
                      CronTrigger(day_of_week='mon', hour=9, minute=0, timezone=america_sp_timezone))
    scheduler.add_job(scheduled_task_send_email_open_qr,
                      CronTrigger(day_of_week='wed', hour=9, minute=0, timezone=america_sp_timezone))
    scheduler.add_job(scheduled_task_send_email_qp_open_late,
                      CronTrigger(day_of_week='mon', hour=9, minute=30, timezone=america_sp_timezone))
    scheduler.add_job(scheduled_task_send_email_qp_open_up_to_date,
                      CronTrigger(day_of_week='tue', hour=9, minute=30, timezone=america_sp_timezone))
    scheduler.add_job(scheduled_task_send_email_qp_closed_no_date,
                      CronTrigger(day_of_week='wed', hour=9, minute=30, timezone=america_sp_timezone))
    scheduler.add_job(scheduled_task_send_email_open_sc,
                      CronTrigger(day_of_week='0-4', hour=10, minute=0, timezone=america_sp_timezone))
    scheduler.add_job(scheduled_task_save_all_indicators,
                      CronTrigger(day_of_week='0-4', hour=15, minute=0, timezone=america_sp_timezone))

    logging.info(f"Job agendado para executar no fuso horário {america_sp_timezone}")
    scheduler.start()
    logging.info("Scheduler iniciado!")

    serve(app, host='0.0.0.0', port=5000)

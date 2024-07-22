from flask import request, jsonify
from app import create_app
from app.controllers.indicator_controller import get_all_indicators, get_all_totvs_indicators, save_indicators, get_project_data
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import pydevd_pycharm
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
        print(f"Erro ao consultar todos os indicadores: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/indicators/project', methods=['GET'])
def all_project_indicators():
    try:
        print("request: Visualização de Indicadores de Projetos...")
        response = get_project_data()
        return response.to_json()
    except Exception as e:
        print(f"Erro ao consultar todos os Indicadores de Projeto: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/indicators/totvs', methods=['GET'])
def all_totvs_indicators():
    try:
        response = get_all_totvs_indicators()
        return jsonify(response)
    except Exception as e:
        print(f"Erro ao consultar os indicadores do TOTVS: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/indicators/save', methods=['GET', 'POST'])
def save_all_indicators():
    try:
        save_indicators()
        return f"Atualização de Indicadores realizada com sucesso!", 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def scheduled_task_all_indicators():
    try:
        print("Executando agendamento: Visualização de Indicadores...")
        requests.get('http://localhost:5000/indicators', timeout=None)
        print("scheduled: Visualização de Indicadores realizada com sucesso!")
        
    except requests.exceptions.ConnectionError as ex:
        print(f"Erro de conexão: {ex}")


def scheduled_task_all_project_indicators():
    try:
        print("Executando agendamento: Visualização de Indicadores de Projetos...")
        requests.get('http://localhost:5000/indicators/project', timeout=None)
        print("scheduled: Visualização de Indicadores de Projeto realizada com sucesso!")

    except requests.exceptions.ConnectionError as ex:
        print(f"Erro de conexão: {ex}")


def scheduled_task_save_totvs_indicators():
    try:
        print("Executando agendamento: Atualização de indicadores...")
        requests.post('http://localhost:5000//indicators/save')
        print("scheduled: Atualização de Indicadores realizada com sucesso!")
        
    except requests.exceptions.ConnectionError as ex:
        print(f"Erro de conexão: {ex}")


if __name__ == '__main__':
    # pydevd_pycharm.settrace('localhost', port=59333, stdoutToServer=True, stderrToServer=True)

    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_task_all_project_indicators, 'interval', seconds=30)
    scheduler.start()

    app.run(host='0.0.0.0', port=5000, use_reloader=False, debug=True)

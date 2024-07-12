from flask import request, jsonify
from app import create_app
from app.queries import get_all_indicators, get_all_totvs_indicators

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

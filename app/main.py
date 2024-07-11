from flask import request, jsonify
from app import create_app
from app.queries import get_qp_data

app = create_app()

@app.route('/update-indicators', methods=['GET'])
def run_scheduled_task():
    data = get_qp_data()
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

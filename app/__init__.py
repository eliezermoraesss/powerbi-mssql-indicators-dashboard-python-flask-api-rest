from flask import Flask
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mssql+pyodbc://coognicao:0705@Abc@192.175.175.6/PROTHEUS12_R27?driver=SQL+Server'
    db.init_app(app)
    return app
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from .setup_mssql import setup_mssql

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)

    username, password, database, server = setup_mssql()
    driver = 'SQL Server'
    conn_str = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    db_url = f'mssql+pyodbc:///?odbc_connect={conn_str}'

    app.config['SQLALCHEMY_DATABASE_URI'] = db_url
    db.init_app(app)
    return app
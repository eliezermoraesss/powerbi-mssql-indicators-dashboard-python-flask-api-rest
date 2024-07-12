from app.setup_mssql import setup_mssql

username, password, database, server = setup_mssql()

driver = 'SQL Server'
conn_str = f'DRIVER={driver};SERVER={server};UID={username};PWD={password}'
db_url = f'mssql+pyodbc:///?odbc_connect={conn_str}'

class Config:
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_DATABASE_URI = db_url
    SCHEDULER_API_ENABLED = True

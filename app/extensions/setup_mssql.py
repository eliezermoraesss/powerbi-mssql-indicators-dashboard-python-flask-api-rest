import logging
import os


def setup_mssql():
    try:
        data = os.getenv('USER_PASSWORD_MSSQL_PROD')
        username_txt, password_txt, database_txt, server_txt = data.split(';')
        return username_txt, password_txt, database_txt, server_txt
    except Exception as ex:
        logging.error(f"Ocorreu um erro ao ler ENV_VAR = USER_PASSWORD_MSSQL_PROD: {ex}")

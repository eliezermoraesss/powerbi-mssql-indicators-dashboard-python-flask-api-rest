import logging
import os


def setup_mssql():
    path = os.getenv('USER_PASSWORD_MSSQL_PROD')
    try:
        with open(path, 'r') as arquivo:
            string_lida = arquivo.read().strip()
            username_txt, password_txt, database_txt, server_txt = string_lida.split(';')
            return username_txt, password_txt, database_txt, server_txt

    except FileNotFoundError as ex:
        logging.error(f"Erro ao ler credenciais de acesso ao banco de dados MSSQL.\n\nBase de "
                      f"dados ERP TOTVS PROTHEUS.\n\nPor favor, informe ao desenvolvedor/TI\n\n{ex}")

    except Exception as ex:
        logging.error(f"Ocorreu um erro ao ler o arquivo: {ex}")

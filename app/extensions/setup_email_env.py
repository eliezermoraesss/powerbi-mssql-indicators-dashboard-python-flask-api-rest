import logging
import json
import os


def read_email_params():
    path = os.getenv('EMAIL_APP_PASSWORD_GMAIL')
    try:
        with open(path, 'r') as file:
            string = file.read()
            data_dict = json.loads(string)
            return data_dict

    except FileNotFoundError as ex:
        logging.error(f"Erro ao ler credenciais de acesso ao banco de dados MSSQL.\n\nBase de "
                      f"dados ERP TOTVS PROTHEUS.\n\nPor favor, informe ao desenvolvedor/TI\n\n{ex}")
    except json.JSONDecodeError:
        logging.error("O arquivo não está no formato JSON válido.")
    except Exception as ex:
        logging.error(f"Ocorreu um erro ao ler o arquivo: {ex}")

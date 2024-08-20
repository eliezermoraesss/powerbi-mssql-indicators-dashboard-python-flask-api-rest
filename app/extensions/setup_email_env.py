import logging
import json
import os


def read_email_params(operation=None):
    if operation is None:
        data = os.getenv('EMAIL_APP_PASSWORD_GMAIL')
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            logging.error("Estrutura de dados incorreta.")
        except Exception as ex:
            logging.error(f"Ocorreu um erro ao ler ENV_VAR = EMAIL_APP_PASSWORD_GMAIL: {ex}")
    else:
        path = r"\\192.175.175.4\desenvolvimento\REPOSITORIOS\resources\application-properties\ENAPLIC_EMAILS.txt"
        try:
            with open(path, 'r') as file:
                string = file.read()
                email_recipients = json.loads(string)
                return email_recipients
        except FileNotFoundError as ex:
            logging.error(f"Arquivo não localizado: {ex}")
        except json.JSONDecodeError as ex:
            logging.error(f"O arquivo não está no formato JSON válido: {ex}")
        except Exception as ex:
            logging.error(f"Ocorreu um erro ao ler o arquivo: {ex}")

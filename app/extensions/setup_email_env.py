import logging
import json
import os


def read_email_params():
    data = os.getenv('EMAIL_APP_PASSWORD_GMAIL')
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        logging.error("Estrutura de dados incorreta.")
    except Exception as ex:
        logging.error(f"Ocorreu um erro ao ler ENV_VAR: {ex}")

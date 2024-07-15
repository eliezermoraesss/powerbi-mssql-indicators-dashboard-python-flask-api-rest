import os
import sys
import ctypes
from .extensions.show_message_tk import exibir_mensagem

def setup_mssql():
    caminho_do_arquivo = (r"\\192.175.175.4\f\INTEGRANTES\ELIEZER\PROJETO SOLIDWORKS "
                          r"TOTVS\libs-python\user-password-mssql\USER_PASSWORD_MSSQL_PROD.txt")
    try:
        with open(caminho_do_arquivo, 'r') as arquivo:
            string_lida = arquivo.read().strip()
            username_txt, password_txt, database_txt, server_txt = string_lida.split(';')
            return username_txt, password_txt, database_txt, server_txt

    except FileNotFoundError:
        exibir_mensagem("CADASTRO DE ESTRUTURA - TOTVS®", "Erro ao ler credenciais de acesso ao banco de dados MSSQL.\n\nBase de "
                                         "dados ERP TOTVS PROTHEUS.\n\nPor favor, informe ao desenvolvedor/TI "
                                         "sobre o erro exibido.\n\nTenha um bom dia! ツ", "error")
        sys.exit()

    except Exception as ex:
        exibir_mensagem("CADASTRO DE ESTRUTURA - TOTVS®", f"Ocorreu um erro ao ler o arquivo: {ex}", "error")
        sys.exit()

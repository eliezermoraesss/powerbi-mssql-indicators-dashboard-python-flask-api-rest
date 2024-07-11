import os
import sys
import ctypes

def setup_mssql():
    caminho_do_arquivo = (r"\\192.175.175.4\f\INTEGRANTES\ELIEZER\PROJETO SOLIDWORKS "
                          r"TOTVS\libs-python\user-password-mssql\USER_PASSWORD_MSSQL_PROD.txt")
    try:
        with open(caminho_do_arquivo, 'r') as arquivo:
            string_lida = arquivo.read().strip()
            username_txt, password_txt, database_txt, server_txt = string_lida.split(';')
            return username_txt, password_txt, database_txt, server_txt

    except FileNotFoundError:
        ctypes.windll.user32.MessageBoxW(0,
                                         "Erro ao ler credenciais de acesso ao banco de dados MSSQL.\n\nBase de "
                                         "dados ERP TOTVS PROTHEUS.\n\nPor favor, informe ao desenvolvedor/TI "
                                         "sobre o erro exibido.\n\nTenha um bom dia! ツ",
                                         "CADASTRO DE ESTRUTURA - TOTVS®", 16 | 0)
        sys.exit()

    except Exception as ex:
        ctypes.windll.user32.MessageBoxW(0, f"Ocorreu um erro ao ler o arquivo: {ex}", "CADASTRO DE ESTRUTURA - TOTVS®",
                                         16 | 0)
        sys.exit()

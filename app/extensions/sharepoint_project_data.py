import os
import win32com.client as win32
import pandas as pd
import tempfile
import pythoncom


def get_sharepoint_project_data(file_name):
    pythoncom.CoInitialize()

    # Caminho para o arquivo Excel
    file_path = os.path.join(tempfile.gettempdir(), file_name)

    # Verifica se o arquivo existe
    if not os.path.exists(file_path):
        print(f"O arquivo {file_path} não foi encontrado.")
    else:
        # Verifica se o Excel já está em execução
        try:
            excel_app = win32.GetActiveObject('Excel.Application')
            new_instance = False
        except Exception:
            excel_app = win32.Dispatch('Excel.Application')
            excel_app.Visible = False  # Mantenha o Excel invisível
            new_instance = True

        try:
            # Abre a pasta de trabalho
            workbook = excel_app.Workbooks.Open(file_path)

            # Executa a macro
            excel_app.Application.Run('PROJ_INDICATORS.xlsm!Macro2')

            # Espera a macro terminar de executar
            excel_app.CalculateUntilAsyncQueriesDone()

            # Salva a pasta de trabalho
            workbook.Save()

            # Lê os dados da planilha "BD" em um DataFrame do Pandas
            sheet_name = "BD"
            df = pd.read_excel(file_path, sheet_name=sheet_name)

            return df

            # Exibe as primeiras linhas do DataFrame
            # print(df)

        except Exception as e:
            print(f"Ocorreu um erro: {e}")
        finally:
            # Fecha a pasta de trabalho
            workbook.Close(SaveChanges=True)
            # Fecha o Excel somente se criamos uma nova instância
            if new_instance:
                excel_app.Quit()
            pythoncom.CoUninitialize()

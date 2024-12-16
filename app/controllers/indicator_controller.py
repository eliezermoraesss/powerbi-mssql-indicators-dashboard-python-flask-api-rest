import base64
import locale
from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta
from pandas import DataFrame

from app import db
from sqlalchemy import text
from app.extensions.sharepoint_project_data import get_sharepoint_project_data
import pandas as pd
from typing import Dict, Any
import logging
from app.extensions.email_service import send_email

logging.basicConfig(level=logging.INFO)

indicators_table = "tb_dashboard_indicators"
file_name = {
    "open": "PROJ_INDICATORS.xlsm",
    "closed": "PROJ_INDICATORS_QP_CONCLUIDO.xlsm",
    "test": "PROJ_INDICATORS-TEST.xlsm"}
status = {"open": 'A', "closed": 'F'}


def get_all_indicators() -> Dict[str, Any]:
    result = find_all_qp()
    cod_qps = [row[1] for row in result]
    data = {}

    indicators = {
        "descricao": "des_qp",
        "status_qp": "status_qp",
        "data_emissao_qp": "dt_open_qp",
        "prazo_entrega_qp": "dt_end_qp",
        "data_inicio_proj": "dt_start_proj",
        "data_fim_proj": "dt_end_proj",
        "duracao_proj": "vl_proj_duration",
        "status_proj": "status_proj",
        "baseline": "vl_proj_all_prod",
        "desconsiderar": "vl_proj_prod_cancel",
        "indice_mudanca": "vl_proj_modify_perc",
        "projeto_liberado": "vl_proj_released",
        "projeto_pronto": "vl_proj_finished",
        "em_ajuste": "vl_proj_adjusted",
        "quant_pi_proj": "vl_proj_pi",
        "quant_mp_proj": "vl_proj_mp",
        "op_total": "vl_all_op",
        "indice_pcp": "vl_pcp_perc",
        "op_fechada": "vl_closed_op",
        "indice_producao": "vl_product_perc",
        "sc_total": "vl_all_sc",
        "pc_total": "vl_all_pc",
        "indice_compra": "vl_compras_perc",
        "mat_entregue": "vl_mat_received",
        "indice_recebimento": "vl_mat_received_perc",
        "custo_total_mp_pc": "vl_total_mp_pc_cost",
        "custo_mp_pc": "vl_mp_pc_cost",
        "custo_item_com_pc": "vl_com_pc_cost",
        "custo_mp_mat_entregue": "vl_mp_deliver_cost",
        "custo_item_com_mat_entregue": "vl_com_deliver_cost"
    }

    for cod_qp in cod_qps:
        cod_qp_formatado = cod_qp.lstrip('0')
        data[cod_qp] = {}
        for key, value in indicators.items():
            try:
                data[cod_qp][key] = get_indicator_value(f"TOP 1 {value}",
                                                        f"enaplic_management.dbo.{indicators_table}",
                                                        f"cod_qp LIKE '%{cod_qp_formatado}' ORDER BY id DESC")
            except Exception as e:
                error_message = f"Error retrieving {key} for cod_qp {cod_qp_formatado}: {e}"
                logging.error(error_message)
                send_email("API Error - get_all_indicators", error_message)
                data[cod_qp][key] = 0  # Default to 0 if there's an error
    return data


def get_all_totvs_indicators(status_qp) -> Dict[str, Any]:
    result = find_qp_by_status_qp(status_qp)
    cod_qps = [row[1] for row in result]
    data = {}
    for cod_qp in cod_qps:
        cod_qp_formatado = cod_qp.lstrip('0')
        try:
            data[cod_qp] = {
                "op_total": get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC2010",
                                                f"C2_ZZNUMQP LIKE '%{cod_qp_formatado}' AND D_E_L_E_T_ <> '*'"),

                "op_fechada": get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC2010",
                                                  f"C2_ZZNUMQP LIKE '%{cod_qp_formatado}' AND C2_DATRF <> '       ' "
                                                  f"AND D_E_L_E_T_ <> '*'"),

                "sc_total": get_indicator_value("COUNT(DISTINCT (C1_NUM + C1_ITEM))", "PROTHEUS12_R27.dbo.SC1010",
                                                f"C1_ZZNUMQP LIKE '%{cod_qp_formatado}' AND D_E_L_E_T_ <> '*'"),

                "pc_total": get_indicator_value("COUNT(DISTINCT (C7_NUM + C7_ITEM))", "PROTHEUS12_R27.dbo.SC7010",
                                                f"C7_ZZNUMQP LIKE '%{cod_qp_formatado}' AND C7_NUMSC <> '      ' "
                                                f"AND D_E_L_E_T_ <> '*'"),

                "mat_entregue": get_indicator_value("COUNT(DISTINCT (C7_NUM + C7_ITEM))", "PROTHEUS12_R27.dbo.SC7010",
                                                    f"C7_ZZNUMQP LIKE '%{cod_qp_formatado}' AND C7_ENCER = 'E' "
                                                    f"AND C7_NUMSC <> '      ' AND D_E_L_E_T_ <> '*'"),

                "custo_mp_pc": get_indicator_value("ROUND(SUM(C7_TOTAL), 2)", "PROTHEUS12_R27.dbo.SC7010",
                                                   f"C7_ZZNUMQP LIKE '%{cod_qp_formatado}' AND C7_LOCAL = '01' "
                                                   f"AND D_E_L_E_T_ <> '*'"),

                "custo_item_com_pc": get_indicator_value("ROUND(SUM(C7_TOTAL), 2)", "PROTHEUS12_R27.dbo.SC7010",
                                                         f"C7_ZZNUMQP LIKE '%{cod_qp_formatado}' AND C7_LOCAL <> '01' "
                                                         f"AND D_E_L_E_T_ <> '*'"),

                "custo_mp_mat_entregue": get_indicator_value("ROUND(SUM(C7_TOTAL), 2)", "PROTHEUS12_R27.dbo.SC7010",
                                                             f"C7_ZZNUMQP LIKE '%{cod_qp_formatado}' "
                                                             f"AND C7_NUMSC <> '      ' AND C7_ENCER = 'E' AND "
                                                             f"C7_LOCAL = '01' AND D_E_L_E_T_ <> '*'"),

                "custo_item_com_mat_entregue": get_indicator_value(
                    "ROUND(SUM(C7_TOTAL), 2)", "PROTHEUS12_R27.dbo.SC7010", f"C7_ZZNUMQP "
                                                                            f"LIKE '%{cod_qp_formatado}' AND C7_NUMSC "
                                                                            f"<> '      ' AND C7_ENCER = 'E' AND "
                                                                            f"C7_LOCAL <> '01' "
                                                                            f"AND D_E_L_E_T_ <> '*'")
            }
            data[cod_qp]["custo_total_mp_pc"] = round(data[cod_qp]["custo_mp_pc"] + data[cod_qp]["custo_item_com_pc"],
                                                      2)
        except Exception as e:
            error_message = f"Error retrieving TOTVS indicators for cod_qp {cod_qp_formatado}: {e} + QP: {cod_qp}"
            logging.error(error_message)
            send_email("API Error - get_all_totvs_indicators", error_message)
            data[cod_qp] = {key: 0 for key in ["op_total", "op_fechada", "sc_total", "pc_total", "mat_entregue",
                                               "custo_total_mp_pc", "custo_mp_pc", "custo_item_com_pc",
                                               "custo_mp_mat_entregue", "custo_item_com_mat_entregue"]}

    return add_percentage_indicators(data)


def get_indicator_value(select_clause: str, table_name: str, where_clause: str) -> float:
    try:
        query = text(f"SELECT {select_clause} AS value FROM {table_name} WHERE {where_clause};")
        result = db.session.execute(query).scalar()
        return result if result is not None else 0
    except Exception as e:
        error_message = f"Error executing query: SELECT {select_clause} FROM {table_name} WHERE {where_clause}: {e}"
        logging.error(error_message)
        send_email("API Error - get_indicator_value", error_message)
        return 0


def add_percentage_indicators(data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    for cod_qp, values in data.items():
        try:
            values['indice_producao'] = round((values['op_fechada'] / values['op_total']) * 100, 2) if values[
                                                                                                           'op_total'] > 0 else 0
            values['indice_compra'] = round((values['pc_total'] / values['sc_total']) * 100, 2) if values[
                                                                                                       'sc_total'] > 0 else 0
            values['indice_recebimento'] = round((values['mat_entregue'] / values['pc_total']) * 100, 2) if values[
                                                                                                                'pc_total'] > 0 else 0
        except Exception as e:
            error_message = f"Error calculating percentage indicators for cod_qp {cod_qp}: {e}"
            logging.error(error_message)
            send_email("API Error - add_percentage_indicators", error_message)
            values['indice_producao'] = 0
            values['indice_compra'] = 0
            values['indice_recebimento'] = 0
    return data


def insert_query(table_name):
    return text(f"""
                INSERT INTO 
                    enaplic_management.dbo.{table_name} 
                    (cod_qp, des_qp, status_qp, dt_open_qp, dt_end_qp, dt_start_proj,
                     dt_end_proj, vl_proj_duration, status_proj, 
                     vl_proj_all_prod, vl_proj_prod_cancel, vl_proj_modify_perc, 
                     vl_proj_released, vl_proj_finished, vl_proj_adjusted, 
                     vl_proj_pi, vl_proj_mp, vl_all_op, vl_pcp_perc, vl_closed_op, 
                     vl_product_perc, vl_all_sc, vl_all_pc, vl_compras_perc, 
                     vl_mat_received, vl_mat_received_perc, vl_total_mp_pc_cost, vl_mp_pc_cost, 
                     vl_com_pc_cost, vl_mp_deliver_cost, vl_com_deliver_cost) 
                VALUES
                    (:qp, :description, :status_qp, :data_emissao_qp, :prazo_entrega_qp, :data_inicio_proj,
                     :data_fim_proj, :duracao_proj, :status_proj, 
                     :baseline, :desconsiderar, :indice_mudanca, 
                     :projeto_liberado, :projeto_pronto, :em_ajuste, 
                     :quant_pi_proj, :quant_mp_proj, :op_total, :indice_pcp, :op_fechada, 
                     :indice_producao, :sc_total, :pc_total, :indice_compra, 
                     :mat_entregue, :indice_recebimento, :custo_total_mp_pc, :custo_mp_pc,
                     :custo_item_com_pc, :custo_mp_mat_entregue, :custo_item_com_mat_entregue);
                """)


def find_all_sharepoint_indicators(status_qp: str) -> Dict[str, Any]:
    project_data = get_project_data(file_name[status_qp])
    update_all_qps_table(project_data, status_qp)
    return project_data


def save_indicators(project_data: Dict[str, Any], totvs_indicators: Dict[str, Any], status_qp,
                    clean_table=None) -> None:
    try:
        if clean_table is None:
            db.session.execute(text(f"TRUNCATE TABLE enaplic_management.dbo.{indicators_table}"))
        for cod_qp, project_indicators in project_data.items():
            try:
                query = insert_query(indicators_table)
                op_total = int(totvs_indicators[cod_qp]['op_total'])
                quant_pi_proj = int(project_indicators['quant_pi_proj'])
                indice_pcp = round((op_total / quant_pi_proj) * 100, 2) if quant_pi_proj > 0 else 0

                db.session.execute(query, {
                    'qp': cod_qp,
                    'description': project_indicators['description'],
                    'status_qp': status[status_qp],
                    'data_emissao_qp': project_indicators['data_emissao_qp'],
                    'prazo_entrega_qp': project_indicators['prazo_entrega_qp'],
                    'data_inicio_proj': project_indicators['data_inicio_proj'],
                    'data_fim_proj': project_indicators['data_fim_proj'],
                    'duracao_proj': int(project_indicators['duracao_proj']),
                    'status_proj': project_indicators['status_proj'],
                    'baseline': int(project_indicators['baseline']),
                    'desconsiderar': int(project_indicators['desconsiderar']),
                    'indice_mudanca': float(project_indicators['indice_mudanca']),
                    'projeto_liberado': int(project_indicators['projeto_liberado']),
                    'projeto_pronto': int(project_indicators['projeto_pronto']),
                    'em_ajuste': int(project_indicators['em_ajuste']),
                    'quant_pi_proj': int(quant_pi_proj),
                    'quant_mp_proj': int(project_indicators['quant_mp_proj']),
                    'op_total': op_total,
                    'indice_pcp': float(indice_pcp),
                    'indice_producao': float(totvs_indicators[cod_qp]['indice_producao']),
                    'op_fechada': int(totvs_indicators[cod_qp]['op_fechada']),
                    'sc_total': int(totvs_indicators[cod_qp]['sc_total']),
                    'pc_total': int(totvs_indicators[cod_qp]['pc_total']),
                    'indice_compra': float(totvs_indicators[cod_qp]['indice_compra']),
                    'mat_entregue': int(totvs_indicators[cod_qp]['mat_entregue']),
                    'indice_recebimento': float(totvs_indicators[cod_qp]['indice_recebimento']),
                    'custo_total_mp_pc': float(totvs_indicators[cod_qp]['custo_total_mp_pc']),
                    'custo_mp_pc': float(totvs_indicators[cod_qp]['custo_mp_pc']),
                    'custo_item_com_pc': float(totvs_indicators[cod_qp]['custo_item_com_pc']),
                    'custo_mp_mat_entregue': float(totvs_indicators[cod_qp]['custo_mp_mat_entregue']),
                    'custo_item_com_mat_entregue': float(totvs_indicators[cod_qp]['custo_item_com_mat_entregue'])
                })
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                error_message = f"Error saving indicators for cod_qp {cod_qp}: {e}"
                logging.error(error_message)
                send_email("API Error - save_indicators", error_message)
    except Exception as e:
        error_message = f"General error in save_indicators: {e}"
        logging.error(error_message)
        send_email("API Error - save_indicators", error_message)


def get_all_data_conclusao(qp_number):
    try:
        query = text(f"""
            SELECT dt_completed_qp
            FROM enaplic_management.dbo.tb_qps
            WHERE cod_qp = :qp
        """)
        result = db.session.execute(query, {'qp': qp_number}).fetchone()
        return result[0] if result else None
    except Exception as e:
        error_message = f"Error to get_all_data_conclusao() tb_qps QPs for {qp_number}: {e}"
        logging.error(error_message)
        send_email("API Error - get_all_data_conclusao()", error_message)
        return None


def update_all_qps_table(data_proj_indicator: Dict[str, Any], status_qp: str) -> None:
    delete_qp_by_status(status_qp, data_proj_indicator)
    for cod_qp, qp_indicators in data_proj_indicator.items():
        try:
            prazo_de_entrega = qp_indicators['prazo_entrega_qp']
            if pd.isnull(prazo_de_entrega) or prazo_de_entrega == '':
                prazo_de_entrega = ''
                intervalo_de_dias = 0
                status_entrega = 'SEM REFERÊNCIA DE ENTREGA'
            else:
                intervalo_de_dias = (pd.to_datetime(prazo_de_entrega, dayfirst=True) - datetime.now()).days + 1
                if intervalo_de_dias >= 0:
                    status_entrega = 'EM DIA'
                else:
                    status_entrega = 'ATRASADO'
            query_params = {
                'qp': cod_qp,
                'description': qp_indicators['description'],
                'data_emissao': qp_indicators['data_emissao_qp'],
                'prazo_entrega': prazo_de_entrega,
                'intervalo_de_dias': intervalo_de_dias,
                'status_entrega': status_entrega
            }
            if not find_qp_by_cod_qp(cod_qp, status_qp) and status_qp == 'open':
                insert_params = query_params.copy()
                insert_params['status_qp'] = 'A'
                insert_open_qps_query = text(f"""
                            INSERT INTO 
                                enaplic_management.dbo.tb_qps 
                                (cod_qp, des_qp, status_qp, dt_open_qp, dt_end_qp, vl_delay, status_delivery)
                            VALUES
                                (:qp, :description, :status_qp, :data_emissao, :prazo_entrega, :intervalo_de_dias, :status_entrega);
                            """)
                db.session.execute(insert_open_qps_query, insert_params)
                db.session.commit()
            elif find_qp_by_cod_qp(cod_qp, status_qp) and status_qp == 'open':
                update_open_qps_query = text(f"""
                    UPDATE 
                        enaplic_management.dbo.tb_qps 
                    SET
                        des_qp = :description, 
                        dt_open_qp = :data_emissao,
                        dt_end_qp = :prazo_entrega,
                        vl_delay = :intervalo_de_dias,
                        status_delivery = :status_entrega
                    WHERE
                        cod_qp = :qp;
                """)
                db.session.execute(update_open_qps_query, query_params)
                db.session.commit()
            if status_qp == 'closed':
                insert_params = query_params.copy()
                insert_params['status_qp'] = 'F'
                insert_params['status_entrega'] = 'SEM DATA DE ENTREGA'
                if not find_qp_by_cod_qp(cod_qp, status_qp):
                    insert = text(f"""
                                INSERT INTO 
                                    enaplic_management.dbo.tb_qps 
                                    (cod_qp, des_qp, status_qp, dt_open_qp, dt_end_qp, vl_delay, status_delivery)
                                VALUES
                                    (:qp, :description, :status_qp, :data_emissao, :prazo_entrega, :intervalo_de_dias, :status_entrega);
                                """)
                    db.session.execute(insert, insert_params)
                    db.session.commit()
                else:
                    update_params = query_params.copy()
                    update_params['status_entrega'] = 'SEM DATA DE ENTREGA'
                    data_de_entrega = get_all_data_conclusao(cod_qp)
                    if data_de_entrega is not None:
                        intervalo_de_dias_com_data_entrega = (
                                                                     pd.to_datetime(prazo_de_entrega, dayfirst=True) -
                                                                     pd.to_datetime(data_de_entrega,
                                                                                    dayfirst=True)).days + 1
                        if not pd.isnull(intervalo_de_dias_com_data_entrega):
                            if intervalo_de_dias_com_data_entrega >= 0:
                                update_params['status_entrega'] = 'ENTREGUE NO PRAZO'
                            else:
                                update_params['status_entrega'] = 'ENTREGUE EM ATRASO'
                            intervalo_de_dias = intervalo_de_dias_com_data_entrega
                    else:
                        data_de_entrega = ''

                    update_params['intervalo_de_dias'] = intervalo_de_dias
                    update_params['data_de_entrega'] = data_de_entrega

                    update = text(f"""
                                UPDATE 
                                    enaplic_management.dbo.tb_qps 
                                SET
                                    des_qp = :description, 
                                    dt_open_qp = :data_emissao,
                                    dt_end_qp = :prazo_entrega,
                                    dt_completed_qp = :data_de_entrega,
                                    vl_delay = :intervalo_de_dias, 
                                    status_delivery = :status_entrega
                                WHERE
                                    cod_qp = :qp;
                                """)
                    db.session.execute(update, update_params)
                    db.session.commit()
        except Exception as e:
            db.session.rollback()
            error_message = f"Error inserting {status_qp} QP {cod_qp}: {e}"
            logging.error(error_message)
            send_email("API Error - update_all_qps_table", error_message)


def get_project_data(excel_file_name) -> Dict[str, Any]:
    global qp
    try:
        dataframe = get_sharepoint_project_data(excel_file_name)
        if dataframe is not None:
            total_rows = len(dataframe)
            chunk_size = 9
            dataframe_dict = {}
            qps_description_dict = {}

            for i in range(0, total_rows, chunk_size):
                chunk_df = dataframe.iloc[i:i + chunk_size]
                cod_qp = [format_qp(cell) for cell in chunk_df["QP_CLIENTE"]]

                dataframe_dict[cod_qp[0]] = chunk_df
                qps_description_dict.update(
                    {code: clean_string(cell) for code, cell in zip(cod_qp, chunk_df["QP_CLIENTE"])})

            data_proj_indicator = {}
            for qp, description in qps_description_dict.items():
                df = dataframe_dict[qp]

                baseline = df[df['ITEM'] == 'BASELINE']['GERAL'].values[0]  # REMOVER
                desconsiderar = df[df['ITEM'] == 'DESCONSIDERAR']['GERAL'].values[0] * -1  # REMOVER
                indice_mudanca = round((desconsiderar / baseline) * 100, 2) if baseline != 0 else 0  # REMOVER

                duracao_proj = df[df['ITEM'] == 'BASELINE']['DURACAO'].values[0]  # REMOVER
                duracao_proj = 0 if pd.isna(duracao_proj) else duracao_proj  # REMOVER

                data_inicio_proj = format_date_sharepoint(
                    df[df['ITEM'] == 'BASELINE']['DATA_INICIO_PROJ'].values[0])  # REMOVER
                data_fim_proj = format_date_sharepoint(
                    df[df['ITEM'] == 'BASELINE']['DATA_FIM_PROJ'].values[0])  # REMOVER
                data_inicio_proj = '' if data_inicio_proj == '00/01/1900' else data_inicio_proj  # REMOVER
                data_fim_proj = '' if data_fim_proj == '00/01/1900' else data_fim_proj  # REMOVER

                status_proj = map_status_proj(df[df['ITEM'] == 'BASELINE']['STATUS_PROJETO'].values[0])

                data_emissao_qp = format_date_sharepoint(
                    df[df['ITEM'] == 'BASELINE']['DATA_EMISSAO'].values[0])  # REMOVER
                prazo_entrega_qp = format_date_sharepoint(
                    df[df['ITEM'] == 'BASELINE']['PRAZO_ENTREGA'].values[0])  # REMOVER
                data_proj_indicator[qp] = {
                    "qp": qp,
                    "description": description,
                    "baseline": baseline,
                    "desconsiderar": desconsiderar,
                    "indice_mudanca": indice_mudanca,
                    "projeto_liberado": df[df['ITEM'] == 'PROJETO']['GERAL'].values[0],
                    "projeto_pronto": df[df['ITEM'] == 'PRONTO']['GERAL'].values[0] * -1,
                    "em_ajuste": df[df['ITEM'] == 'AJUSTE']['GERAL'].values[0] * -1,
                    "data_emissao_qp": data_emissao_qp,
                    "prazo_entrega_qp": prazo_entrega_qp,
                    "data_inicio_proj": data_inicio_proj,
                    "data_fim_proj": data_fim_proj,
                    "duracao_proj": duracao_proj,
                    "status_proj": status_proj,
                    "quant_mp_proj": df[df['ITEM'] == 'PRONTO']['MP'].values[0] * -1,
                    "quant_pi_proj": df[df['ITEM'] == 'PRONTO']['PI'].values[0] * -1
                }
            return data_proj_indicator
        else:
            raise Exception("Dataframe ETL Error")
    except Exception as e:
        error_message = f"Error fetching project data on SHAREPOINT: {e} + QP: {qp}"
        logging.error(error_message)
        send_email("API Error - get_project_data - SHAREPOINT", error_message)
        return {}


def find_qp_by_status_qp(qp_status) -> list:
    try:
        query = text(f"""
            SELECT 
                * 
            FROM 
                enaplic_management.dbo.tb_qps
            WHERE
                status_qp = '{status[qp_status]}';
        """)
        result = db.session.execute(query).fetchall()
        return result
    except Exception as e:
        error_message = f"Error fetching QPs: {e}"
        logging.error(error_message)
        send_email("API Error - find_qp_by_status_qp()", error_message)
        return []


def find_qp_by_cod_qp(qp: str, status_qp: str) -> bool:
    try:
        query = text(f"""
            SELECT 1 
            FROM enaplic_management.dbo.tb_qps
            WHERE cod_qp = :qp 
            AND status_qp = :status_qp
        """)
        result = db.session.execute(query, {'qp': qp, 'status_qp': status[status_qp]}).fetchone()
        return result is not None
    except Exception as e:
        error_message = f"Error checking tb_qps QPs for {qp}: {e}"
        logging.error(error_message)
        send_email("API Error - find_qp_by_cod_qp()", error_message)
        return False


def find_all_qp():
    try:
        query = text("SELECT * FROM enaplic_management.dbo.tb_qps")
        return db.session.execute(query).fetchall()
    except Exception as ex:
        error_message = f"Error to find all QPs: {ex}"
        logging.error(error_message)
        send_email("API Error - find_all_qp()", error_message)


def find_all_indicators():
    try:
        query = text("SELECT * FROM enaplic_management.dbo.tb_dashboard_indicators")
        return db.session.execute(query).fetchall()
    except Exception as ex:
        error_message = f'Error to find all Baseline Indicators: {ex}'
        logging.error(error_message)
        send_email("API Error - find_all_indicators()", error_message)


def find_open_qrs():
    try:
        query = text(f"""	
            -- QUERY PARA PROJETAR INFORMAÇÕES REFERENTE AOS PEDIDOS DE VENDA E NOTAS FISCAIS DE SAÍDA
            SELECT
                C6_NUM AS 'QR',
                C5_ZZNOME AS 'CLIENTE',
                C6_PRODUTO AS 'CÓDIGO',
                C6_DESCRI AS 'DESCRIÇÃO',
                C6_UM AS 'UN.', 
                C6_QTDVEN AS 'QTD. VENDA',
                C1_NUM AS 'SOLIC. COMPRA',
                C2_NUM AS 'OP',
                C5_EMISSAO 'PV ABERTO EM:',
                C6_ENTREG AS 'DATA DE ENTREGA'
            FROM 
                PROTHEUS12_R27.dbo.SC6010 itemPedidoVenda
            LEFT JOIN
                PROTHEUS12_R27.dbo.SD2010 itemNotaFiscalSaida
            ON
                itemPedidoVenda.C6_NUM = itemNotaFiscalSaida.D2_PEDIDO 
                AND itemPedidoVenda.C6_PRODUTO = itemNotaFiscalSaida.D2_COD
                AND	itemPedidoVenda.D_E_L_E_T_ = itemNotaFiscalSaida.D_E_L_E_T_
            INNER JOIN
                PROTHEUS12_R27.dbo.SC5010 cabecalhoPedidoVenda
            ON
                itemPedidoVenda.C6_NUM = cabecalhoPedidoVenda.C5_NUM
            LEFT JOIN
                SC1010 tabelaSolicCompras
            ON
                itemPedidoVenda.C6_NUM = tabelaSolicCompras.C1_ZZNUMQP
                AND itemPedidoVenda.C6_PRODUTO = tabelaSolicCompras.C1_PRODUTO
                AND	itemPedidoVenda.D_E_L_E_T_ = tabelaSolicCompras.D_E_L_E_T_
            LEFT JOIN 
                PROTHEUS12_R27.dbo.SC2010 tabelaOrdemDeProducao
            ON
                itemPedidoVenda.C6_NUM = tabelaOrdemDeProducao.C2_ZZNUMQP
                AND itemPedidoVenda.C6_PRODUTO = tabelaOrdemDeProducao.C2_PRODUTO
                AND itemPedidoVenda.D_E_L_E_T_ = tabelaOrdemDeProducao.D_E_L_E_T_ 
            WHERE 
                C6_XTPOPER LIKE '2%' -- C6_XTPOPER = 1 (QP) / 2 (QR) / 3 (ND - OUTROS)
                AND cabecalhoPedidoVenda.C5_NOTA <> 'XXXXXXXXX'
                AND D2_DOC IS NULL -- PV (ABERTO)
                AND	itemPedidoVenda.D_E_L_E_T_ <> '*'
            ORDER BY 
                itemPedidoVenda.R_E_C_N_O_ ASC;
        """)
        return db.session.execute(query).fetchall()
    except Exception as e:
        error_message = f'Error to find open QRs: {e}'
        logging.error(error_message)
        send_email("API Error - find_open_qrs()", error_message)


def find_open_sc():
    data_atual = datetime.now()
    data_modificada = data_atual - relativedelta(months=4)
    data_limite_inferior = data_modificada.strftime("%Y%m%d")
    try:
        query = text(f"""
            SELECT
                C1_ZZNUMQP AS [QP/QR],
                C1_NUM AS 'SOLIC. COMPRA',
                C1_PRODUTO AS 'CÓDIGO',
                C1_DESCRI AS 'DESCRIÇÃO',
                C1_UM AS 'UN.',
                C1_QUANT AS 'QUANTIDADE',
                C1_ITEM AS 'ITEM',
                C1_EMISSAO AS 'EMISSÃO',
                C1_OBS AS 'OBSERVAÇÃO',
                US.USR_NOME AS 'SOLICITANTE'
            FROM 
                PROTHEUS12_R27.dbo.SC1010 SC
            LEFT JOIN
                PROTHEUS12_R27.dbo.SYS_USR US
            ON
                C1_SOLICIT = US.USR_CODIGO 
                AND US.D_E_L_E_T_ <> '*'
            WHERE
                SC.D_E_L_E_T_ <> '*'
                AND C1_PEDIDO = '      '
                AND C1_EMISSAO >= '{data_limite_inferior}'
            ORDER BY 
                C1_NUM ASC;
        """)
        return db.session.execute(query).fetchall()
    except Exception as e:
        error_message = f'Error to find open SC: {e}'
        logging.error(error_message)
        send_email("API Error - find_open_sc()", error_message)


def send_email_notification_sc(status_url: str):
    try:
        if status_url == 'open':
            row_all_qr = find_open_sc()
            if row_all_qr:
                dataframe = pd.DataFrame(row_all_qr)
            else:
                raise Exception("Não foi encontrada nenhuma Solic. de Compras durante a consulta.")
            subject_title = "🦾🤖 Eureka® BOT - Notificação de Solicitação de Compras em Aberto 🛒🟢"
            if status_url == 'open' and not dataframe.empty:
                num_qrs = dataframe['SOLIC. COMPRA'].nunique()
                dataframe = formatar_dataframe_solic_compra(dataframe)
                status_message = f"""
                    <p>🔔 Identifiquei <strong>{num_qrs}</strong> solicitações de compra <u>em aberto</u> no sistema.<p>
                    <p>📅 O período de consulta foi de <u><b>4 meses</b></u>.</p>
                    <p>📋 Recomendo atenção aos prazos para garantir a entrega pontual dos produtos aos clientes.</p>"""
                message = generate_email_body(dataframe, "Solicitação de Compras em aberto 🛒🟢", status_message)

            if dataframe.empty:
                raise Exception(f"Não há registros que atendam à condição para a operação {status_url}.")

            send_email(subject_title, message, dataframe=dataframe, operation='sc', status=status_url)
            return True, "✔️ Serviço de notificação por e-mail executado com sucesso!"
    except Exception as ex:
        return False, str(ex)


def send_email_notification_qr(status_url: str):
    try:
        if status_url == 'open':
            row_all_qr = find_open_qrs()
            if row_all_qr:
                dataframe = pd.DataFrame(row_all_qr)
            else:
                raise Exception("Não foi encontrada nenhuma QR durante a consulta.")
            subject_title = "🦾🤖 Eureka® BOT - Notificação de Status de QR 🛒🕗"
            if status_url == 'open' and not dataframe.empty:
                num_qrs = dataframe['QR'].nunique()
                dataframe = formatar_dataframe_qrs(dataframe)
                status_message = f"""
                    <p>🔔 Identifiquei <strong>{num_qrs}</strong> QR(s) <u>em aberto</u> no sistema.<br>
                    <br>📋 Recomenda-se atenção aos prazos para garantir a entrega pontual aos clientes.</p>"""
                message = generate_email_body(dataframe, "QR(s) em aberto 🟢⏰📅", status_message)

            if dataframe.empty:
                raise Exception(f"Não há registros que atendam à condição para a operação {status_url}.")

            send_email(subject_title, message, dataframe=dataframe, operation='qr', status=status_url)
            return True, "✔️ Serviço de notificação por e-mail executado com sucesso!"
    except Exception as ex:
        return False, str(ex)


def send_email_notification_qp(status_url: str):
    try:
        rows_all_qps = find_all_qp()
        rows_all_indicators = find_all_indicators()
        if rows_all_qps and rows_all_indicators:
            dataframe = pd.DataFrame(rows_all_qps)
            dataframe_all_indicators = pd.DataFrame(rows_all_indicators)
            dataframe = dataframe.merge(dataframe_all_indicators[['cod_qp', 'status_proj', 'vl_proj_duration']],
                                        on='cod_qp', how='left')
        else:
            raise Exception("Não foi encontrada nenhuma QP durante a consulta.")

        subject_title = "🦾🤖 Eureka® BOT - Notificação de Status de QP 🕗"
        if status_url == 'open_late':
            dataframe = dataframe[(dataframe['status_qp'] == 'A') & (dataframe['vl_delay'] < 0)]

            if not dataframe.empty:
                num_qps = len(dataframe)
                dataframe = formatar_dataframe_qps(dataframe, status_url)
                status_message = f"""
                    <p>🚨 Identifiquei <strong>{num_qps}</strong> QP(s) <u>em atraso quanto ao prazo de entrega</u>.
                    </p>"""
                message = generate_email_body(dataframe, "QP(s) abertas em atraso 🟢⏰📅", status_message)

        elif status_url == 'open_up_to_date':
            dataframe = dataframe[
                (dataframe['status_qp'] == 'A') & (dataframe['vl_delay'] >= 0) & (dataframe['vl_delay'] <= 30)]

            if not dataframe.empty:
                num_qps = len(dataframe)
                dataframe = formatar_dataframe_qps(dataframe, status_url)
                status_message = f"""
                    <p>⏰ Identifiquei <strong>{num_qps}</strong> QP(s) <u>próximas do prazo de entrega</u>.</p>"""
                message = generate_email_body(dataframe, "QP(s) abertas em dia 🟢📅", status_message)

        elif status_url == 'closed_no_date':
            dataframe = dataframe[(dataframe['status_qp'] == 'F') & (dataframe['vl_delay'] < 0) & (
                    dataframe['status_delivery'] == 'SEM DATA DE ENTREGA')]

            if not dataframe.empty:
                num_qps = len(dataframe)
                dataframe = formatar_dataframe_qps(dataframe, status_url)
                status_message = f"""
                    <p>✅ Identifiquei <strong>{num_qps}</strong> QP(s) <u>finalizada(s) com produto entregue ao cliente</u>.
                    <br>⚠️ <strong>Data de entrega não preenchida</strong> no módulo <u><b>Gestão de QPs</b></u> do Eureka®.
                    <br>📋 Recomenda-se o preenchimento para manter o histórico completo e possibilitar análises 
                    precisas.</p>"""
                message = generate_email_body(dataframe, "QP(s) finalizadas sem data de entrega 🔴📅", status_message)

        if dataframe.empty:
            raise Exception(f"Não há registros que atendam à condição para a operação {status_url}.")

        send_email(subject_title, message, dataframe=dataframe, operation='qp', status=status_url)
        return True, "✔️ Serviço de notificação por e-mail executado com sucesso!"
    except Exception as ex:
        return False, str(ex)


def generate_email_body(df: pd.DataFrame, description: str, status_message: str) -> str:
    image_path = Path(__file__).resolve().parent.parent.parent / "assets" / "images" / "logo_enaplic.jpg"
    with open(image_path, "rb") as image_file:
        encoded_image = base64.b64encode(image_file.read()).decode('utf-8')

    df_html = df.to_html(index=False, border=0, justify='center', classes='table table-striped')

    body = f"""
    <html>
    <head>
        <style>
            .table {{
                width: 100%;
                border-collapse: collapse;
            }}
            .table-striped tbody tr:nth-of-type(odd) {{
                background-color: #f9f9f9;
            }}
            th, td {{
                text-align: center;
                padding: 8px;
                border: 1px solid #dddddd;
            }}
        </style>
    </head>
    <body>
        <h2>{description}</h2>
        <p>🤖 Bom dia!</p>
        <p>🤖 Espero que esteja bem!</p>
        {status_message}
        <br>
        {df_html}
        <br>
        <p>🤖 Tenha um excelente dia!</p>
        <p>Atenciosamente,</p>
        <p><strong>🦾🤖 Eureka® BOT</strong></p>
        <p>👨‍💻 <i>Este e-mail foi gerado automaticamente e não há necessidade de respondê-lo.</i></p>
        <img src="data:image/jpeg;base64,{encoded_image}" alt="Enaplic logo" width="400px">
    </body>
    </html>
    """

    return body


def delete_qp_by_status(status_qp: str, data_sharepoint_qp_files: Dict[str, Any]):
    qps_table = find_qp_by_status_qp(status_qp)

    cod_qps_database = [row[1] for row in qps_table]
    cod_qps_sharepoint = data_sharepoint_qp_files.keys()

    cod_qps_to_be_removed = []

    for cod_qp in cod_qps_database:
        if cod_qp not in cod_qps_sharepoint:
            cod_qps_to_be_removed.append(cod_qp)

    for cod_qp in cod_qps_sharepoint:
        if cod_qp not in cod_qps_database:
            cod_qps_to_be_removed.append(cod_qp)

    for qp_to_remove in cod_qps_to_be_removed:
        try:
            query = text(f"""
            DELETE FROM 
                enaplic_management.dbo.tb_qps 
            WHERE
                cod_qp = :cod_qp
            """)
            db.session.execute(query, {'cod_qp': qp_to_remove})
        except Exception as ex:
            error_message = f"Error to delete QPs from tb_qps: {ex}"
            logging.error(error_message)
            send_email("API Error - delete_qp_by_status()", error_message)


def clean_string(input_string: str) -> str:
    return input_string[8:].replace('-', '').replace('_NOVA_VERSÃO', '').replace('.xlsm', '').strip().upper()


def map_status_proj(status_proj: str) -> str:
    status_mapping = {
        'Finalizado': 'F',
        'Em andamento': 'A',
        'Não iniciado': 'N'
    }
    return status_mapping[status_proj]


def format_qp(qp_number: str) -> str:
    return qp_number.split('-')[1].replace('E', '').strip().zfill(6)


def format_date_sharepoint(date: Any) -> str:
    if pd.isnull(date):
        return ''
    return pd.to_datetime(date).strftime('%d/%m/%Y')


def format_date_db_sqlserver(date: str):
    date_obj = datetime.strptime(date, "%Y%m%d")
    return date_obj.strftime("%d/%m/%Y")


def formatar_dataframe_qps(dataframe: pd.DataFrame, operation: str) -> DataFrame:
    dataframe = dataframe.drop(columns=['id', 'S_T_A_M_P'])
    dataframe['cod_qp'] = dataframe['cod_qp'].astype(str).str.lstrip('0')
    dataframe = dataframe.rename(columns={
        'cod_qp': 'QP',
        'des_qp': 'PROJETO',
        'status_qp': 'STATUS',
        'dt_open_qp': 'DATA DE EMISSÃO',
        'dt_end_qp': 'PRAZO DE ENTREGA',
        'vl_delay': 'SALDO (EM DIAS)',
        'status_delivery': 'STATUS DE ENTREGA'
    })
    if operation != 'closed_no_date':
        dataframe = dataframe.drop(columns=['dt_completed_qp'])
        dataframe['STATUS'] = dataframe['STATUS'].replace('A', 'ABERTO')
        dataframe['status_proj'] = dataframe['status_proj'].replace({'N': 'NÃO INICIADO', 'A': 'EM ANDAMENTO',
                                                                     'F': 'FINALIZADO'})
        dataframe = dataframe.rename(columns={
            'status_proj': 'STATUS PROJETO',
            'vl_proj_duration': 'DURAÇÃO PROJETO (EM DIAS)'
        })
        new_order_columns = ['QP', 'PROJETO', 'STATUS', 'DATA DE EMISSÃO', 'PRAZO DE ENTREGA', 'SALDO (EM DIAS)',
                             'STATUS PROJETO', 'DURAÇÃO PROJETO (EM DIAS)', 'STATUS DE ENTREGA']
        dataframe = dataframe.reindex(columns=new_order_columns)
    elif operation == 'closed_no_date':
        dataframe = dataframe.rename(columns={'dt_completed_qp': 'DATA DE ENTREGA'})
        dataframe['STATUS'] = dataframe['STATUS'].replace('F', 'FINALIZADA')
        dataframe = dataframe.drop(columns=['status_proj', 'vl_proj_duration'])
    return dataframe


def format_number(value):
    value = float(value)
    if value.is_integer():
        return int(value)
    else:
        return locale.format_string("%.2f", value, grouping=True)


def formatar_dataframe_qrs(df: pd.DataFrame):
    df = df.fillna('')

    columns_remove_left_zeros = ['QR', 'SOLIC. COMPRA', 'OP']
    df[columns_remove_left_zeros] = df[columns_remove_left_zeros].apply(lambda x: x.str.lstrip('0'))

    columns_format_date = ['PV ABERTO EM:', 'DATA DE ENTREGA']
    df[columns_format_date] = df[columns_format_date].apply(lambda x: x.apply(format_date_db_sqlserver))

    df['DATA DE ENTREGA'] = (
        pd.to_datetime(df['DATA DE ENTREGA'], dayfirst=True, format='%d/%m/%Y').dt.strftime('%d/%m/%Y'))

    today = datetime.now()
    df.insert(8, 'SALDO (EM DIAS)', '')
    df['SALDO (EM DIAS)'] = ((pd.to_datetime(df['DATA DE ENTREGA'], dayfirst=True, format='%d/%m/%Y') - today)
                             .dt.days + 1)

    df.loc[df['DATA DE ENTREGA'].isna(), 'SALDO (EM DIAS)'] = pd.NA

    number_columns = ['QTD. VENDA', 'SALDO (EM DIAS)']
    df[number_columns] = df[number_columns].apply(lambda x: x.apply(format_number))

    df['STATUS'] = df['SALDO (EM DIAS)'].apply(
        lambda saldo: 'EM ATRASO' if saldo < 0 else 'EM DIA' if pd.notna(saldo) else pd.NA
    )

    return df


def formatar_dataframe_solic_compra(df: pd.DataFrame):
    df = df.fillna('')

    df.insert(0, 'id', range(1, len(df) + 1))

    columns_remove_left_zeros = ['QP/QR', 'SOLIC. COMPRA']
    df[columns_remove_left_zeros] = df[columns_remove_left_zeros].apply(lambda x: x.str.lstrip('0'))

    columns_format_date = ['EMISSÃO']
    df[columns_format_date] = df[columns_format_date].apply(lambda x: x.apply(format_date_db_sqlserver))

    today = datetime.now()
    df.insert(len(df.columns), 'DIAS SC ABERTA SEM PEDIDO', '')
    df['DIAS SC ABERTA SEM PEDIDO'] = ((today - pd.to_datetime(df['EMISSÃO'], dayfirst=True, format='%d/%m/%Y'))
                                       .dt.days)

    number_columns = ['QUANTIDADE', 'DIAS SC ABERTA SEM PEDIDO']
    df[number_columns] = df[number_columns].apply(lambda x: x.apply(format_number))

    return df

from app import db
from sqlalchemy import text
from app.extensions.sharepoint_project_data import get_sharepoint_project_data
import pandas as pd
from typing import Dict, Any
import logging
from app.extensions.email_service import send_email

logging.basicConfig(level=logging.DEBUG)

open_qps_table = "tb_open_qps"
indicators_table = "tb_dashboard_indicators"
current_indicators_table = "tb_current_dashboard_indicators"
indicators_table_list = ["tb_dashboard_indicators", "tb_current_dashboard_indicators"]


def get_all_indicators() -> Dict[str, Any]:
    cod_qps = fetch_all_open_qps()
    data = {}

    indicators = {
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


def get_all_totvs_indicators() -> Dict[str, Any]:
    cod_qps = fetch_all_open_qps()
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
            error_message = f"Error retrieving TOTVS indicators for cod_qp {cod_qp_formatado}: {e}"
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
                    (cod_qp, des_qp, dt_open_qp, dt_end_qp, status_proj, 
                     vl_proj_all_prod, vl_proj_prod_cancel, vl_proj_modify_perc, 
                     vl_proj_released, vl_proj_finished, vl_proj_adjusted, 
                     vl_proj_pi, vl_proj_mp, vl_all_op, vl_pcp_perc, vl_closed_op, 
                     vl_product_perc, vl_all_sc, vl_all_pc, vl_compras_perc, 
                     vl_mat_received, vl_mat_received_perc, vl_total_mp_pc_cost, vl_mp_pc_cost, 
                     vl_com_pc_cost, vl_mp_deliver_cost, vl_com_deliver_cost) 
                VALUES
                    (:qp, :description, :data_emissao_qp, :prazo_entrega_qp, :status_proj, 
                     :baseline, :desconsiderar, :indice_mudanca, 
                     :projeto_liberado, :projeto_pronto, :em_ajuste, 
                     :quant_pi_proj, :quant_mp_proj, :op_total, :indice_pcp, :op_fechada, 
                     :indice_producao, :sc_total, :pc_total, :indice_compra, 
                     :mat_entregue, :indice_recebimento, :custo_total_mp_pc, :custo_mp_pc,
                     :custo_item_com_pc, :custo_mp_mat_entregue, :custo_item_com_mat_entregue);
                """)


def find_all_indicators():
    project_data = get_project_data()
    update_open_qps_table(project_data)
    totvs_indicators = get_all_totvs_indicators()

    return project_data, totvs_indicators


def save_indicators(project_data, totvs_indicators) -> None:
    try:
        for table in indicators_table_list:
            if table == 'tb_current_dashboard_indicators':
                db.session.execute(text(f"TRUNCATE TABLE enaplic_management.dbo.{table}"))

            for cod_qp, project_indicators in project_data.items():
                try:
                    query = insert_query(table)

                    op_total = int(totvs_indicators[cod_qp]['op_total'])
                    quant_pi_proj = int(project_indicators['quant_pi_proj'])

                    indice_pcp = round((op_total / quant_pi_proj) * 100, 2) if quant_pi_proj > 0 else 0

                    db.session.execute(query, {
                        'qp': cod_qp,
                        'description': project_indicators['description'],
                        'data_emissao_qp': project_indicators['data_emissao_qp'],
                        'prazo_entrega_qp': project_indicators['prazo_entrega_qp'],
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


def update_open_qps_table(data_proj_indicator: Dict[str, Any]) -> None:
    delete_open_qps_table()
    for cod_qp, qp_indicators in data_proj_indicator.items():
        try:
            if not get_open_qps(cod_qp):
                insert_open_qps_query = text(f"""
                            INSERT INTO 
                                enaplic_management.dbo.{open_qps_table} 
                                (cod_qp, des_qp, dt_open_qp, dt_end_qp)
                            VALUES(:qp, :description, :data_emissao, :prazo_entrega);
                            """)

                db.session.execute(insert_open_qps_query, {
                    'qp': cod_qp,
                    'description': qp_indicators['description'],
                    'data_emissao': qp_indicators['data_emissao_qp'],
                    'prazo_entrega': qp_indicators['prazo_entrega_qp']
                })
                db.session.commit()
        except Exception as e:
            db.session.rollback()
            error_message = f"Error inserting open QP {cod_qp}: {e}"
            logging.error(error_message)
            send_email("API Error - update_open_qps_table", error_message)


def delete_open_qps_table():
    try:
        db.session.execute(text(f"TRUNCATE TABLE enaplic_management.dbo.{open_qps_table}"))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        error_message = f'Error truncating open QPS table: {e}'
        logging.error(error_message)
        send_email("API Error - delete_open_qps_table", error_message)


def get_project_data() -> Dict[str, Any]:
    try:
        dataframe = get_sharepoint_project_data()

        total_rows = len(dataframe)
        chunk_size = 9
        dataframe_dict = {}
        qps_description_dict = {}

        for i in range(0, total_rows, chunk_size):
            chunk_df = dataframe.iloc[i:i + chunk_size]
            cod_qp = [format_qp(cell) for cell in chunk_df["QP_CLIENTE"]]

            dataframe_dict[cod_qp[0]] = chunk_df
            qps_description_dict.update({code: clean_string(cell) for code, cell in zip(cod_qp, chunk_df["QP_CLIENTE"])})

        data_proj_indicator = {}
        for qp, description in qps_description_dict.items():
            df = dataframe_dict[qp]

            status_proj = map_status_proj(df[df['ITEM'] == 'BASELINE']['STATUS_PROJETO'].values[0])
            baseline = df[df['ITEM'] == 'BASELINE']['GERAL'].values[0]
            desconsiderar = df[df['ITEM'] == 'DESCONSIDERAR']['GERAL'].values[0] * -1
            indice_mudanca = round((desconsiderar / baseline) * 100, 2) if baseline != 0 else 0

            data_proj_indicator[qp] = {
                "qp": qp,
                "description": description,
                "baseline": baseline,
                "desconsiderar": desconsiderar,
                "indice_mudanca": indice_mudanca,
                "projeto_liberado": df[df['ITEM'] == 'PROJETO']['GERAL'].values[0],
                "projeto_pronto": df[df['ITEM'] == 'PRONTO']['GERAL'].values[0] * -1,
                "em_ajuste": df[df['ITEM'] == 'AJUSTE']['GERAL'].values[0] * -1,
                "data_emissao_qp": format_date(df[df['ITEM'] == 'BASELINE']['DATA_EMISSAO'].values[0]),
                "prazo_entrega_qp": format_date(df[df['ITEM'] == 'BASELINE']['PRAZO_ENTREGA'].values[0]),
                "status_proj": status_proj,
                "quant_mp_proj": df[df['ITEM'] == 'PRONTO']['MP'].values[0] * -1,
                "quant_pi_proj": df[df['ITEM'] == 'PRONTO']['PI'].values[0] * -1
            }

        return data_proj_indicator
    except Exception as e:
        error_message = f"Error fetching project data on SHAREPOINT: {e}"
        logging.error(error_message)
        send_email("API Error - get_project_data - SHAREPOINT", error_message)
        return {}


def fetch_all_open_qps() -> list:
    try:
        query_qps_em_aberto = text(f"""
            SELECT cod_qp 
            FROM enaplic_management.dbo.{open_qps_table};
        """)
        cod_qps = db.session.execute(query_qps_em_aberto).fetchall()
        return [row[0] for row in cod_qps]
    except Exception as e:
        error_message = f"Error fetching open QPs: {e}"
        logging.error(error_message)
        send_email("API Error - fetch_all_open_qps", error_message)
        return []


def get_open_qps(qp: str) -> bool:
    try:
        query_open = text(f"""
            SELECT 1 
            FROM enaplic_management.dbo.{open_qps_table}
            WHERE cod_qp = :qp
        """)
        result = db.session.execute(query_open, {'qp': qp}).fetchone()
        return result is not None
    except Exception as e:
        error_message = f"Error checking open QPs for {qp}: {e}"
        logging.error(error_message)
        send_email("API Error - get_open_qps", error_message)
        return False


def clean_string(input_string: str) -> str:
    return input_string[8:].replace('-', '').replace('_NOVA_VERSÃO', '').replace('.xlsm', '').strip().upper()


def map_status_proj(status: str) -> str:
    status_mapping = {
        'Finalizado': 'F',
        'Em andamento': 'A',
        'Não iniciado': 'N'
    }
    return status_mapping[status]


def format_qp(qp: str) -> str:
    return qp.split('-')[1].replace('E', '').strip().zfill(6)


def format_date(date: Any) -> str:
    if pd.isnull(date):
        return ''
    return pd.to_datetime(date).strftime('%d/%m/%Y')

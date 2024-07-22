from app import db
from sqlalchemy import text
from app.extensions.sharepoint_project_data import get_sharepoint_project_data
import pandas as pd

indicators_table = "tb_dashboard_indicators"
open_qps_table = "tb_open_qps"


def get_all_indicators():
    query_qps_em_andamento = text(
        f"SELECT cod_qp FROM enaplic_management.dbo.{open_qps_table} WHERE status_proj = 'A';")
    cod_qps = db.session.execute(query_qps_em_andamento).fetchall()
    cod_qps = [row[0] for row in cod_qps]

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
        "indice_pcp": "vl_pcp_perc",
        "indice_producao": "vl_product_perc",
        "indice_compra": "vl_compras_perc",
        "indice_recebimento": "vl_mat_received_perc"
    }

    for cod_qp in cod_qps:
        cod_qp_formatado = cod_qp.lstrip('0')
        data[cod_qp_formatado] = {}
        for key, indicator in indicators.items():
            data[cod_qp_formatado][key] = get_indicator_value(f"TOP 1 {indicator}",
                                                              f"enaplic_management.dbo.{indicators_table}",
                                                              f"cod_qp LIKE '%{cod_qp_formatado}' ORDER BY id DESC")

        # Adiciona os valores que dependem de contagens específicas
        data[cod_qp_formatado]["op_total"] = get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC2010",
                                                                 f"C2_ZZNUMQP LIKE '%{cod_qp_formatado}' AND D_E_L_E_T_ <> '*'")

        data[cod_qp_formatado]["op_fechada"] = get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC2010",
                                                                   f"C2_ZZNUMQP LIKE '%{cod_qp_formatado}' AND C2_DATRF <> '       ' AND D_E_L_E_T_ <> '*'")

        data[cod_qp_formatado]["sc_total"] = get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC1010",
                                                                 f"C1_ZZNUMQP LIKE '%{cod_qp_formatado}' AND D_E_L_E_T_ <> '*'")

        data[cod_qp_formatado]["pc_total"] = get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC7010",
                                                                 f"C7_ZZNUMQP LIKE '%{cod_qp_formatado}' AND D_E_L_E_T_ <> '*'")

        data[cod_qp_formatado]["mat_entregue"] = get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC7010",
                                                                     f"C7_ZZNUMQP LIKE '%{cod_qp_formatado}' AND C7_ENCER = 'E' AND D_E_L_E_T_ <> '*'")

    return percentage_indicators_calculate(data)


def get_all_totvs_indicators():
    query_qps_em_andamento = text(
        f"SELECT cod_qp FROM enaplic_management.dbo.{open_qps_table} WHERE status_proj = 'A';")
    cod_qps = db.session.execute(query_qps_em_andamento).fetchall()
    cod_qps = [row[0] for row in cod_qps]

    data = {}

    for cod_qp in cod_qps:
        cod_qp_formatado = cod_qp.lstrip('0')
        data[cod_qp_formatado] = {
            "op_total": get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC2010",
                                            f"C2_ZZNUMQP LIKE '%{cod_qp_formatado}' AND D_E_L_E_T_ <> '*'"),
            "op_fechada": get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC2010",
                                              f"C2_ZZNUMQP LIKE '%{cod_qp_formatado}' AND C2_DATRF <> '       ' AND D_E_L_E_T_ <> '*'"),
            "sc_total": get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC1010",
                                            f"C1_ZZNUMQP LIKE '%{cod_qp_formatado}' AND D_E_L_E_T_ <> '*'"),
            "pc_total": get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC7010",
                                            f"C7_ZZNUMQP LIKE '%{cod_qp_formatado}' AND D_E_L_E_T_ <> '*'"),
            "mat_entregue": get_indicator_value("COUNT(*)", "PROTHEUS12_R27.dbo.SC7010",
                                                f"C7_ZZNUMQP LIKE '%{cod_qp_formatado}' AND C7_ENCER = 'E' AND D_E_L_E_T_ <> '*'"),
        }

    return data


def get_indicator_value(select_clause, table_name, where_clause):
    query = text(f"SELECT {select_clause} AS value FROM {table_name} WHERE {where_clause};")
    result = db.session.execute(query).fetchone()

    return result[0] if result else 0


def percentage_indicators_calculate(data):
    # Verificações para evitar divisões por zero
    for cod_qp, values in data.items():

        if values['op_total'] != 0:
            indice_producao = (values['op_fechada'] / values['op_total']) * 100
        else:
            indice_producao = 0

        if values['sc_total'] != 0:
            indice_compra = (values['pc_total'] / values['sc_total']) * 100
        else:
            indice_compra = 0

        if values['pc_total'] != 0:
            indice_recebimento = (values['mat_entregue'] / values['pc_total']) * 100
        else:
            indice_recebimento = 0

        data[cod_qp]['indice_producao'] = round(indice_producao, 2)
        data[cod_qp]['indice_compra'] = round(indice_compra, 2)
        data[cod_qp]['indice_recebimento'] = round(indice_recebimento, 2)

    return data


def save_indicators():
    data = get_all_indicators()
    for cod_qp, values in data.items():
        cod_qp_formatted = cod_qp.zfill(6)
        insert_query = text(f"""
        INSERT INTO 
            enaplic_management.dbo.{indicators_table} 
            (cod_qp, vl_all_op, vl_closed_op, vl_product_perc, vl_all_sc, vl_all_pc, vl_compras_perc, vl_mat_received, vl_mat_received_perc) 
        VALUES 
            (:cod_qp, :vl_all_op, :vl_closed_op, :vl_product_perc, :vl_all_sc, :vl_all_pc, :vl_compras_perc, :vl_mat_received, :vl_mat_received_perc)
        """)

        db.session.execute(insert_query, {
            'cod_qp': cod_qp_formatted,
            'vl_all_op': values['op_total'],
            'vl_closed_op': values['op_fechada'],
            'vl_product_perc': values['indice_producao'],
            'vl_all_sc': values['sc_total'],
            'vl_all_pc': values['pc_total'],
            'vl_compras_perc': values['indice_compra'],
            'vl_mat_received': values['mat_entregue'],
            'vl_mat_received_perc': values['indice_recebimento']
        })

        db.session.commit()


def get_project_data():
    dataframe = get_sharepoint_project_data()

    total_rows = len(dataframe)  # Contar o número total de linhas
    chunk_size = 9  # Definir o tamanho de cada pedaço (chunk)
    dataframe_dict = {}
    open_qps = []

    # Dividir o DataFrame a cada 9 linhas e armazenar no dicionário
    for i in range(0, total_rows, chunk_size):
        chunk_df = dataframe.iloc[i:i + chunk_size]
        qp_client = chunk_df["QP_CLIENTE"]

        cod_qp = []
        for qp in qp_client:
            cod_qp_formatted = qp.split('-')[1].replace('E', '').strip().zfill(6)
            cod_qp.append(cod_qp_formatted)
            open_qps.append(cod_qp_formatted)

        dataframe_dict[cod_qp[0]] = chunk_df

    open_qps = set(open_qps)

    data_proj_indicator = {}
    for qp in open_qps:
        df = dataframe_dict[qp]

        status_proj = df[df['ITEM'] == 'BASELINE']['STATUS_PROJETO'].values[0]
        if status_proj == 'Finalizado':
            status_proj = 'F'
        elif status_proj == 'Em andamento':
            status_proj = 'A'
        elif status_proj == 'Não iniciado':
            status_proj = 'N'

        baseline = df[df['ITEM'] == 'BASELINE']['GERAL'].values[0]
        desconsiderar = df[df['ITEM'] == 'DESCONSIDERAR']['GERAL'].values[0] * -1,
        indice_mudanca = round((df[df['ITEM'] == 'DESCONSIDERAR']['GERAL'].values[0] /
                                 df[df['ITEM'] == 'BASELINE']['GERAL'].values[0]) * 100, 2)
        projeto_liberado = df[df['ITEM'] == 'PROJETO']['GERAL'].values[0]
        projeto_pronto = df[df['ITEM'] == 'PRONTO']['GERAL'].values[0] * -1
        em_ajuste = df[df['ITEM'] == 'AJUSTE']['GERAL'].values[0]
        data_emissao_qp = df[df['ITEM'] == 'BASELINE']['DATA_EMISSAO'].values[0] if not pd.isnull(
            df[df['ITEM'] == 'BASELINE']['DATA_EMISSAO'].values[0]) else "SEM DATA"
        prazo_entrega_qp = df[df['ITEM'] == 'BASELINE']['PRAZO_ENTREGA'].values[0] if not pd.isnull(
            df[df['ITEM'] == 'BASELINE']['PRAZO_ENTREGA'].values[0]) else "SEM DATA"
        status_proj = status_proj,
        quant_mp_proj = df[df['ROTULO_2'] == 'PRONTO']['MP'].values[0] * 1
        quant_pi_proj = df[df['ROTULO_3'] == 'PRONTO']['PI'].values[0] * 1

        data_proj_indicator[qp] = {
            "baseline": baseline,
            "desconsiderar": desconsiderar,
            "indice_mudanca": indice_mudanca,
            "projeto_liberado": projeto_liberado,
            "projeto_pronto": projeto_pronto,
            "em_ajuste": em_ajuste,
            "data_emissao_qp": data_emissao_qp,
            "prazo_entrega_qp": prazo_entrega_qp,
            "status_proj": status_proj,
            "quant_mp_proj": quant_mp_proj,
            "quant_pi_proj": quant_pi_proj
        }
    return dataframe

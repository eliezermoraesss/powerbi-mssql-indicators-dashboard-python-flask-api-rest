from app import db
from sqlalchemy import text

def get_all_indicators():
    query_qps_em_andamento = text("SELECT cod_qp FROM enaplic_management.dbo.tb_status_qps WHERE status_proj = 'A';")
    cod_qps = db.session.execute(query_qps_em_andamento).fetchall()
    cod_qps = [row[0] for row in cod_qps]

    data = {}
    for cod_qp in cod_qps:
        data[cod_qp] = {
            "baseline": get_indicator_value("TOP 1 vl_proj_all_prod", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "desconsiderar": get_indicator_value("TOP 1 vl_proj_prod_cancel", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "indice_mudanca": get_indicator_value("TOP 1 vl_proj_modify_perc", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "projeto_liberado": get_indicator_value("TOP 1 vl_proj_released", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "projeto_pronto": get_indicator_value("TOP 1 vl_proj_finished", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "em_ajuste": get_indicator_value("TOP 1 vl_proj_adjusted", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "num_pi": get_indicator_value("TOP 1 vl_proj_pi", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "num_mp": get_indicator_value("TOP 1 vl_proj_mp", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "op_aberta": get_indicator_value("COUNT(*)", "SC2010", f"C2_ZZNUMQP LIKE '%{cod_qp}' AND C2_DATRF = '       ' AND D_E_L_E_T_ <> '*'"),
            "indice_pcp": get_indicator_value("TOP 1 vl_pcp_perc", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "op_fechada": get_indicator_value("COUNT(*)", "SC2010", f"C2_ZZNUMQP LIKE '%{cod_qp}' AND C2_DATRF <> '       ' AND D_E_L_E_T_ <> '*'"),
            "indice_producao": get_indicator_value("TOP 1 vl_product_perc", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "sc_aberta": get_indicator_value("COUNT(*)", "SC1010", f"C1_ZZNUMQP LIKE '%{cod_qp}' AND C1_PEDIDO = '      ' AND D_E_L_E_T_ <> '*'"),
            "pc_total": get_indicator_value("COUNT(*)", "SC7010", f"C7_ZZNUMQP LIKE '%{cod_qp}' AND D_E_L_E_T_ <> '*'"),
            "indice_compra": get_indicator_value("TOP 1 vl_compras_perc", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC"),
            "mat_entregue": get_indicator_value("COUNT(*)", "SC7010", f"C7_ZZNUMQP LIKE '%{cod_qp}' AND C7_ENCER = 'E' AND D_E_L_E_T_ <> '*'"),
            "indice_recebimento": get_indicator_value("TOP 1 vl_mat_received_perc", "enaplic_management.dbo.tb_dashboard_indicators", f"cod_qp LIKE '%{cod_qp}' ORDER BY id DESC")
        }

    return data

def get_all_totvs_indicators():
    query_qps_em_andamento = text("SELECT cod_qp FROM enaplic_management.dbo.tb_status_qps WHERE status_proj = 'A';")
    cod_qps = db.session.execute(query_qps_em_andamento).fetchall()
    cod_qps = [row[0] for row in cod_qps]

    data = {}
    for cod_qp in cod_qps:
        data[cod_qp] = {
            "op_aberta": get_indicator_value("COUNT(*)", "SC2010", f"C2_ZZNUMQP LIKE '%{cod_qp}' AND C2_DATRF = '       ' AND D_E_L_E_T_ <> '*'"),
            "op_fechada": get_indicator_value("COUNT(*)", "SC2010", f"C2_ZZNUMQP LIKE '%{cod_qp}' AND C2_DATRF <> '       ' AND D_E_L_E_T_ <> '*'"),
            "sc_aberta": get_indicator_value("COUNT(*)", "SC1010", f"C1_ZZNUMQP LIKE '%{cod_qp}' AND C1_PEDIDO = '      ' AND D_E_L_E_T_ <> '*'"),
            "pc_total": get_indicator_value("COUNT(*)", "SC7010", f"C7_ZZNUMQP LIKE '%{cod_qp}' AND D_E_L_E_T_ <> '*'"),
            "mat_entregue": get_indicator_value("COUNT(*)", "SC7010", f"C7_ZZNUMQP LIKE '%{cod_qp}' AND C7_ENCER = 'E' AND D_E_L_E_T_ <> '*'"),
        }

    return data

def get_indicator_value(select_clause, table_name,where_clause):
    query = text(f"SELECT {select_clause} AS value FROM {table_name} WHERE {where_clause};")
    result = db.session.execute(query).fetchone()
    return result[0] if result else 0

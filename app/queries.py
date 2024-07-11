from app import db
from sqlalchemy import text

def get_qp_data():
    query_qps_em_andamento = text("SELECT cod_qp FROM enaplic_management.dbo.tb_status_qps WHERE status_proj = 'A';")
    cod_qps = db.session.execute(query_qps_em_andamento).fetchall()
    cod_qps = [row['cod_qp'] for row in cod_qps]

    data = {}
    for cod_qp in cod_qps:
        data[cod_qp] = {
            "op_aberta": get_single_value("COUNT(*)", "SC2010", f"C2_ZZNUMQP LIKE '%{cod_qp}' AND C2_DATRF = '       ' AND D_E_L_E_T_ <> '*'"),
            "op_fechada": get_single_value("COUNT(*)", "SC2010", f"C2_ZZNUMQP LIKE '%{cod_qp}' AND C2_DATRF <> '       ' AND D_E_L_E_T_ <> '*'"),
            "sc_aberta": get_single_value("COUNT(*)", "SC1010", f"C1_ZZNUMQP LIKE '%{cod_qp}' AND C1_PEDIDO = '      ' AND D_E_L_E_T_ <> '*'"),
            "pc_total": get_single_value("COUNT(*)", "SC7010", f"C7_ZZNUMQP LIKE '%{cod_qp}' AND D_E_L_E_T_ <> '*'"),
            "mat_entregue": get_single_value("COUNT(*)", "SC7010", f"C7_ZZNUMQP LIKE '%{cod_qp}' AND C7_ENCER = 'E' AND D_E_L_E_T_ <> '*'")
        }

    return data

def get_single_value(select_clause, table_name,where_clause):
    query = text(f"SELECT {select_clause} AS value FROM {table_name} WHERE {where_clause};")
    result = db.session.execute(query).fetchone()
    return result['value'] if result else 0

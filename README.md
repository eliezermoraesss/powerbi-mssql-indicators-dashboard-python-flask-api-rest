# Projeto Eureka Dashboard

![Imagem da página inicial do aplicativo Eureka Dashboard]

## Visão Geral

O Projeto Eureka Dashboard é uma solução robusta e automatizada para integrar dados de indicadores de projetos a partir de diversas fontes, incluindo um arquivo Excel hospedado no SharePoint, um banco de dados MSSQL e um sistema ERP TOTVS. A aplicação extrai, transforma e carrega (ETL) esses dados em tabelas SQL, alimentando um dashboard interativo no Power BI, facilitando a análise e tomada de decisões estratégicas.

## Arquitetura e Componentes

![Diagrama ilustrando o fluxo de trabalho ETL da aplicação]

### API Flask (app.py)

![Snippet de código de app.py mostrando as rotas Flask]

A API Flask é o coração do sistema, responsável por:

#### Rotas:

- GET /: Exibe a página inicial, notificando o status da API e enviando um e-mail de confirmação.
- GET /indicators: Recupera todos os indicadores de projetos, combinando dados do SharePoint e TOTVS.
- GET /indicators/totvs: Obtém indicadores específicos do sistema TOTVS.
- POST /indicators/save?qp={status}: Salva os indicadores processados no banco de dados MSSQL. O parâmetro qp define o status dos projetos: "open" (abertos), "closed" (concluídos) ou "test" (ambiente de teste).

#### Lógica ETL:

- Extração: Coleta dados do arquivo Excel no SharePoint através da biblioteca sharepoint_project_data e do TOTVS por meio de consultas SQL.
- Transformação: Limpa, formata, agrega e calcula indicadores relevantes para análise.
- Carregamento: Insere os dados processados nas tabelas tb_dashboard_indicators e tb_current_dashboard_indicators no MSSQL.
- Agendamento: Utiliza o APScheduler para agendamento flexível das tarefas de atualização, garantindo a consistência dos dados.

#### Tratamento de Erros e Logging:

- Implementa logging detalhado para auxiliar na identificação e resolução de problemas.
- Envia notificações por e-mail em caso de erros durante o processo.

![Screenshot de uma notificação de e-mail enviada pela aplicação]

### Banco de Dados (models.py)

Define a estrutura das tabelas do MSSQL:

- tb_open_qps: Armazena informações sobre as QPs (Quadros de Projetos) abertas.
- tb_end_qps: Contém dados das QPs concluídas.
- tb_dashboard_indicators: Histórico de indicadores de projetos.
- tb_current_dashboard_indicators: Indicadores atuais dos projetos.

### Integração com SharePoint (sharepoint_project_data.py)

Realiza a comunicação com o SharePoint para extrair os dados do arquivo Excel, utilizando a biblioteca pythoncom para automação do Excel.

### Utilitários (utils.py)

Oferece um conjunto de funções auxiliares para manipulação de dados, como limpeza, formatação de datas e cálculo de indicadores.

## Endpoints da API e Testes com cURL

Utilize o cURL para testar os endpoints da API:

### GET /indicators:

```bash
curl http://localhost:5000/indicators
{
  "005552": {
    "baseline": 2091,
    "custo_item_com_mat_entregue": 401063.13,
    "custo_item_com_pc": 401204.13,
    "custo_mp_mat_entregue": 316233.79,
    "custo_mp_pc": 316233.79,
    "custo_total_mp_pc": 717437.92,
    "data_emissao_qp": "15/02/2022",
    "data_fim_proj": "", 
    "data_inicio_proj": "",
    "desconsiderar": 156,
    "duracao_proj": 0,
    "em_ajuste": 0,
    "indice_compra": 106.81,
    "indice_mudanca": 7.46,
    "indice_pcp": 75.79,
    "indice_producao": 100.0,
    "indice_recebimento": 100.0,
    "mat_entregue": 455,
    "op_fechada": 573,
    "op_total": 573,
    "pc_total": 455,
    "prazo_entrega_qp": "30/09/2022",
    "projeto_liberado": 1935,
    "projeto_pronto": 1935,
    "quant_mp_proj": 1179,
    "quant_pi_proj": 756,
    "sc_total": 426,
    "status_proj": "F"
  },
  "005618": { 
    // ... (dados de outro projeto)
  }
}

curl http://localhost:5000/indicators/totvs
{
  "005552": {
    "custo_item_com_mat_entregue": 401063.13,
    "custo_item_com_pc": 401204.13,
    "custo_mp_mat_entregue": 316233.79,
    "custo_mp_pc": 316233.79,
    "custo_total_mp_pc": 717437.92,
    "indice_compra": 106.81,
    "indice_producao": 100.0,
    "indice_recebimento": 100.0,
    "mat_entregue": 455,
    "op_fechada": 573,
    "op_total": 573,
    "pc_total": 455,
    "sc_total": 426
  },
  "005618": { 
    // ... (dados de outro projeto)
  }
}

curl -X POST http://localhost:5000/indicators/save?qp=open


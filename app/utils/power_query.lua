let
    Fonte = SharePoint.Files("https://enaplic.sharepoint.com/sites/QP/", [ApiVersion = 15]),
    #"Linhas Filtradas" = Table.SelectRows(Fonte, each ([Folder Path] = "https://enaplic.sharepoint.com/sites/QP/Documentos Compartilhados/QP ABERTA/") and ([Name] <> "BASELINE_PROJETO - TEMAPLATE.xlsm" and [Name] <> "QP - EXXX - CLIENTE - PROJ.xlsm" and [Name] <> "QP-Exxxx - MAJOPAR.xlsm")),
    #"Arquivos Ocultos Filtrados1" = Table.SelectRows(#"Linhas Filtradas", each [Attributes]?[Hidden]? <> true),
    #"Invocar Função Personalizada1" = Table.AddColumn(#"Arquivos Ocultos Filtrados1", "Transformar Arquivo", each #"Transformar Arquivo"([Content])),
    #"Colunas Renomeadas1" = Table.RenameColumns(#"Invocar Função Personalizada1", {"Name", "Nome da Origem"}),
    #"Outras Colunas Removidas1" = Table.SelectColumns(#"Colunas Renomeadas1", {"Nome da Origem", "Transformar Arquivo"}),
    #"Coluna de Tabela Expandida1" = Table.ExpandTableColumn(#"Outras Colunas Removidas1", "Transformar Arquivo", Table.ColumnNames(#"Transformar Arquivo"(#"Arquivo de Amostra"))),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Coluna de Tabela Expandida1",{{"Nome da Origem", type text}, {"ITEM", type text}, {"ROTULO_1", type text}, {"GERAL", Int64.Type}, {"ROTULO_2", type text}, {"MP", Int64.Type}, {"ROTULO_3", type text}, {"PI", Int64.Type}}),
    #"dataEmissao" = Table.AddColumn(#"Tipo Alterado", "DATA_EMISSAO", each DataEmissao[Column1]{0}),
    #"prazoEntrega" = Table.AddColumn(#"dataEmissao", "PRAZO_ENTREGA", each prazo_entrega_qp),
    #"statusProjeto" = Table.AddColumn(#"prazoEntrega", "STATUS_PROJETO", each status_projeto_qp),
    #"table" = Table.AddColumn(#"statusProjeto", "DATA_ATUALIZACAO", each DateTime.LocalNow())

in
    #"table"

    let
    Fonte = SharePoint.Files("https://enaplic.sharepoint.com/sites/QP/", [ApiVersion = 15]),
    #"Linhas Filtradas" = Table.SelectRows(Fonte, each ([Folder Path] = "https://enaplic.sharepoint.com/sites/QP/Documentos Compartilhados/QP ABERTA/") and ([Name] <> "BASELINE_PROJETO - TEMAPLATE.xlsm" and [Name] <> "QP - EXXX - CLIENTE - PROJ.xlsm" and [Name] <> "QP-Exxxx - MAJOPAR.xlsm")),
    #"Arquivos Ocultos Filtrados1" = Table.SelectRows(#"Linhas Filtradas", each [Attributes]?[Hidden]? <> true),
    #"Invocar Função Personalizada1" = Table.AddColumn(#"Arquivos Ocultos Filtrados1", "Transformar Arquivo", each #"Transformar Arquivo"([Content])),
    #"Colunas Renomeadas1" = Table.RenameColumns(#"Invocar Função Personalizada1", {"Name", "Nome da Origem"}),
    #"Outras Colunas Removidas1" = Table.SelectColumns(#"Colunas Renomeadas1", {"Nome da Origem", "Transformar Arquivo"}),
    #"Coluna de Tabela Expandida1" = Table.ExpandTableColumn(#"Outras Colunas Removidas1", "Transformar Arquivo", Table.ColumnNames(#"Transformar Arquivo"(#"Arquivo de Amostra"))),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Coluna de Tabela Expandida1",{{"Nome da Origem", type text}, {"ITEM", type text}, {"ROTULO_1", type text}, {"GERAL", Int64.Type}, {"ROTULO_2", type text}, {"MP", Int64.Type}, {"ROTULO_3", type text}, {"PI", Int64.Type}}),
    #"Personalização Adicionada" = Table.AddColumn(#"Tipo Alterado", "ÚLTIMA_ATUALIZAÇÃO", each DateTime.LocalNow())
in
    #"Personalização Adicionada"
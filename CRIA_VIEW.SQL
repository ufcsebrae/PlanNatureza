-- Passo 1: Remover a FUNCTION antiga, APENAS SE ELA EXISTIR.
-- Ajustado para 'IF', que corresponde a uma Table-Valued Function Inline.
IF OBJECT_ID('dbo.vw_Analise_Planejado_vs_Executado_v2', 'IF') IS NOT NULL
BEGIN
    DROP FUNCTION dbo.vw_Analise_Planejado_vs_Executado_v2;
    PRINT 'FUNCTION dbo.vw_Analise_Planejado_vs_Executado_v2 foi removida com sucesso.';
END
GO

-- Passo 2: Criar a nova FUNCTION com a padronização de natureza embutida.
CREATE FUNCTION dbo.vw_Analise_Planejado_vs_Executado_v2 
(   
    @DATAINICIO DATE,
    @DATAFIM DATE,
    @PPA VARCHAR(MAX)
)
RETURNS TABLE
AS
RETURN 
(
    WITH 
    -- CTE para o mapeamento de Naturezas, simulando o arquivo NATUREZA.csv
    MAPA_NATUREZA AS (
        SELECT Original, Final FROM (VALUES
            ('Pessoal', 'Pessoal'),
            ('Encargos Sociais', 'Encargos Sociais'),
            ('Benefícios Sociais', 'Benefícios Sociais'),
            ('Serviços Especializados', 'Serviços Especializados'),
            ('SERVIÇOS ESPECIALIZADOS - PJ', 'Serviços Especializados'),
            ('Serviços Contratados', 'Serviços Contratados'),
            ('SERVIÇOS CONTRATADOS - PJ', 'Serviços Contratados'),
            ('Encargos Sociais s/Serviços de Terceiros', 'Encargos Sociais S/Serviços De Terceiros'),
            ('ENCARGOS SOCIAIS S/ SERVIÇOS DE TERCEIROS', 'Encargos Sociais S/Serviços De Terceiros'),
            ('DESPESAS COM VIAGENS E LOCOMOÇÃO', 'Despesas Com Viagens'),
            ('Despesas com Viagens', 'Despesas Com Viagens'),
            ('Aluguéis e Encargos', 'Aluguéis E Encargos'),
            ('ALUGUÉIS E ENCARGOS - PESSOA JURÍDICA (PJ)', 'Aluguéis E Encargos'),
            ('Divulgação, Anúncios, Publicidade e Propaganda', 'Divulgação, Anúncios, Publicidade E Propaganda'),
            ('Serviços Gráficos e de Reprodução', 'Serviços Gráficos E De Reprodução'),
            ('Serviços de Comunicação em Geral', 'Serviços De Comunicação Em Geral'),
            ('Materiais de Consumo', 'Materiais De Consumo'),
            ('DEMAIS CUSTOS E DESPESAS GERAIS - PJ', 'Demais Custos E Despesas Gerais'),
            ('Demais Custos e Despesas Gerais', 'Demais Custos E Despesas Gerais'),
            ('Doações e Subvenções', 'Doações E Subvenções'),
            ('DESPESAS TRIBUTÁRIAS', 'Despesas Tributárias'),
            ('DESPESAS FINANCEIRAS', 'Despesas Financeiras'),
            ('Transf. Externas - Convênios c/Outras Entidades', 'Transf. Externas - Convênios C/Outras Entidades'),
            ('LIBERAÇÕES DE CONVÊNIOS', 'Liberações De Convênios'),
            ('Bens Imóveis', 'Bens Imóveis'),
            ('Bens Móveis', 'Bens Móveis'),
            ('BENS MÓVEIS', 'Bens Móveis'),
            ('Depósitos Judiciais', 'Depósitos Judiciais'),
            ('DEPÓSITOS JUDICIAIS', 'Depósitos Judiciais'),
            ('Fundo de Empresas Emergentes', 'Fundo De Empresas Emergentes'),
            ('BENS INTANGÍVEIS', 'Bens Intangíveis'),
            ('FUNDO M. DE INVEST. EMPRESAS EMERGENTES', 'Fundo M. De Invest. Empresas Emergentes')
        ) AS V(Original, Final)
    ),
    EXECUTADO AS (
        SELECT 
            YEAR(DATA) AS ANO, MONTH(DATA) AS MES, PROJETO, ACAO, UNIDADE,
            CC AS CODCCUSTO, cdgContaNvl4 AS CODIGO_NATUREZA_ORCAMENTARIA,
            DESCNVL4 AS Descricao_Natureza_Orcamentaria, SUM(VALOR) AS VALOR_EXECUTADO
        FROM FATOFECHAMENTO_V2
        WHERE CC IS NOT NULL 
          AND DESCNVL1 IN ('DESPESAS', 'DESPESAS EXCLUSIVAS DO ORÇAMENTO')
          AND (DATA BETWEEN @DATAINICIO AND @DATAFIM)
        GROUP BY YEAR(DATA), MONTH(DATA), PROJETO, ACAO, UNIDADE, CC, cdgContaNvl4, DESCNVL4
    ),
    PLANEJADO AS (
        SELECT
            ANO, MES, PROJETO, ACAO, UNIDADE, CODCCUSTO, Valor_Ajustado, 
            Descricao_PPA, Codigo_Natureza_Orcamentaria, Descricao_Natureza_Orcamentaria,
            DTUNIDADE, DTPROJETO, DTACAO
        FROM [FINANCA].[dbo].[ORCADO_ENRIQUECIDO_COM_CC]
        WHERE descricao_ppa = @PPA
    ),
    CHAVES_COMUNS AS (
        SELECT ANO, MES, PROJETO, ACAO, UNIDADE, CODCCUSTO, Codigo_Natureza_Orcamentaria FROM PLANEJADO
        UNION
        SELECT ANO, MES, PROJETO, ACAO, UNIDADE, CODCCUSTO, CODIGO_NATUREZA_ORCAMENTARIA FROM EXECUTADO
    ),
    DADOS_COMBINADOS AS (
        SELECT 
            C.ANO, C.MES, C.PROJETO, C.ACAO, C.UNIDADE, C.CODCCUSTO,
            C.Codigo_Natureza_Orcamentaria,
            -- Mantém a descrição original para fazer o JOIN com o mapa
            COALESCE(P.Descricao_Natureza_Orcamentaria, E.Descricao_Natureza_Orcamentaria) AS Descricao_Natureza_Original,
            ISNULL(P.Valor_Ajustado, 0) AS Valor_Planejado,
            ISNULL(E.VALOR_EXECUTADO, 0) AS Valor_Executado
        FROM CHAVES_COMUNS C
        LEFT JOIN PLANEJADO P ON C.ANO = P.ANO AND C.MES = P.MES AND C.PROJETO = P.PROJETO AND C.CODCCUSTO = P.CODCCUSTO AND C.Codigo_Natureza_Orcamentaria = P.Codigo_Natureza_Orcamentaria
        LEFT JOIN EXECUTADO E ON C.ANO = E.ANO AND C.MES = E.MES AND C.PROJETO = E.PROJETO AND C.CODCCUSTO = E.CODCCUSTO AND C.Codigo_Natureza_Orcamentaria = E.Codigo_Natureza_Orcamentaria
    )
    -- SELECT final que aplica o mapeamento
    SELECT
        D.ANO, D.MES, D.PROJETO, D.ACAO, D.UNIDADE, D.CODCCUSTO,
        D.Codigo_Natureza_Orcamentaria,
        D.Descricao_Natureza_Original AS Descricao_Natureza_Orcamentaria,
        -- **NOVA COLUNA PADRONIZADA**
        -- Se encontrar um mapa, usa. Se não, usa a descrição original como fallback.
        ISNULL(MN.Final, D.Descricao_Natureza_Original) AS NATUREZA_FINAL,
        D.Valor_Planejado,
        D.Valor_Executado
    FROM DADOS_COMBINADOS D
    -- Junta com o mapa de naturezas
    LEFT JOIN MAPA_NATUREZA MN ON D.Descricao_Natureza_Original = MN.Original COLLATE SQL_Latin1_General_CP1_CI_AI
);
GO

PRINT 'FUNCTION dbo.vw_Analise_Planejado_vs_Executado_v2 foi criada com sucesso com a padronização de naturezas.';
GO

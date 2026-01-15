-- Passo 1: Remover a VIEW antiga, APENAS SE ELA EXISTIR.
-- A verificação 'V' garante que estamos tentando remover uma VIEW.
IF OBJECT_ID('dbo.vw_Analise_Planejado_vs_Executado_v2', 'V') IS NOT NULL
BEGIN
    DROP VIEW dbo.vw_Analise_Planejado_vs_Executado_v2;
    PRINT 'VIEW dbo.vw_Analise_Planejado_vs_Executado_v2 foi removida com sucesso.';
END
GO

-- Passo 2: Agora, criamos a FUNCTION do zero.
-- Como já a removemos, usamos CREATE FUNCTION em vez de CREATE OR ALTER.
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
    PC AS (
        SELECT CODCONTA, DESCRICAO
        FROM [HubDados].[CorporeRM].[CCONTA] WHERE LEN(CODCONTA) = 7
    ),
    CHAVES_COMUNS AS (
        SELECT ANO, MES, PROJETO, ACAO, UNIDADE, CODCCUSTO, Codigo_Natureza_Orcamentaria FROM PLANEJADO
        UNION
        SELECT ANO, MES, PROJETO, ACAO, UNIDADE, CODCCUSTO, CODIGO_NATUREZA_ORCAMENTARIA FROM EXECUTADO
    )
    SELECT 
        C.ANO, C.MES, C.PROJETO, C.ACAO, C.UNIDADE, C.CODCCUSTO,
        C.Codigo_Natureza_Orcamentaria,
        COALESCE(P.Descricao_Natureza_Orcamentaria, E.Descricao_Natureza_Orcamentaria, PC.DESCRICAO) AS Descricao_Natureza_Orcamentaria,
        ISNULL(P.Valor_Ajustado, 0) AS Valor_Planejado,
        ISNULL(E.VALOR_EXECUTADO, 0) AS Valor_Executado
    FROM CHAVES_COMUNS C
    LEFT JOIN PLANEJADO P ON C.ANO = P.ANO AND C.MES = P.MES AND C.PROJETO = P.PROJETO AND C.CODCCUSTO = P.CODCCUSTO AND C.Codigo_Natureza_Orcamentaria = P.Codigo_Natureza_Orcamentaria
    LEFT JOIN EXECUTADO E ON C.ANO = E.ANO AND C.MES = E.MES AND C.PROJETO = E.PROJETO AND C.CODCCUSTO = E.CODCCUSTO AND C.Codigo_Natureza_Orcamentaria = E.Codigo_Natureza_Orcamentaria
    LEFT JOIN PC ON C.Codigo_Natureza_Orcamentaria = PC.CODCONTA COLLATE SQL_Latin1_General_CP1_CI_AI
);
GO

PRINT 'FUNCTION dbo.vw_Analise_Planejado_vs_Executado_v2 foi criada com sucesso.';
GO

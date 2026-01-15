DECLARE @DATAINICIO AS DATE = '2025-01-01';
DECLARE @DATAFIM AS DATE = '2025-12-31';
DECLARE @PPA AS VARCHAR(MAX) = 'PPA 2025 - 2025/DEZ';
DECLARE @DESCNVL1 TABLE (Descnvl1 VARCHAR (MAX));
INSERT INTO @DESCNVL1 (Descnvl1) VALUES ('DESPESAS'), ('DESPESAS EXCLUSIVAS DO ORÇAMENTO');

WITH 
EXECUTADO AS (
    -- 1. Agrupa tudo o que foi executado (sem alterações aqui)
    SELECT 
        YEAR(DATA) AS ANO,
        MONTH(DATA) AS MES,
        PROJETO,
        ACAO,
        UNIDADE,
        CC AS CODCCUSTO,
        cdgContaNvl4 AS CODIGO_NATUREZA_ORCAMENTARIA,
        DESCNVL4 AS Descricao_Natureza_Orcamentaria,
        SUM(VALOR) AS VALOR_EXECUTADO
    FROM FATOFECHAMENTO_V2 A
    INNER JOIN @DESCNVL1 B ON A.DESCNVL1 = B.Descnvl1
    WHERE CC IS NOT NULL AND (DATA BETWEEN @DATAINICIO AND @DATAFIM)
    GROUP BY YEAR(DATA), MONTH(DATA), PROJETO, ACAO, UNIDADE, CC, cdgContaNvl4, DESCNVL4
),
PLANEJADO AS (
    -- 2. Seleciona os dados do planejamento (sem alterações aqui)
    SELECT
        ANO, MES, PROJETO, ACAO, UNIDADE, CODCCUSTO,
        Valor_Ajustado, Descricao_PPA, Codigo_Natureza_Orcamentaria,
        Descricao_Natureza_Orcamentaria, DTUNIDADE, DTPROJETO, DTACAO
    FROM [FINANCA].[dbo].[ORCADO_ENRIQUECIDO_COM_CC]
    WHERE descricao_ppa IN (@PPA)
),
PC AS (
    SELECT CODCONTA, DESCRICAO
    FROM [HubDados].[CorporeRM].[CCONTA] WHERE LEN(CODCONTA) = 7
),
-- 3. NOVA CTE: Cria uma lista mestra com todas as combinações de chaves existentes
CHAVES_COMUNS AS (
    SELECT ANO, MES, PROJETO, ACAO, UNIDADE, CODCCUSTO, Codigo_Natureza_Orcamentaria FROM PLANEJADO
    UNION
    SELECT ANO, MES, PROJETO, ACAO, UNIDADE, CODCCUSTO, CODIGO_NATUREZA_ORCAMENTARIA FROM EXECUTADO
)
-- 4. Junta tudo a partir da lista mestra de chaves
SELECT 
    C.ANO,
    C.MES,
    C.PROJETO,
    C.ACAO,
    C.UNIDADE,
    C.CODCCUSTO,
    C.Codigo_Natureza_Orcamentaria,
    COALESCE(P.Descricao_Natureza_Orcamentaria, E.Descricao_Natureza_Orcamentaria, PC.DESCRICAO) AS Descricao_Natureza_Orcamentaria,
    ISNULL(P.Valor_Ajustado, 0) AS Valor_Planejado,
    ISNULL(E.VALOR_EXECUTADO, 0) AS Valor_Executado
FROM CHAVES_COMUNS C
LEFT JOIN PLANEJADO P ON C.ANO = P.ANO 
                      AND C.MES = P.MES 
                      AND C.PROJETO = P.PROJETO 
                      AND C.CODCCUSTO = P.CODCCUSTO 
                      AND C.Codigo_Natureza_Orcamentaria = P.Codigo_Natureza_Orcamentaria
LEFT JOIN EXECUTADO E ON C.ANO = E.ANO 
                      AND C.MES = E.MES 
                      AND C.PROJETO = E.PROJETO 
                      AND C.CODCCUSTO = E.CODCCUSTO 
                      AND C.Codigo_Natureza_Orcamentaria = E.CODIGO_NATUREZA_ORCAMENTARIA
LEFT JOIN PC ON C.Codigo_Natureza_Orcamentaria = PC.CODCONTA COLLATE SQL_Latin1_General_CP1_CI_AI
ORDER BY 
    C.ANO, C.MES, C.PROJETO, C.CODCCUSTO;

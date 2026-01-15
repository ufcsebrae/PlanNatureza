
SELECT * 
FROM dbo.vw_Analise_Planejado_vs_Executado_v2(
    '2025-01-01',           -- @DATAINICIO
    '2025-12-31',           -- @DATAFIM
    'PPA 2025 - 2025/DEZ'   -- @PPA
)
-- Você pode adicionar filtros e ordenação aqui normalmente
WHERE 
    PROJETO = 'Aprendizes e Estagiários'
ORDER BY 
    MES, CODCCUSTO;

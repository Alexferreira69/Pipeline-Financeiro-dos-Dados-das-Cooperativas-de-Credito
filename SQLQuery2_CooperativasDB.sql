CREATE DATABASE CooperativasDB;

USE CooperativasDB;

--DROP TABLE tbl_Cooperativa;
-- Cria��o da tabela de Cooperativas
CREATE TABLE tbl_Cooperativa (
    ID_Cooperativa INT PRIMARY KEY IDENTITY(1,1),
    CNPJ VARCHAR(20) UNIQUE NOT NULL,
    Nome_Instituicao VARCHAR(100) NOT NULL
);


--DROP TABLE tbl_Conta;
-- Cria��o da tabela de Contas
CREATE TABLE tbl_Conta (
    ID_Conta INT PRIMARY KEY IDENTITY(1,1),
    Conta VARCHAR(50) NOT NULL,
    Nome_Conta VARCHAR(100) NOT NULL,
    Tipo_Conta VARCHAR(50) NOT NULL -- Receita, Despesa, Ativo, Passivo, etc.
);

--DROP TABLE tbl_Balanco;
IF OBJECT_ID('dbo.tbl_Balanco', 'U') IS NOT NULL
    DROP TABLE dbo.tbl_Balanco;
GO
-- Cria��o da tabela de Balancos
CREATE TABLE tbl_Balanco (
    ID_Balanco INT PRIMARY KEY IDENTITY(1,1),
    Data_Base DATE NOT NULL,
    CNPJ VARCHAR(20) NOT NULL,
    Nome_Instituicao VARCHAR(100) NOT NULL,
    Conta VARCHAR(50) NOT NULL,
    Nome_Conta VARCHAR(100) NOT NULL,
    Saldo DECIMAL(18, 2) NOT NULL,
    ID_Cooperativa INT,
    ID_Conta INT,
    FOREIGN KEY (ID_Cooperativa) REFERENCES tbl_Cooperativa(ID_Cooperativa),
    FOREIGN KEY (ID_Conta) REFERENCES tbl_Conta(ID_Conta)
);
GO


--consultar tabelas
SELECT * FROM tbl_Cooperativa;
SELECT * FROM tbl_Conta;
SELECT * FROM tbl_Balanco;

---1-Remover os Dados Atuais da tabela
--ALTER TABLE tbl_Balanco NOCHECK CONSTRAINT ALL;
--ALTER TABLE tbl_Conta NOCHECK CONSTRAINT ALL;
--ALTER TABLE tbl_Cooperativa NOCHECK CONSTRAINT ALL;
---2-Esvaziar todas as tabelas
--DELETE FROM tbl_Balanco;
--DELETE FROM tbl_Conta;
--DELETE FROM tbl_Cooperativa;
---3-Restaurar as restri��es
--ALTER TABLE tbl_Balanco WITH CHECK CHECK CONSTRAINT ALL;
--ALTER TABLE tbl_Conta WITH CHECK CHECK CONSTRAINT ALL;
--ALTER TABLE tbl_Cooperativa WITH CHECK CHECK CONSTRAINT ALL;




----consultas basicas no select--------
--1.1.total dos ativos e passivos das cooperativas no periodo---consulta ok
SELECT 
    SUM(CASE WHEN b.Nome_Conta = 'TOTAL GERAL DO ATIVO' THEN b.Saldo ELSE 0 END) AS Total_Ativos,
    SUM(CASE WHEN b.Nome_Conta = 'TOTAL GERAL DO PASSIVO' THEN b.Saldo ELSE 0 END) AS Total_Passivos
FROM 
    tbl_Balanco b
WHERE 
    b.Nome_Conta IN ('TOTAL GERAL DO ATIVO', 'TOTAL GERAL DO PASSIVO');
--------------

--1.2.receitas--ok
--receitas cooperativas no periodo das cooperativas
SELECT 
    SUM(CASE WHEN Nome_Conta = 'RECEITAS OPERACIONAIS' THEN Saldo ELSE 0 END) AS Total_Receitas_Operacionais,
    SUM(CASE WHEN Nome_Conta = 'RECEITAS N�O OPERACIONAIS' THEN Saldo ELSE 0 END) AS Total_Receitas_Nao_Operacionais,
    SUM(CASE WHEN Nome_Conta IN ('RECEITAS OPERACIONAIS', 'RECEITAS N�O OPERACIONAIS') THEN Saldo ELSE 0 END) AS Total_Receitas
FROM 
    tbl_Balanco
WHERE 
    Nome_Conta IN ('RECEITAS OPERACIONAIS', 'RECEITAS N�O OPERACIONAIS');

--total geral receitas cooperativas no periodo das cooperativas(operacional+nao operacional)
SELECT 
    SUM(Saldo) AS Total_Receitas
FROM 
    tbl_Balanco
WHERE 
    Nome_Conta IN ('RECEITAS OPERACIONAIS', 'RECEITAS N�O OPERACIONAIS');

----------------------

--1.3.despesas - 
--despesas cooperativas no periodo das cooperativas
SELECT 
    SUM(CASE WHEN Nome_Conta = '(-) DESPESAS OPERACIONAIS' THEN Saldo ELSE 0 END) AS Total_Despesas_Operacionais,
    SUM(CASE WHEN Nome_Conta = '(-) DESPESAS N�O OPERACIONAIS' THEN Saldo ELSE 0 END) AS Total_Despesas_Nao_Operacionais,
    SUM(CASE WHEN Nome_Conta IN ('(-) DESPESAS OPERACIONAIS', '(-) DESPESAS N�O OPERACIONAIS') THEN Saldo ELSE 0 END) AS Total_Despesas_Combinadas
FROM 
    tbl_Balanco
WHERE 
    Nome_Conta IN ('(-) DESPESAS OPERACIONAIS', '(-) DESPESAS N�O OPERACIONAIS');

--total geral despesas cooperativas no periodo das cooperativas(operacional+nao operacional)
SELECT 
    SUM(Saldo) AS Total_Despesas
FROM 
    tbl_Balanco
WHERE 
    Nome_Conta IN ('(-) DESPESAS OPERACIONAIS', '(-) DESPESAS N�O OPERACIONAIS');

-------------
--1.4--capital social
--todos
SELECT 
    YEAR(b.Data_Base) AS Ano,
    MONTH(b.Data_Base) AS Mes,
    SUM(b.Saldo) AS Total_Capital_Social
FROM 
    tbl_Balanco b
JOIN 
    tbl_Conta ct ON b.ID_Conta = ct.ID_Conta
WHERE 
    ct.Nome_Conta = 'Capital Social'
GROUP BY 
    YEAR(b.Data_Base), 
    MONTH(b.Data_Base)
ORDER BY 
    Ano, Mes;

--atual 06/2024
SELECT 
    SUM(Saldo) AS Total_Capital_Social_2024_01_06
FROM 
    tbl_Balanco 
WHERE 
    Conta = '61100004' 
    AND Data_Base = '2024-01-06';                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
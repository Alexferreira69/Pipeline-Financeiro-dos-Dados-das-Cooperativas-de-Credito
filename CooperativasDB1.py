#esta é a etl do Pipeline Financeiro dos Dados das Cooperativas de Credito

import pandas as pd
import pyodbc

# Mapeamento dos arquivos .csv para as tabelas
mapa_tabelas = {
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas122023.csv": 'tbl_Cooperativa',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas112023.csv": 'tbl_Cooperativa',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas102023.csv": 'tbl_Cooperativa',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas092023.csv": 'tbl_Conta',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas082023.csv": 'tbl_Conta',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas072023.csv": 'tbl_Conta',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas062024.csv": 'tbl_Balanco',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas052024.csv": 'tbl_Balanco',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas042024.csv": 'tbl_Balanco',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas032024.csv": 'tbl_Balanco',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas022024.csv": 'tbl_Balanco',
    r"C:\Users\alexa\Desktop\PenDriveLe\Credicitrus\FormaTi_TrabalhoFinal\Balancetes\BalanceteCooperativas012024.csv": 'tbl_Balanco'
}

# Conexão com o SQL Server
conn_str = 'DRIVER={SQL Server};SERVER=VirtualBox-PC;DATABASE=CooperativasDB;UID=Alexander;PWD=admim'

# Função para verificar erros ao ler o CSV
def read_csv_with_errors(file_path, encoding, delimiter):
    try:
        return pd.read_csv(file_path, encoding=encoding, sep=delimiter, on_bad_lines='skip')
    except pd.errors.ParserError as e:
        print(f"Erro ao ler o arquivo {file_path}: {e}")
        return None

# Função para inserir os dados nas tabelas
def inserir_dados(df, cursor):
    # Converter a coluna 'Saldo' para numérico, substituindo valores inválidos por 0
    df['SALDO'] = pd.to_numeric(df['SALDO'], errors='coerce').fillna(0)
    
    for index, row in df.iterrows():
        try:
            # Inserir na tabela Cooperativas
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM tbl_Cooperativa WHERE CNPJ = ?)
                INSERT INTO tbl_Cooperativa (CNPJ, Nome_Instituicao) 
                VALUES (?, ?)""",
                row['CNPJ'], row['CNPJ'], row['NOME_INSTITUICAO'])

            # Inserir na tabela Contas
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM tbl_Conta WHERE Conta = ?)
                INSERT INTO tbl_Conta (Conta, Nome_Conta, Tipo_Conta) 
                VALUES (?, ?, ?)""",
                row['CONTA'], row['CONTA'], row['NOME_CONTA'], row['TAXONOMIA'])

            # Inserir na tabela Balancos
            cursor.execute("""
                INSERT INTO tbl_Balanco (Data_Base, CNPJ, Nome_Instituicao, Conta, Nome_Conta, Saldo, ID_Cooperativa, ID_Conta)
                VALUES (?, ?, ?, ?, ?, ?, 
                (SELECT ID_Cooperativa FROM tbl_Cooperativa WHERE CNPJ = ?), 
                (SELECT ID_Conta FROM tbl_Conta WHERE Conta = ?))""",
                row['DATA_BASE'], row['CNPJ'], row['NOME_INSTITUICAO'], row['CONTA'], row['NOME_CONTA'], row['SALDO'], row['CNPJ'], row['CONTA'])

        except Exception as e:
            print(f"Erro ao inserir dados na linha {index}: {e}")
            continue

# Processar os arquivos CSV e inserir dados nas tabelas correspondentes
with pyodbc.connect(conn_str) as conn:
    cursor = conn.cursor()
    
    # Testar diferentes delimitadores (';' e ',') e encodings ('latin1' e 'utf-8')
    for caminho_arquivo, tabela in mapa_tabelas.items():
        df = read_csv_with_errors(caminho_arquivo, 'latin1', ';')  # Tente primeiro com o delimitador ";"
        
        # Se falhar, tente com outro delimitador ","
        if df is None:
            df = read_csv_with_errors(caminho_arquivo, 'latin1', ',')
        
        # Se falhar novamente, tente com outro encoding 'utf-8'
        if df is None:
            df = read_csv_with_errors(caminho_arquivo, 'utf-8', ';')
        
        # Se ainda falhar, tente 'utf-8' com delimitador ","
        if df is None:
            df = read_csv_with_errors(caminho_arquivo, 'utf-8', ',')
        
        # Inserir dados na tabela se o dataframe foi carregado com sucesso
        if df is not None:
            inserir_dados(df, cursor)
    
    conn.commit()

import os
import sqlite3
import pandas as pd
from datetime import date

class Database:
    def __init__(self):
        caminho_atual = os.path.dirname(os.path.abspath(__file__))
        diretorio_db = os.path.join(caminho_atual, "database")
        os.makedirs(diretorio_db, exist_ok=True)
        caminho_db = os.path.join(diretorio_db, "combustiveis.db")
        self.conn = sqlite3.connect(caminho_db)

    def criar_tabela_datas(self):
        query = """
        CREATE TABLE IF NOT EXISTS datas_consultas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ano INTEGER,
            data TEXT,
            UNIQUE(ano, data)
        )
        """
        self.conn.execute(query)
        self.conn.commit()

    def filtros(self, yearfilter, datefilter):
        self.criar_tabela_datas()
        
        query_filtro = """
            SELECT MAX(ano) as ano, MAX(data) as data
            FROM datas_consultas
        """

        filtro_data = pd.read_sql(query_filtro, self.conn)

        if filtro_data.empty:
            return (yearfilter, datefilter)

        ano = filtro_data.iloc[0]["ano"]
        data = filtro_data.iloc[0]["data"]

        if pd.isna(ano) or pd.isna(data):
            return (yearfilter, datefilter)

        return (int(ano), data)
    
    def inserir_data_consulta(self, ano, data):
        query = """
            INSERT INTO datas_consultas (ano, data)
            VALUES (?, ?)
            ON CONFLICT(ano, data) DO NOTHING
        """

        self.conn.execute(query, (ano, data))
        self.conn.commit()
    
    def inserir_dados(self, dataset):
        if dataset.shape[0] > 0:
            dataset.to_sql("precos_combustiveis", self.conn, if_exists="append", index=False)

    def listar_datas(self):
        lista_datas = pd.read_sql("""
                SELECT *
                FROM datas_consultas
                ORDER BY data DESC
                LIMIT 1
            """, self.conn)

        return lista_datas
    
    def listar_combustivel(self, ano = date.today().year):
        combustiveis = pd.read_sql("""
                SELECT *
                FROM precos_combustiveis
            """, self.conn)

        return combustiveis[combustiveis['ano'] == ano]

    def fechar(self):
        self.conn.close()
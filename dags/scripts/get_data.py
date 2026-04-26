import pandas as pd
import basedosdados as bd
import sqlite3
import os
from datetime import datetime, timedelta

YEARSTART = 2021
DATESTART = "2021-01-01"

os.makedirs("database", exist_ok=True)

conn = sqlite3.connect("database/combustiveis.db")

billing_id = "analise-de-dados-492901"

# FUNCTIONS
def criar_tabela_datas(conn):
    query = """
        CREATE TABLE IF NOT EXISTS datas_consultas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ano INTEGER,
            data TEXT,
            UNIQUE(ano, data)
        )
    """
    conn.execute(query)
    conn.commit()

def filtros():
    criar_tabela_datas(conn)
    
    query_filtro = """
        SELECT MAX(ano) as ano, MAX(data) as data
        FROM datas_consultas
    """

    filtro_data = pd.read_sql(query_filtro, conn)

    if filtro_data.empty:
        return (YEARSTART, DATESTART)

    ano = filtro_data.iloc[0]["ano"]
    data = filtro_data.iloc[0]["data"]

    if pd.isna(ano) or pd.isna(data):
        return (YEARSTART, DATESTART)

    return (int(ano), data)

def marge_por_produto(produtos):
    margens = {}

    for produto in produtos:
        if "Gasolina Aditivada" in produto:
            margens[produto] = 0.17
        elif "Gasolina" in produto:
            margens[produto] = 0.15
        elif "Etanol" in produto:
            margens[produto] = 0.12
        elif "Diesel" in produto:
            margens[produto] = 0.10
        elif "Gnv" in produto:
            margens[produto] = 0.18
        elif "Glp" in produto:
            margens[produto] = 0.20
        else:
            margens[produto] = 0.15

    return margens

def calcula_lucro(compra, venda, produto, margens):
    if pd.notna(compra) and pd.notna(venda):
        return venda - compra

    if pd.isna(compra) and pd.notna(venda):
        return venda * margens.get(produto, 0.15)
    
    return 0

filtro_ano ,filtro_data = filtros()

# EXTRACT
query = f"""
        SELECT
            dados.ano as ano,
            dados.sigla_uf AS sigla_uf,
            diretorio_sigla_uf.nome AS sigla_uf_nome,
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            dados.bairro_revenda as bairro_revenda,
            dados.cep_revenda as cep_revenda,
            dados.endereco_revenda as endereco_revenda,
            dados.cnpj_revenda as cnpj_revenda,
            dados.nome_estabelecimento as nome_estabelecimento,
            dados.bandeira_revenda as bandeira_revenda,
            dados.data_coleta as data_coleta,
            dados.produto as produto,
            dados.unidade_medida as unidade_medida,
            dados.preco_compra as preco_compra,
            dados.preco_venda as preco_venda
        FROM `basedosdados.br_anp_precos_combustiveis.microdados` AS dados
        LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE dados.ano = {filtro_ano}
        AND dados.data_coleta = DATE('{filtro_data}')
    """

df = bd.read_sql(query = query, billing_project_id = billing_id)

# TRANSFORM
margens = marge_por_produto(df["produto"].unique())

df["lucro"] = df.apply(
    lambda row: calcula_lucro(
        row["preco_compra"],
        row["preco_venda"],
        row["produto"],
        margens
    ),
    axis=1
)

df["id"] = pd.util.hash_pandas_object(
    df[["cnpj_revenda", "data_coleta", "produto"]],
    index=False
).astype(str)


# LOAD
if df.shape[0] > 0:
    df.to_sql("precos_combustiveis", conn, if_exists="append", index=False)

query = """
        INSERT INTO datas_consultas (ano, data)
        VALUES (?, ?)
        ON CONFLICT(ano, data) DO NOTHING
    """

conn.execute(query, (filtro_ano, filtro_data))
conn.commit()
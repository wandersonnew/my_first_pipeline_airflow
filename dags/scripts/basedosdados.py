# import basedosdados as bd
from google.cloud import bigquery
from google.oauth2 import service_account
import os

def obter_combustivel(ano, data):
    project = os.getenv("PROJECT")

    json_path = os.path.join(os.path.dirname(__file__), "credentials", "analise-de-dados.json")
    
    credentials = service_account.Credentials.from_service_account_file(json_path)
    
    client = bigquery.Client(credentials=credentials, project=project)
    
    # Sua query
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
        WHERE dados.ano = {ano}
        AND dados.data_coleta = DATE('{data}')
    """
    
    # Executa e converte para DataFrame (igual ao que a basedosdados fazia)
    return client.query(query).to_dataframe()
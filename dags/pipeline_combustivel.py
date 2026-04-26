from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

# Parâmetros
YEARSTART = 2021
DATESTART = "2021-01-01"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "pipeline_combustivel",
    default_args=default_args,
    description="Pipeline de combustíveis",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 4, 23),
    catchup=False,
    tags=["dados"],
) as dag:

    @task
    def extract():
        import pandas as pd
        from scripts.database import Database
        from scripts.basedosdados import obter_combustivel
        
        db = Database()
        filtro_ano, filtro_data = db.filtros(YEARSTART, DATESTART)
        df = obter_combustivel(filtro_ano, filtro_data)
        return df.to_json(orient='split')

    @task
    def transform(df_json):
        import pandas as pd
        from scripts.func_aux import marge_por_produto, calcula_lucro
        
        df = pd.read_json(df_json, orient='split')
        
        # Garanta que o nome da função abaixo está correto (margem vs marge)
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

        return df.to_json(orient='split')

    @task
    def load(df_json):
        import pandas as pd
        from scripts.database import Database
        
        df = pd.read_json(df_json, orient='split')
        db = Database()
        filtro_ano, filtro_data = db.filtros(YEARSTART, DATESTART)

        db.inserir_dados(df)
        db.inserir_data_consulta(filtro_ano, filtro_data)

    # Orquestração
    data = extract()
    transformed = transform(data)
    load(transformed)
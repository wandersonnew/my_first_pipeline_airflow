# Pipeline de Combustíveis com Apache Airflow

Pipeline ETL que extrai dados de preços de combustíveis da API Base dos Dados (BigQuery), calcula o lucro por revenda e armazena os resultados em um banco de dados SQLite local. O pipeline é orquestrado pelo Apache Airflow e executado via Docker.

## Estrutura do Projeto

```
my_first_pipeline_airflow/
├── dags/
│   ├── scripts/
│   │   ├── credentials/
│   │   │   └── analise-de-dados.json   # Credenciais do Google Cloud
│   │   ├── database/
│   │   │   └── combustiveis.db         # Banco de dados SQLite
│   │   ├── basedosdados.py             # Consulta ao BigQuery
│   │   ├── database.py                 # Operações no SQLite
│   │   ├── func_aux.py                 # Funções de transformação
│   │   └── get_data.py                 # Script standalone (sem Airflow)
│   └── pipeline_combustivel.py         # DAG principal
├── docker-compose.yaml
├── Dockerfile
└── requirements.txt
```

## Pipeline (DAG)

A DAG `pipeline_combustivel` executa diariamente com 3 tasks encadeadas:

1. **extract** — consulta o BigQuery via `basedosdados.br_anp_precos_combustiveis.microdados`, filtrando pelo último ano/data já processado (ou `2021-01-01` na primeira execução).
2. **transform** — calcula o lucro de cada registro:
   - Se `preco_compra` e `preco_venda` disponíveis: `lucro = preco_venda - preco_compra`
   - Se apenas `preco_venda` disponível: `lucro = preco_venda * margem_estimada`
   - Gera um `id` único por hash de `cnpj_revenda + data_coleta + produto`
3. **load** — insere os dados na tabela `precos_combustiveis` do SQLite e registra a data processada em `datas_consultas`.

### Margens estimadas por produto

| Produto           | Margem |
|-------------------|--------|
| Gasolina Aditivada | 17%   |
| Gasolina           | 15%   |
| Etanol             | 12%   |
| Diesel             | 10%   |
| GNV                | 18%   |
| GLP                | 20%   |
| Outros             | 15%   |

## Pré-requisitos

- Docker e Docker Compose
- Conta no Google Cloud com acesso ao projeto `basedosdados`
- Arquivo de credenciais do serviço GCP em `dags/scripts/credentials/analise-de-dados.json`

## Configuração

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
AIRFLOW_UID=50000
FERNET_KEY=<sua_fernet_key>
PROJECT=<seu_project_id_gcp>
```

Para gerar a `FERNET_KEY`:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Como executar

```bash
# Inicializar o Airflow (primeira vez)
docker compose up airflow-init

# Subir todos os serviços
docker compose up -d
```

Acesse a interface web em `http://localhost:8080` com usuário e senha `airflow`.

Ative a DAG `pipeline_combustivel` na interface para iniciar o pipeline.

## Dependências

Definidas no `requirements.txt` e instaladas via `Dockerfile`:

- `pandas`
- `basedosdados`
- `google-cloud-bigquery`
- `google-auth`

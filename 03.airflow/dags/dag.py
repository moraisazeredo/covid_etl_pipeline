# 3_airflow/dags/dag.py

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import csv
import os
import pendulum
import requests
from pendulum import datetime


# ─────────────────────────────────────────────────────
# 1) PERFIL DEV — aponta para o PostgreSQL local (Docker)
# ─────────────────────────────────────────────────────
profile_config_dev = ProfileConfig(
    profile_name="dw",  # Mesmo nome do profiles.yml
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        # Conexão cadastrada em Admin → Connections no Airflow
        conn_id="docker_postgres_db",
        profile_args={"schema": "public"},
    ),
)

# ──────────────────────────────────────────────────────────
# 2) PERFIL PROD — aponta para o PostgreSQL remoto (Railway)
# ──────────────────────────────────────────────────────────
profile_config_prod = ProfileConfig(
    profile_name="dw",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="railway_postgres_db",  # Outra conexão, apontando para prod
        profile_args={"schema": "public"},
    ),
)

# ──────────────────────────────────────────────────────────────────
# 3) SELEÇÃO DO AMBIENTE — lido da variável "dbt_env" no Airflow UI
# ──────────────────────────────────────────────────────────────────
dbt_env = Variable.get("dbt_env", default_var="dev").lower()
# .lower() garante que "DEV", "Dev" e "dev" funcionam igual

if dbt_env not in ("dev", "prod"):
    raise ValueError(f"dbt_env inválido: {dbt_env!r}, use 'dev' ou 'prod'")

profile_config = profile_config_dev if dbt_env == "dev" else profile_config_prod
conn_id = profile_config.profile_mapping.conn_id

# ──────────────────────────────────────────────────────────────────
# 4) INGESTÃO — API de COVID-19 (disease.sh) → CSV em include/data/ → raw
# ──────────────────────────────────────────────────────────────────
API_URL = "https://disease.sh/v3/covid-19/countries"
DATA_DIR = f"{os.environ['AIRFLOW_HOME']}/include/data"

PAISES = [
    "Brazil", "USA", "France", "Germany", "Italy",
    "Spain", "UK", "Portugal", "Argentina", "Mexico",
    "Canada", "Chile", "Colombia", "Peru", "India",
    "China", "Japan", "S. Korea", "Australia", "South Africa",
]

# Campos da API que interessam, na ordem em que vão para o CSV (e para a tabela raw)
COLUNAS_API = [
    "country", "cases", "deaths", "recovered", "active", "critical",
    "casesPerOneMillion", "deathsPerOneMillion", "tests", "testsPerOneMillion",
    "population", "oneCasePerPeople", "oneDeathPerPeople", "oneTestPerPeople",
    "activePerOneMillion", "recoveredPerOneMillion",
]

# Mesma ordem do CSV: snapshot_date + COLUNAS_API (o COPY casa as colunas pela posição)
# Nomes em snake_case para não precisar de aspas no SQL; o staging faz a tipagem final
RAW_DDL = """
create schema if not exists raw;
create table if not exists raw.covid_countries (
    snapshot_date             date,
    country                   text,
    cases                     numeric,
    deaths                    numeric,
    recovered                 numeric,
    active                    numeric,
    critical                  numeric,
    cases_per_one_million     numeric,
    deaths_per_one_million    numeric,
    tests                     numeric,
    tests_per_one_million     numeric,
    population                numeric,
    one_case_per_people       numeric,
    one_death_per_people      numeric,
    one_test_per_people       numeric,
    active_per_one_million    numeric,
    recovered_per_one_million numeric
);
"""


# ──────────────────────────────────────────────────────────────────
# 5) CRIAÇÃO DO DAG — extração da API, carga na raw e, depois, o dbt (Cosmos)
# ──────────────────────────────────────────────────────────────────
@dag(
    dag_id=f"dag_dw_{dbt_env}",  # Nome do DAG muda conforme o ambiente
    schedule="@daily",           # Execução diária automática
    start_date=datetime(2026, 9, 24),
    catchup=False,               # Não executa datas retroativas desde start_date
    default_args={"retries": 2}, # 2 tentativas em caso de falha
)
def dag_dw():

    @task
    def extract_api():
        """Consulta a API para cada país e grava o CSV do dia em include/data/."""
        snapshot_date = pendulum.now("UTC").to_date_string()  # A API devolve os totais de hoje
        linhas = []
        for pais in PAISES:
            # Sem try/except: se um país falhar, a task falha e o Airflow tenta de novo (retries)
            resposta = requests.get(f"{API_URL}/{pais}", params={"strict": "true"}, timeout=30)
            resposta.raise_for_status()
            dados = resposta.json()
            faltando = [c for c in COLUNAS_API if c not in dados]
            if faltando:
                raise ValueError(f"[{pais}] colunas ausentes na resposta da API: {faltando}")
            linhas.append([snapshot_date] + [dados[c] for c in COLUNAS_API])

        csv_path = f"{DATA_DIR}/covid_countries_{snapshot_date}.csv"
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["snapshot_date"] + COLUNAS_API)
            writer.writerows(linhas)
        print(f"{len(linhas)} países gravados em {csv_path}")
        return {"csv_path": csv_path, "snapshot_date": snapshot_date}  # Só o caminho vai pelo XCom

    @task
    def load_raw(extracao):
        """Substitui o retrato do dia em raw.covid_countries (delete do dia + COPY)."""
        hook = PostgresHook(postgres_conn_id=conn_id)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(RAW_DDL)
            # Apaga só o dia atual: rodar de novo não duplica e os dias anteriores ficam (histórico)
            cur.execute("delete from raw.covid_countries where snapshot_date = %s", (extracao["snapshot_date"],))
            with open(extracao["csv_path"], encoding="utf-8") as f:
                cur.copy_expert("copy raw.covid_countries from stdin with (format csv, header true)", f)
            cur.execute("select count(*) from raw.covid_countries where snapshot_date = %s", (extracao["snapshot_date"],))
            print(f"Linhas carregadas para {extracao['snapshot_date']}: {cur.fetchone()[0]}")
        # Tudo roda numa transação só: se o COPY falhar, o delete é desfeito

    transform = DbtTaskGroup(
        group_id="dbt_dw",
        project_config=ProjectConfig(
            # Caminho do projeto dbt dentro do container (montado pelo docker-compose.override.yml)
            dbt_project_path="/usr/local/airflow/dbt/dw",
            project_name="dw",
        ),
        profile_config=profile_config,  # Credenciais injetadas via conexão do Airflow
        execution_config=ExecutionConfig(
            # Caminho do executável dbt dentro do virtualenv criado no Dockerfile
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        ),
        operator_args={
            "install_deps": True,  # Roda "dbt deps" antes de executar (baixa os pacotes)
            "target": profile_config.target_name,
        },
    )

    load_raw(extract_api()) >> transform  # API → raw → dbt


dag_dw()

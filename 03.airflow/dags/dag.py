# 3_airflow/dags/dag.py

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os
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
# 4) INGESTÃO — CSV substituído diariamente em include/data/
# ──────────────────────────────────────────────────────────────────
CSV_PATH = f"{os.environ['AIRFLOW_HOME']}/include/data/airline_delay_cause.csv"

# Colunas na mesma ordem do CSV; métricas como numeric (o staging faz a tipagem final)
RAW_DDL = """
create schema if not exists raw;
create table if not exists raw.airline_delay_cause (
    year                integer,
    month               integer,
    carrier             text,
    carrier_name        text,
    airport             text,
    airport_name        text,
    arr_flights         numeric,
    arr_del15           numeric,
    carrier_ct          numeric,
    weather_ct          numeric,
    nas_ct              numeric,
    security_ct         numeric,
    late_aircraft_ct    numeric,
    arr_cancelled       numeric,
    arr_diverted        numeric,
    arr_delay           numeric,
    carrier_delay       numeric,
    weather_delay       numeric,
    nas_delay           numeric,
    security_delay      numeric,
    late_aircraft_delay numeric
);
"""


# ──────────────────────────────────────────────────────────────────
# 5) CRIAÇÃO DO DAG — carga do CSV e, depois, as tasks do dbt (Cosmos)
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
    def load_csv():
        """Recarrega raw.airline_delay_cause com o CSV do dia (truncate + COPY)."""
        hook = PostgresHook(postgres_conn_id=conn_id)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(RAW_DDL)
            cur.execute("truncate table raw.airline_delay_cause")
            with open(CSV_PATH, encoding="utf-8") as f:
                # COPY é o carregamento em massa nativo do Postgres — segundos em vez de minutos
                cur.copy_expert(
                    "copy raw.airline_delay_cause from stdin with (format csv, header true)",
                    f,
                )
            cur.execute("select count(*) from raw.airline_delay_cause")
            print(f"Linhas carregadas: {cur.fetchone()[0]}")
        # Tudo roda numa transação só: se o COPY falhar, a tabela antiga é mantida

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

    load_csv() >> transform  # Primeiro carrega o CSV, depois roda o dbt


dag_dw()

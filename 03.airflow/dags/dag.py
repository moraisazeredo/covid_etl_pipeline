# 3_airflow/dags/dag.py

from airflow.models import Variable
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
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

# ──────────────────────────────────────────────────────────────────
# 4) CRIAÇÃO DO DAG — o Cosmos lê o projeto dbt e gera as tasks
# ──────────────────────────────────────────────────────────────────
my_cosmos_dag = DbtDag(

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

    schedule="@daily",                    # Execução diária automática
    start_date=datetime(2026, 9, 24),
    catchup=False,  # Não executa datas retroativas desde start_date

    dag_id=f"dag_dw_{dbt_env}",  # Nome do DAG muda conforme o ambiente
    default_args={"retries": 2},          # 2 tentativas em caso de falha
)
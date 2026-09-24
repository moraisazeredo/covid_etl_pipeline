# 3_airflow/Dockerfile

# Imagem base oficial da Astronomer (Airflow 3.x)
FROM astrocrpublic.azurecr.io/runtime:3.1-8

# Cria um virtualenv isolado e instala o dbt-postgres dentro dele
# Esse venv fica em /usr/local/airflow/dbt_venv/
RUN python -m venv dbt_venv \
    && . dbt_venv/bin/activate \
    && pip install --no-cache-dir dbt-postgres==1.9.0 \
    && deactivate
# O "deactivate" sai do venv — o Airflow continua no Python principal
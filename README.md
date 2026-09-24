# Projeto Prático de Engenharia de Dados

Pipeline de dados ponta a ponta sobre **atrasos de voos nos EUA**: um CSV atualizado diariamente é carregado em um PostgreSQL, transformado em um Data Warehouse com **dbt** (camadas staging → intermediate → mart) e orquestrados diariamente pelo **Apache Airflow** (via Astro CLI + Astronomer Cosmos).

---

## Sumário

1. [Visão geral](#visão-geral)
2. [Arquitetura](#arquitetura)
3. [Stack utilizada](#stack-utilizada)
4. [Estrutura de pastas](#estrutura-de-pastas)
5. [Os dados](#os-dados)
6. [Etapa 1 — Ambiente local (`01.local_setup`)](#etapa-1--ambiente-local-01local_setup)
7. [Etapa 2 — Data Warehouse com dbt (`02.data_warehouse`)](#etapa-2--data-warehouse-com-dbt-02data_warehouse)
8. [Etapa 3 — Orquestração com Airflow (`03.airflow`)](#etapa-3--orquestração-com-airflow-03airflow)
9. [Como executar o projeto do zero](#como-executar-o-projeto-do-zero)
10. [Ambientes dev e prod](#ambientes-dev-e-prod)
11. [Portas utilizadas](#portas-utilizadas)
12. [Problemas comuns](#problemas-comuns)

---

## Visão geral

O projeto é dividido em três etapas, cada uma em sua própria pasta:

| Etapa | Pasta | O que faz |
|---|---|---|
| 1 | `01.local_setup` | Sobe o PostgreSQL (Docker) que funciona como Data Warehouse e prepara o ambiente Python com o dbt |
| 2 | `02.data_warehouse` | Projeto dbt (fonte única): lê a tabela `raw` e cria as tabelas/views de staging, intermediate e mart |
| 3 | `03.airflow` | Airflow rodando via Astro CLI: carrega o CSV do dia na tabela `raw` e, em seguida, o Cosmos executa o dbt com uma task por modelo |

**Resultado final:** tabelas analíticas (marts) prontas para consumo por ferramentas de BI, com KPIs mensais, desempenho por companhia aérea, por aeroporto e por causa de atraso — atualizadas automaticamente todos os dias.

---

## Arquitetura

```mermaid
flowchart LR
    subgraph Fonte
        CSV[(include/data/<br/>airline_delay_cause.csv<br/>substituído diariamente)]
    end

    subgraph Airflow["Airflow (Astro CLI - Docker) - DAG dag_dw_dev / dag_dw_prod"]
        LOAD[task load_csv<br/>TRUNCATE + COPY]
        DBT[task group dbt_dw<br/>gerado pelo Cosmos]
        LOAD --> DBT
    end

    subgraph DW["PostgreSQL 17 - dbt_db (Docker, porta 5433)"]
        RAW[raw.airline_delay_cause<br/>tabela bruta]
        STG[staging<br/>views]
        INT[intermediate<br/>tabelas - modelo estrela]
        MART[mart<br/>tabelas agregadas]
        RAW --> STG --> INT --> MART
    end

    CSV --> LOAD
    LOAD -->|COPY| RAW
    DBT -->|dbt run / test<br/>dbt_venv| STG
    MART --> BI[Ferramentas de BI / análises]
```

### Fluxo de execução

1. Todo dia, o arquivo `03.airflow/include/data/airline_delay_cause.csv` é substituído pela versão mais recente.
2. O Airflow lê `dags/dag.py`. O **Cosmos** (`DbtTaskGroup`) inspeciona o projeto dbt (`02.data_warehouse/dw`, montado em `/usr/local/airflow/dbt/dw`) e cria **uma task para cada modelo**, respeitando as dependências declaradas com `ref()` e `source()`.
3. Diariamente (`@daily`), o scheduler dispara a DAG:
   1. **`load_csv`** — cria o schema/tabela `raw.airline_delay_cause` (se não existir), faz `TRUNCATE` e carrega o CSV com `COPY`, tudo em uma única transação;
   2. **`dbt_dw`** — só começa depois da carga. Cada task do grupo:
      1. gera um `profiles.yml` temporário a partir da **conexão do Airflow** (`docker_postgres_db` ou `railway_postgres_db`);
      2. roda `dbt deps` (instala `dbt_utils`, `dbt_expectations`);
      3. executa `dbt_venv/bin/dbt run --select <modelo>` (e `test`, quando houver testes).
4. O dbt conecta no PostgreSQL e materializa as views/tabelas em cada camada.

> **Por que não usar `dbt seed`?** Seeds são feitos para tabelas pequenas e estáticas (de-paras, listas de códigos). Um CSV de ~42 MB substituído todo dia é **ingestão**: o `dbt seed` insere os dados em lotes e fica muito lento nesse volume, enquanto o `COPY` nativo do Postgres faz a carga em massa em uma única operação.

### Linhagem dos modelos (DAG do dbt)

```mermaid
flowchart LR
    raw[raw.airline_delay_cause<br/><i>source</i>] --> stg[stg_airline_delay_cause]

    stg --> dim_month[int_dim_month]
    stg --> dim_carrier[int_dim_carrier]
    stg --> dim_airport[int_dim_airport]
    stg --> fct[int_fct_flight_delays]

    fct --> kpis[mart_monthly_kpis]
    dim_month --> kpis

    fct --> carrier_perf[mart_carrier_performance]
    dim_carrier --> carrier_perf

    fct --> airport_perf[mart_airport_performance]
    dim_airport --> airport_perf

    fct --> causes_long[mart_delay_causes_long]
    fct --> causes_share[mart_delay_causes_share_month]
```

---

## Stack utilizada

| Ferramenta | Versão | Papel |
|---|---|---|
| PostgreSQL | 17 (Docker) | Data Warehouse |
| dbt-core / dbt-postgres | 1.10+ local / 1.9.0 no Airflow | Transformações SQL em camadas |
| dbt_utils | 1.3.0 | Macros utilitárias |
| dbt_expectations | 0.10.8 | Testes avançados de qualidade de dados |
| Apache Airflow | 3.x (Astro Runtime 3.1-8) | Orquestração |
| Astro CLI | — | Sobe o Airflow localmente em Docker |
| astronomer-cosmos | — | Converte o projeto dbt em DAG do Airflow |
| uv | — | Gerenciador de ambiente/dependências Python (etapa 1) |
| Docker / Docker Compose | — | Containers do Postgres e do Airflow |

---

## Estrutura de pastas

```
projeto_engenharia_de_dados/
├── 01.local_setup/
│   ├── docker-compose.yml        # PostgreSQL 17 do Data Warehouse (porta 5433)
│   ├── pyproject.toml / uv.lock  # Dependências Python (dbt, pandas, etc.)
│   └── .env                      # DBT_USER e DBT_PASSWORD (não versionar!)
│
├── 02.data_warehouse/
│   └── dw/                       # Projeto dbt (fonte única, usado também pelo Airflow)
│       ├── dbt_project.yml       # Configuração do projeto e materializações
│       ├── packages.yml          # dbt_utils e dbt_expectations
│       ├── profiles.yml          # Conexão local (ignorado pelo git)
│       └── models/
│           ├── staging/          # sources.yml (tabela raw) + limpeza e tipagem (views)
│           ├── intermediate/     # Dimensões e fato (tabelas)
│           └── mart/             # Agregações para BI (tabelas)
│
└── 03.airflow/                   # Projeto Astro (Airflow)
    ├── Dockerfile                # Imagem Astro Runtime + virtualenv com dbt
    ├── requirements.txt          # Pacotes Python do Airflow (cosmos, provider postgres)
    ├── docker-compose.override.yml  # Monta ../02.data_warehouse/dw dentro dos containers
    ├── .env                      # Timeouts de parse da DAG (ignorado pelo git)
    ├── .astro/config.yaml        # Porta do Postgres interno do Airflow (5435)
    ├── include/
    │   └── data/
    │       └── airline_delay_cause.csv  # CSV substituído diariamente
    └── dags/
        └── dag.py                # load_csv + tasks do dbt geradas pelo Cosmos
```

> O projeto dbt existe **em um único lugar** (`02.data_warehouse/dw`). O Airflow o enxerga por um volume do Docker, então alterações nos modelos valem imediatamente, sem rebuild da imagem.

---

## Os dados

**Fonte:** *Airline On-Time Statistics and Delay Causes* — Bureau of Transportation Statistics (BTS), Departamento de Transportes dos EUA.

- Arquivo: `03.airflow/include/data/airline_delay_cause.csv` (substituído diariamente, sempre com esse nome)
- Tabela no banco: `raw.airline_delay_cause`
- Volume: ~318 mil linhas (~42 MB)
- **Não versionado:** o CSV está no `.gitignore` (muda todo dia e é grande). Baixe-o no site do BTS e salve no caminho acima.
- Período: **jun/2003 a mai/2022**
- Granularidade: **1 linha por mês × companhia aérea × aeroporto**

| Coluna | Descrição |
|---|---|
| `year`, `month` | Ano e mês de referência |
| `carrier`, `carrier_name` | Código e nome da companhia aérea (ex: `AA` – American Airlines) |
| `airport`, `airport_name` | Código IATA e nome do aeroporto (ex: `ATL`) |
| `arr_flights` | Total de voos que chegaram |
| `arr_del15` | Voos com 15+ minutos de atraso |
| `arr_cancelled`, `arr_diverted` | Voos cancelados / desviados |
| `arr_delay` | Minutos totais de atraso |
| `carrier_delay` | Minutos de atraso por culpa da companhia (manutenção, tripulação) |
| `weather_delay` | Minutos de atraso por clima |
| `nas_delay` | Minutos de atraso pelo sistema aéreo nacional (controle de tráfego) |
| `security_delay` | Minutos de atraso por segurança/triagem |
| `late_aircraft_delay` | Minutos de atraso por aeronave atrasada do voo anterior |
| `*_ct` | Contagem (fracionária) de ocorrências de cada causa |

---

## Etapa 1 — Ambiente local (`01.local_setup`)

Sobe o **PostgreSQL 17** que será o Data Warehouse.

**`docker-compose.yml`**

- Container: `dbt_postgres`
- Banco criado automaticamente: `dbt_db`
- Usuário e senha lidos do arquivo `.env` (`DBT_USER`, `DBT_PASSWORD`)
- Porta: `5433` na máquina → `5432` no container (evita conflito com um Postgres instalado localmente e com o Postgres interno do Airflow)
- Volume `postgres_data`: os dados sobrevivem a reinícios do container
- Healthcheck com `pg_isready` a cada 5 segundos

**`pyproject.toml`** (gerenciado com `uv`, Python ≥ 3.13)

Instala `dbt-core`, `dbt-postgres` e bibliotecas de apoio (`pandas`, `numpy`, `duckdb`, `faker`, `matplotlib`) para rodar o dbt manualmente e explorar os dados fora do Airflow.

---

## Etapa 2 — Data Warehouse com dbt (`02.data_warehouse`)

### Configuração (`dbt_project.yml`)

- Projeto e perfil: `dw`
- Variável `dbt_date:time_zone = America/Sao_Paulo`
- Materialização por camada:

| Camada | Materialização | Motivo |
|---|---|---|
| `staging` | `view` | Barata de recriar, sem custo de armazenamento |
| `intermediate` | `table` | Consultada com frequência pelos marts |
| `mart` | `table` | Consumo direto por BI, precisa ser rápida |

### Source (camada raw)

A tabela `raw.airline_delay_cause` **não é criada pelo dbt**: ela é carregada pelo Airflow (task `load_csv`). O dbt apenas a declara como *source* em `models/staging/sources.yml` e a referencia com `{{ source('raw', 'airline_delay_cause') }}`. Assim, a linhagem do dbt começa na tabela bruta e a ingestão fica separada da transformação.

### Camada staging

**`stg_airline_delay_cause`** (view)

- Seleciona as colunas da tabela raw e aplica **tipagem explícita** (`integer`, `numeric`, `text`).
- Cria a chave de tempo **`year_month_key`** = `year * 100 + month` (ex: `202205`).
- Não altera a granularidade: continua 1 linha por mês × companhia × aeroporto.

### Camada intermediate — modelo dimensional (estrela)

| Modelo | Tipo | Chave | Descrição |
|---|---|---|---|
| `int_dim_month` | Dimensão | `month_id` | Meses distintos (`year`, `month`) |
| `int_dim_carrier` | Dimensão | `carrier_id` | Companhias aéreas; `max(carrier_name)` resolve nomes duplicados |
| `int_dim_airport` | Dimensão | `airport_id` | Aeroportos; `max(airport_name)` resolve nomes duplicados |
| `int_fct_flight_delays` | Fato | `month_id` + `carrier_id` + `airport_id` | Métricas de voos, atrasos em minutos e contagem por causa |

```mermaid
erDiagram
    int_dim_month ||--o{ int_fct_flight_delays : month_id
    int_dim_carrier ||--o{ int_fct_flight_delays : carrier_id
    int_dim_airport ||--o{ int_fct_flight_delays : airport_id

    int_dim_month {
        int month_id PK
        int year
        int month
    }
    int_dim_carrier {
        text carrier_id PK
        text carrier_name
    }
    int_dim_airport {
        text airport_id PK
        text airport_name
    }
    int_fct_flight_delays {
        int month_id FK
        text carrier_id FK
        text airport_id FK
        int arr_flights
        int arr_del15
        int arr_cancelled
        int arr_diverted
        int arr_delay
        int carrier_delay
        int weather_delay
        int nas_delay
        int security_delay
        int late_aircraft_delay
    }
```

### Camada mart — tabelas para análise

| Modelo | Granularidade | Principais métricas |
|---|---|---|
| `mart_monthly_kpis` | 1 linha por mês | voos, atrasados 15+, `pct_delayed_15m`, cancelados, desviados, minutos de atraso |
| `mart_carrier_performance` | 1 linha por companhia | voos, atrasados 15+, `pct_delayed_15m`, cancelados, minutos de atraso |
| `mart_airport_performance` | 1 linha por aeroporto | voos, atrasados 15+, `pct_delayed_15m`, cancelados, minutos de atraso |
| `mart_delay_causes_long` | mês × companhia × aeroporto × causa | minutos de atraso por causa em **formato longo** (unpivot via `UNION ALL`) — ideal para gráficos filtráveis por causa |
| `mart_delay_causes_share_month` | 1 linha por mês | % de cada causa (`pct_carrier`, `pct_weather`, `pct_nas`, `pct_security`, `pct_late_aircraft`) sobre o atraso total do mês |

Todos os percentuais usam `CASE WHEN total = 0 THEN 0` para evitar divisão por zero.

### Pacotes (`packages.yml`)

- **dbt_utils** — macros como `generate_surrogate_key`, `unpivot`, `date_spine`.
- **dbt_expectations** — testes de qualidade (ranges, nulos, valores aceitos). Depende do `dbt_date`, que também é instalado pelo `dbt deps`.

---

## Etapa 3 — Orquestração com Airflow (`03.airflow`)

Projeto criado com o **Astro CLI** (`astro dev init`), que sobe o Airflow 3 em containers Docker.

### `Dockerfile`

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.1-8
RUN python -m venv dbt_venv && . dbt_venv/bin/activate \
    && pip install --no-cache-dir dbt-postgres==1.9.0 && deactivate
```

O dbt é instalado em um **virtualenv separado** (`/usr/local/airflow/dbt_venv`) para que suas dependências não conflitem com as do Airflow. O Cosmos chama esse executável diretamente.

### `requirements.txt`

Pacotes instalados no Python **principal** do Airflow:

- `astronomer-cosmos` — integração dbt + Airflow.
- `apache-airflow-providers-postgres` — habilita o tipo de conexão **Postgres** na interface do Airflow (sem ele, a opção não aparece).

> O arquivo precisa se chamar exatamente `requirements.txt`; o Astro ignora qualquer outro nome.

### `docker-compose.override.yml`

Monta a pasta `../02.data_warehouse/dw` em `/usr/local/airflow/dbt/dw` nos containers **scheduler** e **dag-processor**, para que o Cosmos consiga ler e executar o projeto dbt (e alterações nos modelos apareçam sem rebuild da imagem).

> A pasta `include/` (onde fica o CSV) já é montada automaticamente pelo Astro em `/usr/local/airflow/include`.

### `.env`

Aumenta os limites de tempo para o Airflow ler a DAG (`AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT` e `AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT` = 300 s). Na primeira leitura, o Cosmos roda `dbt deps` + `dbt ls` para descobrir os modelos, o que passa dos 30 s padrão; nas leituras seguintes ele usa cache.

### `.astro/config.yaml`

Muda a porta do Postgres **interno** do Airflow (metadados) para `5435`, evitando conflito com o Postgres do Windows (`5432`) e com o Data Warehouse (`5433`).

### `dags/dag.py` — ingestão + dbt

A DAG tem duas partes, executadas em sequência (`load_csv() >> transform`):

**1. `load_csv` (ingestão)** — task Python que usa o `PostgresHook` com a mesma conexão do ambiente:

1. `create schema if not exists raw` + `create table if not exists raw.airline_delay_cause`;
2. `truncate table raw.airline_delay_cause`;
3. `COPY ... FROM STDIN (FORMAT csv, HEADER true)` com o arquivo do dia;
4. imprime a quantidade de linhas carregadas no log.

Tudo acontece em **uma única transação**: se o arquivo do dia vier com problema e o `COPY` falhar, o `TRUNCATE` é desfeito e a tabela mantém os dados do dia anterior.

**2. `dbt_dw` (transformação)** — não há nenhum `dbt run` escrito explicitamente: quem executa o dbt é o **Cosmos**, através da classe `DbtTaskGroup`.

| Bloco | O que faz |
|---|---|
| `ProfileConfig` (dev/prod) | Gera o `profiles.yml` do dbt a partir de uma **conexão do Airflow** (`PostgresUserPasswordProfileMapping`) — as credenciais ficam no Airflow, não no código |
| `Variable.get("dbt_env")` | Lê a variável `dbt_env` do Airflow (`dev` por padrão) e escolhe o perfil/conexão |
| `ProjectConfig` | Caminho do projeto dbt dentro do container: `/usr/local/airflow/dbt/dw` |
| `ExecutionConfig` | Caminho do executável: `$AIRFLOW_HOME/dbt_venv/bin/dbt` |
| `operator_args` | `install_deps=True` (roda `dbt deps`) e `target` (dev/prod) |
| `@dag(schedule="@daily")` | Execução diária, `catchup=False`, 2 retries por task |
| `dag_id` | `dag_dw_dev` ou `dag_dw_prod`, conforme o ambiente |

Ao carregar o arquivo, o Cosmos lê o projeto dbt e gera automaticamente:

- um grupo de tasks por modelo (`run` e, quando houver testes declarados, `test`);
- as dependências entre elas, seguindo os `ref()` e `source()` do SQL.

Nos **logs** de cada task, na interface do Airflow, dá para ver o comando dbt completo que foi montado e a saída do dbt.

---

## Como executar o projeto do zero

### Pré-requisitos

- Docker Desktop
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- [uv](https://docs.astral.sh/uv/) (opcional, para rodar o dbt fora do Airflow)

### 1. Subir o Data Warehouse

Crie o arquivo `01.local_setup/.env`:

```env
DBT_USER=seu_usuario
DBT_PASSWORD=sua_senha
```

Depois:

```bash
cd 01.local_setup
docker compose up -d
```

### 2. (Opcional) Rodar o dbt manualmente

> Requer que a tabela `raw.airline_delay_cause` já exista, ou seja, que a DAG tenha rodado ao menos uma vez (passo 5).

Crie `02.data_warehouse/dw/profiles.yml` (ele é ignorado pelo git):

```yaml
dw:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5433
      user: seu_usuario
      password: sua_senha
      dbname: dbt_db
      schema: public
      threads: 4
```

E execute:

```bash
cd 01.local_setup
uv sync
cd ../02.data_warehouse/dw
uv run --project ../../01.local_setup dbt deps
uv run --project ../../01.local_setup dbt build --profiles-dir .
```

### 3. Subir o Airflow

Coloque o CSV em `03.airflow/include/data/airline_delay_cause.csv` e crie o arquivo `03.airflow/.env`:

```env
AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300
AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT=300
```

Depois:

```bash
cd 03.airflow
astro dev start
```

A interface fica em **http://localhost:8080**.

> Sempre que alterar `requirements.txt`, `packages.txt`, `Dockerfile`, `.env` ou `docker-compose.override.yml`, rode `astro dev restart`.

### 4. Criar a conexão no Airflow

Em **Admin → Connections → Add Connection**:

| Campo | Valor |
|---|---|
| Connection Id | `docker_postgres_db` |
| Connection Type | `Postgres` |
| Host | `host.docker.internal` |
| Database | `dbt_db` |
| Login | valor de `DBT_USER` |
| Password | valor de `DBT_PASSWORD` |
| Port | `5433` |

> O Host é `host.docker.internal` (e não `localhost`) porque o Airflow roda dentro de um container: `localhost` apontaria para o próprio container do Airflow.

### 5. Executar a DAG

Ative e dispare a DAG **`dag_dw_dev`** na interface. No log da task `load_csv` aparece `Linhas carregadas: ...`. Ao final, a tabela bruta estará em `dbt_db.raw` e os modelos em `dbt_db.public`:

```sql
select count(*) from raw.airline_delay_cause;
select * from public.mart_monthly_kpis order by month_id;
```

### Rotina diária

Basta substituir `03.airflow/include/data/airline_delay_cause.csv` pelo arquivo novo (mantendo o nome). Na próxima execução agendada, a DAG recarrega a tabela raw e reconstrói todas as camadas.

Para cancelar uma execução em andamento: abra a execução → **Mark Run as… → Failed**. Para rodar novamente: **Trigger** (do zero) ou **Clear Run → Only failed tasks** (retoma de onde parou).

---

## Ambientes dev e prod

A DAG suporta dois ambientes, escolhidos pela **Variable** `dbt_env` do Airflow (**Admin → Variables**):

| `dbt_env` | Conexão usada | Banco | DAG gerada |
|---|---|---|---|
| `dev` (padrão) | `docker_postgres_db` | PostgreSQL local (Docker) | `dag_dw_dev` |
| `prod` | `railway_postgres_db` | PostgreSQL remoto (Railway) | `dag_dw_prod` |

Para usar prod, crie a conexão `railway_postgres_db` com os dados do banco no Railway e defina `dbt_env = prod`. Qualquer outro valor gera erro na importação da DAG.

---

## Portas utilizadas

| Porta | Serviço |
|---|---|
| `5432` | Reservada (Postgres instalado localmente no Windows, se houver) |
| `5433` | PostgreSQL do Data Warehouse (`dbt_postgres`) |
| `5435` | PostgreSQL interno do Airflow (metadados) |
| `8080` | Interface web do Airflow |

---

## Problemas comuns

| Sintoma | Causa | Solução |
|---|---|---|
| Tipo de conexão **Postgres** não aparece no Airflow | `apache-airflow-providers-postgres` não instalado (ou `requirements.txt` com nome errado) | Adicionar ao `requirements.txt` e rodar `astro dev restart` |
| `connection refused` ao conectar no banco | Host `localhost` na conexão, ou container `dbt_postgres` parado | Usar `host.docker.internal:5433` e `docker compose up -d` em `01.local_setup` |
| DAG não aparece / erro de import | Projeto dbt não montado ou `dbt_env` inválido | Conferir `docker-compose.override.yml` e a variável `dbt_env` (`dev` ou `prod`) |
| `DagBag import timeout ... after 30.0s` | Cosmos demora na primeira leitura do projeto dbt | Criar o `03.airflow/.env` com os timeouts de 300 s e rodar `astro dev restart` |
| `load_csv` falha com `No such file or directory` | CSV fora do lugar ou com outro nome | Salvar em `03.airflow/include/data/airline_delay_cause.csv` |
| `relation "raw.airline_delay_cause" does not exist` ao rodar o dbt manualmente | A DAG ainda não rodou nenhuma vez | Disparar a DAG (a `load_csv` cria a tabela) |
| DAG demora muitos minutos na carga | Uso de `dbt seed` para arquivos grandes | Carregar via `COPY` (task `load_csv`), como neste projeto |
| Conflito de porta ao subir containers | Outro serviço usando 5432/5433/5435 | Ajustar as portas no `docker-compose.yml` ou em `.astro/config.yaml` |

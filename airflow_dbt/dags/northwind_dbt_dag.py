"""
DAG: northwind_dbt_dag
Orquestra o pipeline completo do projeto dbt Northwind no Databricks usando
Astronomer Cosmos. Executa seeds → snapshots → models (staging, intermediate,
marts) → tests em um único TaskGroup, com cada nó dbt renderizado como uma
tarefa individual no Airflow.

Versões:
    astronomer-cosmos : 1.15.1
    apache-airflow    : 3.1.5+astro.1
    dbt-core          : 1.12.3
    dbt-databricks    : 1.10.9
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag
from airflow import decorators

from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    SeedRenderingBehavior,
    TestBehavior,
    LoadMode,
    ExecutionMode,
)

# ---------------------------------------------------------------------------
# Caminhos — absolutos dentro do container Airflow
# ---------------------------------------------------------------------------
DBT_PROJECT_PATH = Path("/usr/local/airflow/include/northwind_dbt")
DBT_PROFILES_DIR = Path("/usr/local/airflow/include/.dbt")
DBT_EXECUTABLE   = Path("/usr/local/bin/dbt")

# ---------------------------------------------------------------------------
# Configuração do perfil — lê o profiles.yml existente; nenhuma credencial
# fica hardcoded na DAG.
# ---------------------------------------------------------------------------
profile_config = ProfileConfig(
    profile_name="northwind_dbt",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_DIR / "profiles.yml",
)

# ---------------------------------------------------------------------------
# Configuração do projeto dbt
# ---------------------------------------------------------------------------
project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    project_name="northwind_dbt",
    models_relative_path="models",
    seeds_relative_path="seeds",
    snapshots_relative_path="snapshots",
)

# ---------------------------------------------------------------------------
# Modo de execução: LOCAL — o worker do Airflow chama o binário dbt
# diretamente, garantindo que cada modelo seja uma tarefa individual.
# Nota: dbt_project_path é gerenciado pelo ProjectConfig; não repetir aqui.
# ---------------------------------------------------------------------------
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=str(DBT_EXECUTABLE),
    # Instala dbt deps automaticamente antes do primeiro run
    install_dbt_deps=True,
)

# ---------------------------------------------------------------------------
# Configuração de renderização:
#   • seeds     → sempre executar (ALWAYS)
#   • snapshots → incluídos via select
#   • tests     → executar depois de cada modelo (AFTER_EACH)
#   • load_method → DBT_LS para descoberta automática de nós
# Nota: dbt_project_path é gerenciado pelo ProjectConfig; não repetir aqui.
# ---------------------------------------------------------------------------
render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    test_behavior=TestBehavior.AFTER_EACH,
    seed_rendering_behavior=SeedRenderingBehavior.ALWAYS,
    # Inclui todos os tipos de nós: models, seeds, snapshots, tests
    select=["path:models", "path:seeds", "path:snapshots"],
)

# ---------------------------------------------------------------------------
# Argumentos padrão das tasks
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": False,
}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="northwind_dbt_dag",
    description="Pipeline dbt Northwind → Databricks (seeds → snapshots → models → tests)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "databricks", "northwind"],
    max_active_runs=1,
    doc_md=__doc__,
)
def northwind_dbt_dag():
    DbtTaskGroup(
        group_id="northwind_dbt",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        # Propaga retries e retry_delay para cada task gerada pelo Cosmos
        operator_args={
            "retries": default_args["retries"],
            "retry_delay": default_args["retry_delay"],
        },
    )


northwind_dbt_dag()

from datetime import UTC, datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG


def _today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def transform_silver() -> None:
    from pipeline.silver import clean_trades
    clean_trades.main(_today())


def transform_gold() -> None:
    from pipeline.gold import agg_metrics, features
    date = _today()
    agg_metrics.main(date)
    features.main(date)

def dbt_build() -> None:
    import subprocess

    env = {"DBT_PROFILES_DIR": "/opt/airflow/.dbt"}
    for cmd in (["dbt", "--no-partial-parse", "run"], ["dbt", "--no-partial-parse", "test"]):
        result = subprocess.run(
            cmd,
            cwd="/opt/airflow/dbt",
            env={**__import__("os").environ, **env},
            capture_output=True,
            text=True,
            check=False,
        )
        print(result.stdout)
        print(result.stderr)
        if result.returncode != 0:
            raise RuntimeError(f"{' '.join(cmd)} a échoué (code {result.returncode})")


default_args = {
    "owner": "marketpulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="stock_pipeline",
    description="Batch horaire : Silver -> Gold -> dbt (Bronze alimenté en continu, hors Airflow)",
        start_date=datetime(2026, 9, 1, tzinfo=UTC),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["marketpulse", "etl"],
) as dag:
    t_silver = PythonOperator(task_id="transform_silver", python_callable=transform_silver)
    t_gold = PythonOperator(task_id="transform_gold", python_callable=transform_gold)
    t_dbt = PythonOperator(task_id="dbt_build", python_callable=dbt_build)

    t_silver >> t_gold >> t_dbt
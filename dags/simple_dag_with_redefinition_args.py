import logging

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Конфигурация DAG
OWNER = "i.korsakov"
DAG_ID = "simple_dag_with_redefinition_args"

LONG_DESCRIPTION = """
# LONG DESCRIPTION

"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

# Описание возможных ключей для default_args
# https://github.com/apache/airflow/blob/343d38af380afad2b202838317a47a7b1687f14f/airflow/example_dags/tutorial.py#L39
args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year=2025, month=1, day=1, tz="UTC"),
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def simple_task(**context) -> None:
    """
    Печатает контекст DAG.

    @param context: Контекст DAG.
    @return: Ничего не возвращает.
    """

    for context_key, context_key_value in context.items():
        logging.info(
            f"key_name – {context_key} | "
            f"value_name – {context_key_value} | "
            f"type_value_name – {type(context_key_value)}",
        )


def zero_division() -> None:
    """
    Деление на ноль для проверки переопределения аргументов.

    :return: Ничего не возвращает.
    """
    logging.info("Начинаем деление на ноль.")
    foo = 1 / 0  # Деление на ноль
    logging.info(f"Результат деления: {foo}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 10 * * *",
    default_args=args,
    tags=["context"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    simple_task = PythonOperator(
        task_id="simple_task",
        python_callable=simple_task,
    )

    zero_division_0 = PythonOperator(
        task_id="zero_division_0",
        python_callable=zero_division,
    )

    zero_division_1 = PythonOperator(
        task_id="zero_division_1",
        python_callable=zero_division,
        default_args={
            "retries": 10,
            "retry_delay": pendulum.duration(minutes=15),
        },
    )

    end = EmptyOperator(
        task_id="end",
        default_args={"schedule_interval": "0 11 * * *"},
    )

    start >> simple_task >> [zero_division_0, zero_division_1] >> end

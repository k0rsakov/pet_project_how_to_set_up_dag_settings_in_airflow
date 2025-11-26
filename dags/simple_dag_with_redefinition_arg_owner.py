import logging

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Конфигурация DAG
OWNER = "i.korsakov"
DAG_ID = "simple_dag_with_redefinition_arg_owner"

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
    "retry_delay": pendulum.duration(minutes=1),
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


def some_business_logic(number: int = 0) -> None:
    """
    Выполняет некоторую бизнес-логику.

    Получает число и проверяет его на чётность.
    Если число чётное - успех, если нечётное - выбрасывает исключение.

    :param number: Число для проверки.
    :return: Ничего не возвращает.
    :raises ValueError: Если число нечётное.
    """
    logging.info("Executing some business logic.")

    if number % 2 == 0:
        logging.info(f"✅ Число {number} является ЧЁТНЫМ.  Операция выполнена успешно!")
    else:
        logging.error(f"❌ Число {number} является НЕЧЁТНЫМ. Операция завершилась ошибкой!")
        raise ValueError(f"Число {number} нечётное!  Бизнес-логика требует чётное число.")  # noqa: TRY003, EM102


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

    some_business_logic_0 = PythonOperator(
        task_id="some_business_logic_0",
        python_callable=some_business_logic,
        op_kwargs={
            "number": 4,
        },
    )

    some_business_logic_1 = PythonOperator(
        task_id="some_business_logic_1",
        python_callable=some_business_logic,
        op_kwargs={
            "number": 5,
        },
        default_args={
            "owner": "Foo Bar",
            "email": "f.bar@example.com",
            "email_on_failure": True,
        },
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> simple_task >> [some_business_logic_0, some_business_logic_1] >> end

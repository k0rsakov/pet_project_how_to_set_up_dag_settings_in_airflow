from create_pool import create_airflow_pool


if __name__ == "__main__":
    create_airflow_pool(
        name="foo_bar_pool",
        slots=1,
        description="Pool для демо",
        user_name="airflow",
        password_auth="airflow"
    )

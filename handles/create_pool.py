import json

import requests


def create_airflow_pool(
        name: str | None = None,
        slots: int | None = None,
        description: str | None = None,
        include_deferred: bool = False,
        airflow_api_url: str = "http://localhost:8080/api/v1/pools",
        user_name: str | None = None,
        password_auth: str | None = None,
):
    """
    Создаёт pool в Apache Airflow через REST API.

    :param name: Название pool (должно быть уникальным)
    :param slots: Количество слотов в pool (целое число)
    :param description: Описание pool (опционально)
    :param include_deferred: Включать ли отложенные задачи в подсчет слотов
    :param airflow_api_url: URL Airflow API для pools
    :param user_name: Имя пользователя Airflow API
    :param password_auth: Пароль пользователя Airflow API
    """
    headers = {
        "Content-Type": "application/json",
    }

    data = {
        "name": name,
        "slots": slots,
    }

    if description is not None:
        data["description"] = description

    if include_deferred:
        data["include_deferred"] = include_deferred

    try:
        response = requests.post(
            url=airflow_api_url,
            auth=(user_name, password_auth),
            data=json.dumps(data),
            headers=headers,
            timeout=600,
        )

        if response.status_code in {200, 201}:
            print(f"Pool '{name}' успешно создан с {slots} слотами.")
        elif response.status_code == 409:  # noqa: PLR2004
            print(f"Pool '{name}' уже существует.")
        else:
            print(f"Ошибка при создании pool '{name}': {response.status_code} - {response.text}")
            response. raise_for_status()

    except requests.exceptions.RequestException as e:
        print(f"Ошибка соединения с Airflow API: {e}")
        raise

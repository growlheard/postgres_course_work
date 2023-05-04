import json
import psycopg2

from config import config


def create_database(database_name: str, params: dict) -> None:
    """
    Создание базы данных и таблиц для сохранения данных о компаниях и вакансиях на HeadHunter.
        :param database_name:
        :param params:
        :return:
    """
    try:
        conn = psycopg2.connect(dbname='postgres', **params)
    except psycopg2.OperationalError as e:
        print(f"Невозможно подключиться к БД. Проверьте параметры. Error message: {e}")
        return

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{database_name}'")
    exists = cur.fetchone()

    if exists:
        cur.execute(f'DROP DATABASE {database_name}')

    cur.execute(f'CREATE DATABASE {database_name}')

    cur.close()
    conn.close()

    try:
        conn = psycopg2.connect(dbname=database_name, **params)
    except psycopg2.OperationalError as e:
        print(f"Невозможно подключиться к БД. Проверьте параметры. Error message: {e}")
        return
    cur = conn.cursor()

    # Создание таблицы companies
    cur.execute('''CREATE TABLE IF NOT EXISTS companies (
                       companies_id INT,
                       companies_name varchar(255) NOT NULL,
                       CONSTRAINT pk_companies_companies_id PRIMARY KEY (companies_id)
                   )''')
    cur.execute(""" CREATE TABLE IF NOT EXISTS vacancies (
                     vacancy_id int PRIMARY KEY,
                     title varchar(255) NOT NULL,
                     city varchar(255),
                     url text, 
                     salary int,
                     companies_id INT,
                     company_name varchar(100) NOT NULL,
                     requirement TEXT,
                     responsibility TEXT,
                     CONSTRAINT fk_vacancies_companies FOREIGN KEY(companies_id) REFERENCES companies(companies_id)   
                    )
                """)

    # Применение изменений в базе данных
    conn.commit()
    print("Таблицы успешно созданы")
    conn.close()


def insert_data_to_db(database_name: str, params: dict, json_file_path: str) -> None:
    """
    Вставляет данные из файла JSON в таблицы companies и vacancies базы данных.
        :param database_name: Название базы данных.
        :type database_name: str

        :param params: Параметры подключения к базе данных.
        :type params: dict

        :param json_file_path: Путь к файлу JSON с данными.
        :type json_file_path: str

        :return: None
        :rtype: None
    """
    try:
        conn = psycopg2.connect(dbname=database_name, **params)
    except psycopg2.OperationalError as e:
        print(f"Unable to connect to database {database_name}. Error message: {e}")
        return

    conn.autocommit = True

    # Чтение данных из файла JSON
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Вставка данных в таблицу vacancies
    with conn.cursor() as cur:
        for vacancy in data:
            # Вставка данных о компании, если такой компании еще нет в таблице
            company_id = vacancy['employer']['id']
            company_name = vacancy['employer']['name']
            cur.execute(
                """
                INSERT INTO companies (companies_id, companies_name)
                VALUES (%s, %s)
                ON CONFLICT (companies_id) DO NOTHING
                """,
                (company_id, company_name)
            )

            # Вставка данных о вакансии
            cur.execute(
                """
                INSERT INTO vacancies (vacancy_id, title, city, url, salary, companies_id, company_name, responsibility, requirement)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (vacancy_id) DO NOTHING
                """,
                (
                    vacancy['id'],
                    vacancy['name'],
                    vacancy['area']['name'] if vacancy.get('area') and vacancy['area'].get('name') else None,
                    vacancy['alternate_url'],
                    vacancy['salary']['from'] if vacancy.get('salary') and vacancy['salary'].get('from') else None,
                    company_id,
                    company_name,
                    vacancy['snippet']['requirement'] if vacancy.get('snippet') and vacancy['snippet'].get(
                        'requirement') else None,
                    vacancy['snippet']['responsibility'] if vacancy.get('snippet') and vacancy['snippet'].get(
                        'responsibility') else None
                )
            )

    conn.commit()
    print("Данные успешно внесены в таблицы companies и vacancies")
    conn.close()




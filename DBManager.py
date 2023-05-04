import psycopg2


class DBManager:
    def __init__(self, database_name: str, params: dict):
        self.database_name = database_name
        self.params = params

    def _connect(self):
        """
            Устанавливает соединение с базой данных.
        """
        self.conn = psycopg2.connect(database=self.database_name, **self.params)
        self.conn.autocommit = True

    def _close(self):
        """
            Закрывает соединение с базой данных.
        """
        self.conn.close()

    def get_companies_and_vacancies_count(self):
        """
        Возвращает список с названиями компаний и количеством вакансий у каждой компании.

        Returns:
            list: Список кортежей с названием компаний и количеством вакансий.
        """
        self._connect()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT companies.companies_name, COUNT(vacancies.vacancy_id) AS vacancies_count
                FROM companies
                LEFT JOIN vacancies ON companies.companies_id = vacancies.companies_id
                GROUP BY companies.companies_name;
            """)
            result = cur.fetchall()
        self._close()
        return result

    def get_all_vacancies(self):
        """
            Возвращает список вакансий с названием компаний, должностью, зарплатой и ссылкой на вакансию.
            Сортирует вакансии по зарплате в порядке убывания, а вакансии без указанной зарплаты - в конце списка.

            Returns:
                list: Список кортежей с названием компаний, должностью, зарплатой и ссылкой на вакансию.
        """
        self._connect()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT companies.companies_name, vacancies.title, vacancies.salary, vacancies.url
                FROM vacancies
                JOIN companies ON vacancies.companies_id = companies.companies_id
                ORDER BY CASE 
                    WHEN vacancies.salary IS NULL THEN 1 
                    ELSE 0 END, 
                    vacancies.salary DESC;
            """)
            result = cur.fetchall()
        self._close()
        return result

    def get_avg_salary(self):
        """
            Возвращает среднюю зарплату по всем вакансиям, у которых указана зарплата.

            Returns:
                float: Средняя зарплата.
        """
        self._connect()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT AVG(salary)
                FROM vacancies
                WHERE salary IS NOT NULL;
            """)
            result = cur.fetchone()[0]
        self._close()
        return result

    def get_vacancies_with_higher_salary(self):
        """
            Получает вакансии с зарплатой выше среднего значения.

            Returns:
                Список кортежей (companies_name, title, salary, url),
                где companies_name - название компании,
                title - название вакансии,
                salary - зарплата,
                url - ссылка на страницу вакансии.
            """
        self._connect()
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT companies.companies_name, vacancies.title, vacancies.salary, vacancies.url
                FROM vacancies
                JOIN companies ON vacancies.companies_id = companies.companies_id
                WHERE vacancies.salary > {self.get_avg_salary()};
            """)
            result = cur.fetchall()
        self._close()
        return result

    def get_vacancies_with_keyword(self, keyword: str):
        """
            Получает вакансии, содержащие заданный ключевой слово в названии.

            Args:
                keyword: Строка, содержащая ключевое слово для поиска.

            Returns:
                Список кортежей (companies_name, title, salary, url),
                где companies_name - название компании,
                title - название вакансии,
                salary - зарплата,
                url - ссылка на страницу вакансии.
            """
        self._connect()
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT companies.companies_name, vacancies.title, vacancies.salary, vacancies.url
                FROM vacancies
                JOIN companies ON vacancies.companies_id = companies.companies_id
                WHERE LOWER(vacancies.title) LIKE LOWER('%{keyword}%');
            """)
            result = cur.fetchall()
        self._close()
        return result

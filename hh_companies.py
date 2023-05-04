import json

import requests


class Engine:

    def get_companies(self, url: str, params: dict, headers: dict):
        """Абстрактный метод для выполнения запроса
        на получение вакансий на сайтах"""
        companies = requests.get(url=url, params=params, headers=headers)
        return companies.json()

    @staticmethod
    def save_to_json(data: list, file_name: str) -> None:
        """Метод для сохранения списка вакансий в json файл"""
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    @staticmethod
    def read_from_json(file_name: str) -> list:
        """Метод для чтения списка вакансий из json файла"""
        with open(file_name, 'r', encoding='utf-8') as f:
            return json.load(f)


class HH(Engine):
    """Класс HeadHunter для сбора информации о работодателях и их вакансиях на сайте hh.ru"""

    def __init__(self, hh_company: str):
        """Конструктор класса HeadHunter"""
        self.hh_company = hh_company
        self.url = "https://api.hh.ru/vacancies"
        self.params = {'employer_id': '', 'per_page': 100, 'page': 0}
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    def get_companies(self, **kwargs):
        """Метод для выполнения запроса на получение информации о работодателях и их вакансиях на сайте hh.ru"""
        employers_url = "https://api.hh.ru/employers"
        employers_params = {'text': self.hh_company}
        employers_response = requests.get(url=employers_url, params=employers_params, headers=self.headers)
        employers = employers_response.json()

        employers_ids = [employer['id'] for employer in employers['items']]

        vacancies = []
        for employer_id in employers_ids:
            self.params['employer_id'] = employer_id
            vacancies_url = self.url
            vacancies_response = requests.get(url=vacancies_url, params=self.params, headers=self.headers)
            vacancies_data = vacancies_response.json()

            vacancies.extend(vacancies_data['items'])

        self.save_to_json(vacancies, f'{self.hh_company}.json')


from DBManager import DBManager
from Utils import *
from hh_companies import HH
from config import config


def Main():
    company_name = input('Введите название компании: ')

    # Получение данных о компании с сайта hh.ru
    hh = HH(company_name)
    hh.get_companies()

    # Чтение параметров для подключения к БД из конфигурационного файла
    params = config()

    # Ввод названия базы данных
    db_name = input(f'Как Вы хотите назвать Базу Данных для компании {company_name}?\n  ')

    # Создание базы данных и таблиц
    create_database(db_name, params)

    # Вставка данных в таблицу vacancies
    json_file_path = f"{company_name}.json"
    insert_data_to_db(db_name, params, json_file_path)

    # Создание экземпляра класса для работы с базой данных
    db_manager = DBManager(db_name, params)

    while True:
        print('Выберите действие:')
        print('1. Показать количество вакансий у каждой компании')
        print('2. Показать все вакансии')
        print('3. Показать среднюю зарплату')
        print('4. Показать вакансии с зарплатой выше средней')
        print('5. Поиск вакансий по ключевому слову')
        print('0. Выйти')

        choice = input()

        if choice == '1':
            result = db_manager.get_companies_and_vacancies_count()
            for row in result:
                print(f'{row[0]}: {row[1]} вакансий')
        elif choice == '2':
            result = db_manager.get_all_vacancies()
            for row in result:
                print(f'{row[0]}: {row[1]}, зарплата: {row[2]}, ссылка: {row[3]}')
        elif choice == '3':
            result = db_manager.get_avg_salary()
            print(f'Средняя зарплата: {result}')
        elif choice == '4':
            result = db_manager.get_vacancies_with_higher_salary()
            for row in result:
                print(f'{row[0]}: {row[1]}, зарплата: {row[2]}, ссылка: {row[3]}')
        elif choice == '5':
            keyword = input('Введите ключевое слово: ')
            result = db_manager.get_vacancies_with_keyword(keyword)
            for row in result:
                print(f'{row[0]}: {row[1]}, зарплата: {row[2]}, ссылка: {row[3]}')
        elif choice == '0':
            break
        else:
            print('Некорректный выбор.')


if __name__ == '__main__':
    Main()

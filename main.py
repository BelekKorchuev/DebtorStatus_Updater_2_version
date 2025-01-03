import csv
import os
import subprocess
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd

from Detecting_status_actual import detecting_actualed, source_act_with_pagination, search_act
from Parsing_Sending_DB import parse_debtor_info, status_updating, status_au_updating, inactual_update, \
    prepare_data_for_db
from logScript import logger


# создание веб драйвера с виртуальным дисплем
def create_webdriver():
    """
    Создает WebDriver с виртуальным дисплеем.
    """
    try:
        # Настройка WebDriver
        chrome_options = Options()
        chrome_service = Service(ChromeDriverManager().install())

        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
        return driver
    except Exception as e:
        logger.error(f"Ошибка при создании WebDriver: {e}")
        return None

# Функция для перезапуска драйвера
def restart_driver(driver):
    try:
        driver.quit()  # Завершаем текущую сессию
    except Exception as e:
        logger.error(f"Ошибка при завершении WebDriver: {e}")
    return create_webdriver()

# Функция проверки состояния браузера
def is_browser_alive(driver):
    """
    Проверяет, жив ли браузер.
    :param driver: WebDriver instance.
    :return: True, если браузер работает, иначе False.
    """
    try:
        driver.title  # Пробуем получить заголовок текущей страницы
        return True
    except Exception as e:
        logger.warning(f"Браузер не отвечает: {e}")
        return False

input_file_path = "для проверки.xlsx"
missing_file_path = "missing_data.xlsx"

# метод для сохранения пропущенных записей
def save_missing_data_to_excel(missing_data, file_name="missing_data.xlsx"):
    try:
        # Проверяем, существует ли файл
        if os.path.exists(file_name):
            existing_data = pd.read_excel(file_name, dtype=str).fillna("")
            new_data = pd.DataFrame(missing_data)
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        else:
            # Если файла нет, создаем новый DataFrame
            combined_data = pd.DataFrame(missing_data)

        # Сохраняем данные в Excel
        combined_data.to_excel(file_name, index=False)
        logger.info(f"Пропущенные данные успешно сохранены в файл {file_name}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении пропущенных данных в файл {file_name}: {e}")

# основная функция для обработки
def main():
    try:
        driver = create_webdriver()  # Инициализация WebDriver

        # Проверка, нужно ли перезапустить драйвер
        if not is_browser_alive(driver):
            logger.warning("Браузер перестал отвечать. Перезапуск...")
            driver = restart_driver(driver)

        # Проверка существования входного файла
        if not os.path.exists(input_file_path):
            raise FileNotFoundError(f"Файл {input_file_path} не найден.")

        # по строчная обработка строк их файла
        df = pd.read_excel(input_file_path)

        if df.empty:
            raise ValueError(f"Файл {input_file_path} пуст.")

        # Проверяем наличие нужных столбцов
        required_columns = ['ИНН АУ', 'Должник ссылка']
        if not all(column in df.columns for column in required_columns):
            raise ValueError(f"В Excel-файле должны быть столбцы: {', '.join(required_columns)}")

        missing_data = []  # Список для пропущенных данных

        # Обработка каждой строки
        for index, row in df.iterrows():
            inn_au = row['ИНН АУ']
            link_debtor = row['Должник ссылка']

            logger.info(f"Начало обработки для {inn_au} по ссылке {link_debtor}")

            try:
                # парсинг основной инфы
                main_data, soup = parse_debtor_info(driver, link_debtor, inn_au)

                # если нет данных или не получилось спарсить то сохранение и пропуск
                if main_data is None:
                    logger.warning(f"Не удалось спарсить данные для {inn_au}, добавляем в пропущенные.")
                    missing_data.append({'Инн_ау': str(inn_au), 'Ссылка на должника': str(link_debtor), 'Причина': 'Нет данных'})
                    continue

                # определение статуса
                data = detecting_actualed(driver, soup, main_data)
                logger.info(f'проверка первой строки: {data}')

                # если должник неактуален, то сохранение и переход к след должнику
                if "Не актуален" in data:
                    inactual_update(main_data)
                    logger.info(f"Должник {link_debtor} не актуален, пропускаем.")
                    continue

                # поиск всех актов
                list_of_act = source_act_with_pagination(driver, soup, data)
                logger.info(f'список актов должника: {list_of_act}')

                if not list_of_act:
                    inactual_update(main_data)
                    logger.info(f"Должник {link_debtor} не актуален, пропускаем.")
                    continue

                # определение статуса
                dict_of_data = search_act(driver, list_of_act)
                logger.info(f'результат поиска акта: {dict_of_data}')

                is_parsed_arbitr = dict_of_data.get('Арбитражный управляющий')
                logger.info(f'is_parsed_arbitr: {is_parsed_arbitr}')

                if is_parsed_arbitr is None:
                    prepered_data = prepare_data_for_db(dict_of_data)
                    status_updating(prepered_data)
                else:
                    prepered_data = prepare_data_for_db(dict_of_data)
                    status_au_updating(prepered_data)

            except Exception as e:
                logger.error(f"Неожиданная ошибка при обработке {link_debtor}: {e}")
                missing_data.append({'Инн_ау': str(inn_au), 'Ссылка на должника': str(link_debtor), 'Причина': str(e)})

        # Сохранение всех пропущенных данных в Excel после завершения обработки
        if missing_data:
            save_missing_data_to_excel(missing_data, "пропущенные_данные.xlsx")

    except Exception as e:
        logger.error(f'Ошибка в основной функции: {e}')

    finally:
        if driver:
            driver.quit()
            logger.info("WebDriver закрыт.")

if __name__ == "__main__":
    main()

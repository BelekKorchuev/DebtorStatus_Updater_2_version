import csv
import os
import subprocess
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd

from Detecting_status_actual import detecting_actualed, source_act_with_pagination, search_act
from Parsing_Sending_DB import parse_debtor_info, status_updating, status_au_updating, inactual_update
from logScript import logger

# создание виртуального дисплея
def setup_virtual_display():
    """
    Настройка виртуального дисплея через Xvfb.
    """
    try:
        # Запуск Xvfb
        xvfb_process = subprocess.Popen(['Xvfb', ':107', '-screen', '0', '1920x1080x24', '-nolisten', 'tcp'])
        # Установка переменной окружения DISPLAY
        os.environ["DISPLAY"] = ":107"
        logger.info("Виртуальный дисплей успешно настроен с использованием Xvfb.")
        return xvfb_process
    except Exception as e:
        logger.error(f"Ошибка при настройке виртуального дисплея: {e}")
        return None

# создание веб драйвера с виртуальным дисплем
def create_webdriver_with_display():
    """
    Создает WebDriver с виртуальным дисплеем.
    """
    try:
        # # Настройка виртуального дисплея
        # xvfb_process = setup_virtual_display()
        # if not xvfb_process:
        #     raise RuntimeError("Не удалось настроить виртуальный дисплей.")

        # Настройка WebDriver
        chrome_options = Options()
        # chrome_options.add_argument("--no-sandbox")
        # chrome_options.add_argument("--disable-dev-shm-usage")
        # chrome_options.add_argument("--disable-gpu")
        # chrome_options.add_argument("--disable-extensions")
        chrome_service = Service(ChromeDriverManager().install())

        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
        # driver.xvfb_process = xvfb_process  # Сохраняем процесс для последующего завершения
        return driver
    except Exception as e:
        logger.error(f"Ошибка при создании WebDriver: {e}")
        return None

# очистка виртуального дисплея
def cleanup_virtual_display(driver):
    """
    Завершает процесс Xvfb.
    """
    if hasattr(driver, "xvfb_process") and driver.xvfb_process:
        driver.xvfb_process.terminate()
        logger.info("Процесс Xvfb завершен.")

# Функция для перезапуска драйвера
def restart_driver(driver):
    try:
        cleanup_virtual_display(driver)
        driver.quit()  # Завершаем текущую сессию
    except Exception as e:
        logger.error(f"Ошибка при завершении WebDriver: {e}")
    return create_webdriver_with_display()

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



# сохранение припушенных ссылок
def save_skipped_links_to_csv(file_path, skipped_links):
    if skipped_links:
        write_mode = 'a' if os.path.exists(file_path) else 'w'
        with open(file_path, mode=write_mode, newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=["ФИО", "Ссылка"])
            if write_mode == 'w':  # Добавляем заголовки только для нового файла
                writer.writeheader()
            writer.writerows(skipped_links)
        logger.info(f"Пропущенные ссылки сохранены в файл: {file_path}")

# основная функция для обработки
def main():
    try:
        driver = create_webdriver_with_display()  # Инициализация WebDriver

        # Проверка, нужно ли перезапустить драйвер
        if not is_browser_alive(driver):
            logger.warning("Браузер перестал отвечать. Перезапуск...")
            driver = restart_driver(driver)

        # по строчная обработка строк их файла
        df = pd.read_excel(file_path)

        # Проверяем наличие нужных столбцов
        required_columns = ['ФИО', 'Ссылка на должника']
        if not all(column in df.columns for column in required_columns):
            raise ValueError(f"В Excel-файле должны быть столбцы: {', '.join(required_columns)}")

        # Обработка каждой строки
        for index, row in df.iterrows():
            inn_au = row['Инн_ау']
            link_debtor = row['Ссылка на должника']

            logger.info(f"Начало обработки для {inn_au} по ссылке {link_debtor}")

            try:
                # парсинг основной инфы
                mian_data, soup = parse_debtor_info(driver, link_debtor, inn_au)

                # если нет данных или не получилось спарсить то сохранение и пропуск
                if mian_data is None:
                    # сохранение с экзель пропущенных ссылок
                    continue

                # определение статуса
                data = detecting_actualed(driver, soup, mian_data)

                # если должник неактуален, то сохранение и переход к след должнику
                if "Не актуален" in data:
                    inactual_update(data)
                    # не актуален переход к след должнику
                    continue

                # поиск всех актов
                list_of_act = source_act_with_pagination(driver, soup, data)

                # определение статуса
                dict_of_data = search_act(driver, list_of_act)

                is_parsed_arbitr = dict_of_data.get('Арбитражный управляющий')
                if is_parsed_arbitr is None:
                    status_updating(dict_of_data)
                else:
                    status_au_updating(dict_of_data)



            except Exception as e:
                logger.error(f"Неожиданная ошибка при обработке {link_debtor}: {e}")
                # запись в экезель пропущенных

    except Exception as e:
        logger.error(f'ошибка при в основной функции')

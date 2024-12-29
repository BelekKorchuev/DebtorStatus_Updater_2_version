import re
import time
from dotenv import load_dotenv
from psycopg2 import OperationalError
from logScript import logger
import os
import psycopg2
from bs4 import BeautifulSoup

load_dotenv(dotenv_path='.env')

db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

# Функция для подключения к базе данных
def get_db_connection():
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        return connection
    except OperationalError as e:
        logger.error(f"Ошибка при подключении: {e}")
        time.sleep(5)
        return get_db_connection()

# Функция для очистки текста
def clean_text(text):
    """Удаляет лишние символы из текста и приводит его в читаемый вид"""
    text = text.replace('\xa0', ' ').replace('\t', ' ').strip()
    # Убираем лишние пробелы, если они есть
    text = " ".join(text.split())
    return text

# извлечение ИНН из фио
def extract_inn(text):
    match = re.search(r'ИНН[:\s]*(\d+)', str(text))
    return match.group(1) if match else None

# Функция для подготовки данных для вставки в базу данных
def prepare_data_for_db(raw_data):
    """Приводит данные к нужному формату для вставки в базу данных"""
    try:
        # Общие данные для всех сообщений
        data = raw_data.get('дата', '')
        massege_type = raw_data.get('тип_сообщения','')
        message_link = raw_data.get('сообщение_ссылка', '')
        debtor = clean_text(raw_data.get('должник', ''))
        debtor_link = raw_data.get('должник_ссылка', '')
        arbiter = clean_text(raw_data.get('арбитр', ''))
        arbitr_inn_db = raw_data.get('Инн_ау', '')
        arbiter_link = raw_data.get('арбитр_ссылка', '')

        relevance = raw_data.get('Актуальность', '')
        status = clean_text(raw_data.get('статус', ''))

        # Данные о должнике
        debtor_fio = raw_data.get('Полное_имя', '')
        inn = clean_text(raw_data.get('ИНН', ''))
        birth_date = raw_data.get('Дата рождения', '')
        birth_place = clean_text(raw_data.get('Место рождения', ''))
        region_of_the_bankruptcy_case = raw_data.get('Регион ведения дела о банкротстве', '')
        ogrnip = raw_data.get('ОГРНИП', '')
        snils = clean_text(raw_data.get('СНИЛС', ''))
        previous_fullname = raw_data.get('Ранее имевшиеся ФИО', '')
        type_of_debtor = raw_data.get('Категория должника', '')
        residence = clean_text(raw_data.get('Место жительства', ''))
        additional_information = raw_data.get('Дополнительная информация', '')
        short_name = raw_data.get('Краткое наименование', '')
        full_name = raw_data.get('Полное наименование', '')
        address = clean_text(raw_data.get('Адрес', ''))
        phone_number = raw_data.get('Телефон', '')
        ogrn = clean_text(raw_data.get('ОГРН', ''))
        okpo = raw_data.get('ОКПО', '')
        organizational_and_legal_form = raw_data.get('Организационно-правовая форма', '')


        # Данные об арбитраже
        arbiter_name = clean_text(raw_data.get('Арбитражный управляющий', ''))
        correspondence_address = clean_text(raw_data.get('Адрес для корреспонденции', ''))
        email = clean_text(raw_data.get('E-mail', ''))
        sro_au = clean_text(raw_data.get('СРО АУ', ''))
        sro_address = clean_text(raw_data.get('Адрес СРО АУ', ''))

        case_number = clean_text(raw_data.get('номер_дела', ''))
        text = clean_text(raw_data.get('текст', ''))
        files_link = raw_data.get('файлы', '')


        # Подготовленные данные для вставки
        prepared_data = {
            'дата': data,
            'тип_сообщения': massege_type,
            'сообщение_ссылка': message_link,
            'должник': debtor,
            'должник_ссылка': debtor_link,
            'арбитр': arbiter,
            'Инн_ау': arbitr_inn_db,
            'арбитр_ссылка': arbiter_link,
            "Актуальность": relevance,
            'статус': status,

            'Полное_имя': debtor_fio,
            'ИНН': inn,
            'Дата_рождения': birth_date,
            'Место_рождения': birth_place,
            'Регион_ведения_дела_о_банкротстве': region_of_the_bankruptcy_case,
            'ОГРНИП': ogrnip,
            'СНИЛС': snils,
            'Ранее_имевшиеся_ФИО': previous_fullname,
            'Категория_должника': type_of_debtor,
            'Место_жительства': residence,
            'Дополнительная_информация': additional_information,
            'Краткое_наименование': short_name,
            'Полное_наименование': full_name,
            'Адрес': address,
            'Телефон': phone_number,
            'ОГРН': ogrn,
            'ОКПО': okpo,
            'Организационно_правовая_форма': organizational_and_legal_form,

            'номер_дела': case_number,
            'текст': text,
            'файлы': files_link,

            'Арбитражный_управляющий': arbiter_name,
            'Адрес_для_корреспонденции': correspondence_address,
            'e_mail': email,
            'СРО_АУ': sro_au,
            'Адрес_СРО_АУ': sro_address,
        }

        return prepared_data
    except Exception as e:
        logger.error(f'ошибка в методе prepare_data_for_db: {e}')

# отправка данных для обновления статуса в базы данных OurCRM и default_db
def status_au_updating(data):
    try:
        conn_default = get_db_connection()

        cursor_default =conn_default.cursor()

        # SQL-запрос для вставки данных
        query = '''
                    INSERT INTO debtor_status_newau (
                        дата, тип_сообщения, сообщение_ссылка, должник, должник_ссылка, арбитр, 
                        Инн_ау, арбитр_ссылка, Актуальность, статус, номер_дела, текст, файлы, 
                        Полное_имя, ИНН, Дата_рождения, Место_рождения, Регион_ведения_дела_о_банкротстве, 
                        ОГРНИП, СНИЛС, Ранее_имевшиеся_ФИО, Категория_должника, Место_жительства, 
                        Дополнительная_информация, Краткое_наименование, Полное_наименование, Адрес, 
                        Телефон, ОГРН, ОКПО, Организационно_правовая_форма, Арбитражный_управляющий, 
                        Адрес_для_корреспонденции, e_mail, СРО_АУ, Адрес_СРО_АУ
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                '''

        # Значения для вставки из переданных данных
        values = (
            data.get('дата'),
            data.get('тип_сообщения'),
            data.get('сообщение_ссылка'),
            data.get('должник'),
            data.get('должник_ссылка'),
            data.get('арбитр'),
            data.get('Инн_ау'),
            data.get('арбитр_ссылка'),
            data.get('Актуальность'),
            data.get('статус'),
            data.get('номер_дела'),
            data.get('текст'),
            data.get('файлы'),
            data.get('Полное_имя'),
            data.get('ИНН'),
            data.get('Дата_рождения'),
            data.get('Место_рождения'),
            data.get('Регион_ведения_дела_о_банкротстве'),
            data.get('ОГРНИП'),
            data.get('СНИЛС'),
            data.get('Ранее_имевшиеся_ФИО'),
            data.get('Категория_должника'),
            data.get('Место_жительства'),
            data.get('Дополнительная_информация'),
            data.get('Краткое_наименование'),
            data.get('Полное_наименование'),
            data.get('Адрес'),
            data.get('Телефон'),
            data.get('ОГРН'),
            data.get('ОКПО'),
            data.get('Организационно_правовая_форма'),
            data.get('Арбитражный_управляющий'),
            data.get('Адрес_для_корреспонденции'),
            data.get('e_mail'),
            data.get('СРО_АУ'),
            data.get('Адрес_СРО_АУ')
        )

        cursor_default.execute(query, values)
        logger.info('отправил данные для обновления смену АУ')

        # SQL-запрос для вставки данных
        query_default_au = '''
                    UPDATE arbitr_managers 
                    SET ФИО_АУ = %s, ссылка_ЕФРСБ = %s ,город_АУ = %s,
                    почта_ау = %s, СРО_АУ = %s
                    WHERE ИНН_АУ = %s
                    '''

        values_default_au = (
            data.get('ФИО_АУ'), data.get('арбитр_ссылка'), data.get('адрес_корреспонденции'),
            data.get('почта'), data.get('СРО_АУ'), data.get('Инн_ау'),
        )
        # Выполняем запрос с передачей данных из словаря
        cursor_default.execute(query_default_au, values_default_au)

        # SQL-запрос для вставки данных
        query_default = '''
            UPDATE dolzhnik 
            SET текущий_статус = %s
            WHERE Инн_Должника = %s
            '''

        values_default = (
            data.get('cудебный_акт'), data.get('ИНН')
        )
        # Выполняем запрос с передачей данных из словаря
        cursor_default.execute(query_default, values_default)
        logger.info('отправил данные в наш базу')

        # Фиксируем изменения
        conn_default.commit()


        logger.info(f"Данные успешно добавлены в базу для {data['ИНН']}")
    except Exception as e:
        logger.error(f"Ошибка вставки данных в базы для ИНН: {data.get('ИНН')}. Ошибка: {e}")
        # Фиксируем изменения
        if conn_default:
            conn_default.rollback()
    finally:
        if cursor_default:
            cursor_default.close()
        if conn_default:
            conn_default.close()

# отправка данных для обновления статуса и АУ в базы данных OurCRM и default_db
def status_updating(data):
    try:
        conn_default = get_db_connection()

        # Создаем курсор
        cursor_default =conn_default.cursor()

        # SQL-запрос для вставки данных
        query = '''
                    INSERT INTO debtor_status_newau (
                        дата, тип_сообщения, сообщение_ссылка, должник, должник_ссылка, арбитр, 
                        Инн_ау, арбитр_ссылка, Актуальность, статус, номер_дела, текст, файлы, 
                        Полное_имя, ИНН, Дата_рождения, Место_рождения, Регион_ведения_дела_о_банкротстве, 
                        ОГРНИП, СНИЛС, Ранее_имевшиеся_ФИО, Категория_должника, Место_жительства, 
                        Дополнительная_информация, Краткое_наименование, Полное_наименование, Адрес, 
                        Телефон, ОГРН, ОКПО, Организационно_правовая_форма
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                '''

        # Подготовка значений для вставки
        values = (
            data.get('дата'),
            data.get('тип_сообщения'),
            data.get('сообщение_ссылка'),
            data.get('должник'),
            data.get('должник_ссылка'),
            data.get('арбитр'),
            data.get('Инн_ау'),
            data.get('арбитр_ссылка'),
            data.get('Актуальность'),
            data.get('статус'),
            data.get('номер_дела'),
            data.get('текст'),
            data.get('файлы'),
            data.get('Полное_имя'),
            data.get('ИНН'),
            data.get('Дата_рождения'),
            data.get('Место_рождения'),
            data.get('Регион_ведения_дела_о_банкротстве'),
            data.get('ОГРНИП'),
            data.get('СНИЛС'),
            data.get('Ранее_имевшиеся_ФИО'),
            data.get('Категория_должника'),
            data.get('Место_жительства'),
            data.get('Дополнительная_информация'),
            data.get('Краткое_наименование'),
            data.get('Полное_наименование'),
            data.get('Адрес'),
            data.get('Телефон'),
            data.get('ОГРН'),
            data.get('ОКПО'),
            data.get('Организационно_правовая_форма')
        )
        cursor_default.execute(query, values)
        logger.info('получилось отправить в status_updating для обновления статуса')

        # SQL-запрос для вставки данных
        query_default = '''
                   UPDATE dolzhnik 
                   SET ИНН_АУ = %s, текущий_статус = %s, Актуальность = %s
                   WHERE Инн_Должника = %s
                   '''

        values_default = (
            data.get('Инн_ау'), data.get('статус'), data.get('Актуальность'), data.get('ИНН')
        )
        # Выполняем запрос с передачей данных из словаря
        cursor_default.execute(query_default, values_default)
        logger.info('успешно отправил в таблицу dolzhnik ')

        # Фиксируем измене ния
        conn_default.commit()

        logger.info(f"Данные успешно добавлены в базу для {data['ИНН']}")
    except Exception as e:
        logger.error(f"Ошибка вставки данных в базы для ИНН: {data.get('ИНН')}. Ошибка: {e}")
        # Фиксируем изменения
        if conn_default:
            conn_default.rollback()
    finally:
        if cursor_default:
            cursor_default.close()
        if conn_default:
            conn_default.close()

# отправка данных для неактуальных
def inactual_update(data):
    try:
        conn_default = get_db_connection()

        # Создаем курсор
        cursor_default = conn_default.cursor()

        # SQL-запрос для вставки данных
        query_default = '''
                           UPDATE debtor_status_newau 
                           SET Инн_ау = %s, Актуальность = %s
                           WHERE должник_ссылка = %s
                           '''

        values_default = (
            data.get('Инн_ау'), data.get('Актуальность'), data.get('должник_ссылка')
        )
        # Выполняем запрос с передачей данных из словаря
        cursor_default.execute(query_default, values_default)

        # Фиксируем изменения
        conn_default.commit()


        logger.info(f"Данные успешно добавлены в базу для {data.get('ИНН')}")
    except Exception as e:
        logger.error(f"Ошибка вставки данных в базы для ИНН: {data.get('ИНН')}. Ошибка: {e}")
        # Фиксируем изменения
        if conn_default:
            conn_default.rollback()
    finally:
        if cursor_default:
            cursor_default.close()
        if conn_default:
            conn_default.close()

# парсинг основой инфы
def parse_debtor_info(driver, link_debtor, inn_au):

    driver.get(link_debtor)

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    data = {
        'Инн_ау': inn_au,
        'должник_ссылка': link_debtor,
    }

    last_name, first_name, middle_name = None, None, None
    try:
        # Основная информация
        table_main = soup.find('table', class_='au')
        if table_main:
            rows = table_main.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) == 2:
                    field = cells[0].text.strip()
                    value = cells[1].text.strip()

                    if "Фамилия" in field:
                        last_name = value
                    elif "Имя" in field:
                        first_name = value
                    elif "Отчество" in field:
                        middle_name = value
                    elif "Краткое наименование" in field:
                        data['Краткое наименование'] = value
                    elif "Полное наименование" in field:
                        data['Полное наименование'] = value
                    else:
                        data[field] = value

            # Объединяем Фамилию, Имя и Отчество в один столбец
            data["Полное_имя"] = " ".join(filter(None, [last_name, first_name, middle_name]))
            logger.info(f"Полное имя: {data['Полное_имя']}")

            logger.info(f'Данные должника спарсины {data}')

        return data, soup
    except Exception as e:
        logger.error(f'не удалось спарсить инфу должника')
        return None


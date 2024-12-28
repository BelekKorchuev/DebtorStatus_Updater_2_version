import time
from collections import deque
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from logScript import logger
from bs4 import BeautifulSoup
from selenium.common import WebDriverException


list_of_incorrect_type = {'о завершении конкурсного производства', 'о завершении реализации имущества гражданина', 'о прекращении производства по делу'}
list_of_status = {'о введении наблюдения',
                  'о признании обоснованным заявления о признании гражданина банкротом и введении реструктуризации его долгов',
                  'о признании должника банкротом и открытии конкурсного производства',
                  'о передаче дела на рассмотрение другого арбитражного суда об утверждении плана реструктуризации долгов гражданина',
                  'о признании должника банкротом и введении реализации имущества гражданина',
                  'о напременении в отношении гражданина правила об освобождении от исполнения обязательств',
                  'о завершении реализации имущества гражданина', 'о прекращении производства по делу',
                  'о признании гражданина банкротом и введении реализации имущества гражданина'}
changed_au = {'об утверждении арбитражного управляющего'}

# Загружаем очередь из файла при старте программы
checked_messages = deque(maxlen=500)


# определения актуальности должника по первой строке
def detecting_actualed(driver, soup, data):
    try:
        table = soup.find('table', class_='bank')
        if table:
            rows = table.find_all('tr')
            cells = rows[1].find_all('td')
            logger.info('Последнее сообщение найдено')
            if len(cells) == 4:
                status = cells[1].text.strip()
                logger.info(f'язейка собщения {status}')
                inner_link = cells[1].find("a")["href"] if cells[1].find("a") else None
                inner_link_http = f"https://old.bankrot.fedresurs.ru/{inner_link}"
                if "Сообщение о судебном акте" in status:
                    logger.info(f"ячейка это {status}")
                    logger.info(f'ссылка на сообшение {inner_link_http}')
                    try:
                        driver.get(inner_link_http)
                        html2 = driver.page_source
                        soup = BeautifulSoup(html2, 'html.parser')
                        table = soup.find('table', class_='headInfo')
                        logger.info('зашел в сообщение о акте')
                        if table:
                            rows = table.find_all('tr')
                            logger.info("нашел tr")
                            for rower in rows:
                                cells = rower.find_all('td')
                                logger.info('нашел td')
                                if len(cells) == 2:  # Убедимся, что строка содержит две ячейки
                                    field_name = cells[0].text.strip()  # Название поля в первой ячейке
                                    field_value = cells[1].text.strip()  # Значение поля во второй ячейке
                                    logger.info(f"Поле: {field_name}, Значение: {field_value}")

                                    if "судебный акт" in field_name.lower() or "тип решения" in field_name.lower():
                                        if field_value.lower() in list_of_incorrect_type:
                                            data['Актуальность'] = "неактуален"
                                            logger.info(f"Неактуален: {field_value}")
                                            # сразу переход к след должнику
                                            return "Не актуален"
                                        else:
                                            data['Актуальность'] = "актуален"
                                            logger.info("Актуален")
                                            # дальше поиск статуса
                                            return data

                    except WebDriverException as e:
                        logger.error(f"Ошибка при переходе по внутренней ссылке {inner_link}: {e}")
                else:
                    data['Актуальность'] = "актуален"
                    logger.info('актуален')
                    # дальше поиск статуса
                    return data

    except Exception as e:
        logger.error(f'проверить первую строку не получилось')
        return None

# поиск 5 актов или всех актов если их меньше 5
def source_act_with_pagination(driver, soup, data):
    logger.info('началась поиск всех актов у должника')

    messages = []
    limit = 0
    checked_messages = set()
    visited_pages = set()
    needed_stop = False

    while not needed_stop:
        if limit == 5:
            needed_stop = True

        table = soup.find('table', class_='bank')
        if table:
            logger.info('нашел таблицу')
            rows = table.find_all('tr')
            for row in rows:
                logger.info('нашел строки')
                row_class = row.get('class', [])
                if not row_class or 'row' in row_class:
                    # Если это строка с данными сообщения
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        # Извлекаем данные из ячеек
                        date = cells[0].get_text(strip=True)
                        message_title = cells[1].get_text(strip=True)
                        tag = cells[1].find('a')
                        raw_link = tag['onclick'].split("'")[1]
                        link = f"https://old.bankrot.fedresurs.ru{raw_link}"

                        link_arbitr = cells[2].find("a")["href"] if cells[2].find("a") else None
                        published_by = cells[2].get_text(strip=True)

                        if link in checked_messages:
                            needed_stop = True
                            break

                        checked_messages.add(link)
                        logger.info(message_title)
                        if tag and 'onclick' in tag.attrs:  # Безопасная проверка наличия атрибута
                            logger.info(f"Ссылка на сообщение: {link}")
                            if "Сообщение о судебном акте" in message_title:
                                logger.info(f'нашел {message_title}')

                                message_face = {
                                    "дата": date,
                                    "тип_сообщения": message_title,
                                    "сообщение_ссылка": link,
                                    "должник": data.get('Полное_имя'),
                                    "должник_ссылка": data.get('должник_ссылка'),
                                    "арбитр": published_by,
                                    "Инн_ау": data.get("Инн_ау"),
                                    "арбитр_ссылка": f"https://old.bankrot.fedresurs.ru{link_arbitr}" if link_arbitr else "Нет ссылки",
                                    "Актуальность": data.get("Актуальность"),
                                    'статус': "статус не определен"
                                }
                                limit += 1
                                messages.append(message_face)

                # Если это строка с пагинацией
                if 'pager' in row_class:
                    pager_table = row.find_next('table')
                    if not pager_table:
                        logger.info("Таблица пагинации не найдена")
                        return

                    page_elements = pager_table.find_all('a', href=True)
                    if not page_elements:
                        logger.info("Ссылки пагинации отсутствуют")
                        return

                    for page_element in page_elements:

                        href = page_element['href']
                        page_action = href.split("'")[3]  # Получаем 'Page$31'
                        logger.info(f"Обнаружено действие: {page_action}")

                        if page_action in visited_pages:
                            logger.info(f"Страница {page_action} уже обработана, пропускаем")
                            continue

                        # Проверяем, начинается ли href с нужного JavaScript
                        if "javascript:__doPostBack" in href:
                            try:
                                script = """
                                    var theForm = document.forms['aspnetForm'];
                                    if (!theForm) {
                                        theForm = document.aspnetForm;
                                    }
                                    if (!theForm.onsubmit || (theForm.onsubmit() != false)) {
                                        theForm.__EVENTTARGET.value = arguments[0];
                                        theForm.__EVENTARGUMENT.value = arguments[1];
                                        theForm.submit();
                                    }
                                    """
                                logger.info(f"Клик по элементу пагинации: {page_action}")
                                driver.execute_script(script, 'ctl00$cphBody$gvMessages', page_action)

                                # element.click()  # Кликаем по элементу
                                WebDriverWait(driver, 10).until(
                                    EC.presence_of_element_located((By.TAG_NAME, 'html'))
                                )
                                time.sleep(3)  # Ожидание загрузки новой страницы

                                # Обновляем soup для новой страницы и продолжаем обработку
                                soup = BeautifulSoup(driver.page_source, 'html.parser')

                                visited_pages.add(page_action)
                                break
                            except Exception as e:
                                logger.error(f"Ошибка при клике на элемент пагинации: {e}")
                                return
                    else:
                        logger.info("Дополнительных страниц для перехода не найдено")
                        return

    logger.info('закончил поиск актов')
    return messages

# метод для определения нужного акта и статуса должника
def search_act(driver, list_dic):
    try:
        arbitr_data = {}

        for dic in list_dic:
            link = dic.get('сообщение_ссылка', '')

            driver.get(link)
            logger.info(f'текущее сообщение: {link}')

            temporary = {}

            # Получение HTML-кода страницы
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Основная информация
            table_main = soup.find('table', class_='headInfo')
            if table_main:
                rows = table_main.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) == 2:
                        field = cells[0].text.strip()
                        value = cells[1].text.strip()
                        temporary[field] = value

                # Данные о должнике
            debtor_section = soup.find('div', string="Должник")
            if debtor_section:
                debtor_table = debtor_section.find_next('table')
                if debtor_table:
                    debtor_rows = debtor_table.find_all('tr')
                    for row in debtor_rows:
                        cells = row.find_all('td')
                        if len(cells) == 2:
                            field = cells[0].text.strip()
                            value = cells[1].text.strip()
                            temporary[field] = value

            if temporary.get('Судебный акт').lower() == changed_au:
                # Информация об арбитражном управляющем
                arbiter_section = soup.find('div', string="Кем опубликовано")
                if arbiter_section:
                    arbiter_table = arbiter_section.find_next('table')
                    if arbiter_table:
                        arbiter_rows = arbiter_table.find_all('tr')
                        for row in arbiter_rows:
                            cells = row.find_all('td')
                            if len(cells) == 2:
                                field = cells[0].text.strip()
                                value = cells[1].text.strip()
                                if not arbitr_data:
                                    arbitr_data[field] = value

            text_section = soup.find_all('div', class_='msg')
            temporary['текст'] = "; ".join(text.text.strip() for text in text_section if text.text.strip())

            file_links = []
            pinned_files = soup.find_all('a', class_='Reference')
            if pinned_files:
                for file in pinned_files:
                    file_link = file['href'].replace("&amp;", "&")
                    file_links.append(f'https://old.bankrot.fedresurs.ru/{file_link}')
            else:
                file_links.append("Нет файлов")

            temporary['файлы'] = "&&& ".join(file_links)

            dic['номер_дела'] = temporary.get('№ дела')
            dic['текст'] = temporary.get('текст')
            dic['файлы'] = temporary.get('файлы')

            dic['статус'] = temporary.get('Судебный акт', 'статус не определен')
            act_status = temporary.get('Судебный акт', '')

            if act_status == "об утверждении арбитражного управляющего":
                logger.info(f'первый акт про СМЕНУ АУ')
                continue

            if act_status not in list_of_status:
                logger.info(f'акт не тот, что нам нужен')
                continue

            if arbitr_data:
                dic.uptare(arbitr_data)

            logger.info(f'НУЖНЫЙ акт')

            return dic

        return dic
    except Exception as e:
        logger.error(f"НЕ удалось спарсить найденный акт у должника {list_dic[0]['должник_ссылка']}: {e}")
        return
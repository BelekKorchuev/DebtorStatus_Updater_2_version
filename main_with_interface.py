import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText
from threading import Thread
from Detecting_status_actual import detecting_actualed, source_act_with_pagination, search_act, search_au_doc
from Parsing_Sending_DB import parse_debtor_info, status_updating, status_au_updating, inactual_update, prepare_data_for_db
import logging
from webdriver import create_webdriver, is_browser_alive, restart_driver

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Класс для отображения логов в интерфейсе
class LogHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        self.text_widget.insert(tk.END, msg + "\n")
        self.text_widget.see(tk.END)

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Обработка должников")
        self.input_file_path = ""
        self.missing_file_path = "missing_data.xlsx"
        self.create_widgets()

    def create_widgets(self):
        # Выбор файла для обработки
        tk.Label(self.root, text="Файл Excel с данными:").pack(pady=5)
        self.file_entry = tk.Entry(self.root, width=50)
        self.file_entry.pack(pady=5)
        tk.Button(self.root, text="Выбрать файл", command=self.select_file).pack(pady=5)

        # Выбор пути для сохранения пропущенных данных
        tk.Label(self.root, text="Файл для пропущенных данных:").pack(pady=5)
        self.missing_file_entry = tk.Entry(self.root, width=50)
        self.missing_file_entry.insert(0, self.missing_file_path)  # Устанавливаем значение по умолчанию
        self.missing_file_entry.pack(pady=5)
        tk.Button(self.root, text="Выбрать место для сохранения", command=self.select_missing_file).pack(pady=5)

        # Прогресс-бар
        tk.Label(self.root, text="Прогресс:").pack(pady=5)
        self.progress_bar = ttk.Progressbar(self.root, orient="horizontal", mode="determinate", length=300)
        self.progress_bar.pack(pady=5)
        self.progress_label = tk.Label(self.root, text="0%")
        self.progress_label.pack(pady=5)

        # Кнопка запуска обработки
        tk.Button(self.root, text="Запустить обработку", command=self.start_processing).pack(pady=10)

        # Поле для логов
        tk.Label(self.root, text="Логи:").pack(pady=5)
        self.log_text = ScrolledText(self.root, width=80, height=20)
        self.log_text.pack(pady=5)

        # Настройка логов
        handler = LogHandler(self.log_text)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.INFO)

    def select_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if file_path:
            self.input_file_path = file_path
            self.file_entry.delete(0, tk.END)
            self.file_entry.insert(0, file_path)

    def select_missing_file(self):
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            title="Выберите файл для сохранения пропущенных данных"
        )
        if file_path:
            self.missing_file_path = file_path
            self.missing_file_entry.delete(0, tk.END)
            self.missing_file_entry.insert(0, file_path)
        else:
            logging.warning("Пользователь не выбрал файл для сохранения пропущенных данных")

    def start_processing(self):
        if not self.input_file_path:
            messagebox.showerror("Ошибка", "Выберите файл для обработки!")
            return
        if not self.missing_file_path:
            messagebox.showerror("Ошибка", "Выберите путь для сохранения пропущенных данных!")
            return

        # Запуск обработки в отдельном потоке
        thread = Thread(target=self.run_main)
        thread.start()

    def run_main(self):
        try:
            missing_file_path = self.missing_file_entry.get()
            if not missing_file_path:
                messagebox.showerror("Ошибка", "Выберите путь для сохранения пропущенных данных!")
                return

            main(self.input_file_path, self.missing_file_path, self.update_progress)
            self.update_progress(100)  # Устанавливаем 100% после завершения
            messagebox.showinfo("Успех", "Обработка завершена!")
        except Exception as e:
            logging.error(f"Ошибка выполнения: {e}")
            messagebox.showerror("Ошибка", f"Произошла ошибка: {e}")

    def update_progress(self, progress):
        self.progress_bar["value"] = progress
        self.progress_label.config(text=f"{progress:.0f}%")

# Сохранение пропущенных записей
def save_missing_data_to_excel(missing_data, file_name):
    try:
        if not file_name:
            raise ValueError("Имя файла для сохранения пропущенных данных не задано.")

        if os.path.exists(file_name):
            existing_data = pd.read_excel(file_name, dtype=str).fillna("")
            new_data = pd.DataFrame(missing_data)
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        else:
            combined_data = pd.DataFrame(missing_data)
        combined_data.to_excel(file_name, index=False)
        logging.info(f"Пропущенные данные успешно сохранены в файл {file_name}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении пропущенных данных в файл {file_name}: {e}")

# Обновленный main
def main(input_file_path, missing_file_path, update_progress):
    try:
        logging.info(f"Начало обработки файла: {input_file_path}")
        logging.info(f"Пропущенные данные будут сохранены в файл: {missing_file_path}")

        driver = create_webdriver()
        if not driver or not is_browser_alive(driver):
            driver = restart_driver(driver)

        if not os.path.exists(input_file_path):
            raise FileNotFoundError(f"Файл {input_file_path} не найден.")

        df = pd.read_excel(input_file_path, dtype=str)

        if df.empty:
            raise ValueError(f"Файл {input_file_path} пуст.")

        required_columns = ['ИНН АУ', 'Должник ссылка']
        if not all(column in df.columns for column in required_columns):
            raise ValueError(f"В Excel-файле должны быть столбцы: {', '.join(required_columns)}")

        missing_data = []
        total_rows = len(df)
        for index, row in df.iterrows():
            progress = ((index + 1) / total_rows) * 100
            update_progress(progress)
            logging.info(f"Прогресс: {progress:.2f}% (Строка {index + 1} из {total_rows})")


            inn_au = row['ИНН АУ']
            link_debtor = row['Должник ссылка']

            try:
                main_data, soup = parse_debtor_info(driver, link_debtor, inn_au)
                if main_data is None:
                    missing_data.append({'ИНН АУ': str(inn_au), 'Должник ссылка': str(link_debtor), 'Причина': 'Нет данных'})
                    continue

                data = detecting_actualed(driver, soup, main_data)
                # если должник неактуален, то сохранение и переход к след должнику
                if "Не актуален" in data:
                    inactual_update(main_data)
                    logging.info(f"Должник {link_debtor} не актуален, пропускаем.")
                    continue

                list_of_act = source_act_with_pagination(driver, soup, data)
                if not list_of_act:
                    inactual_update(main_data)
                    logging.info(f"Должник {link_debtor} не актуален, пропускаем.")
                    continue

                dict_of_data = search_act(driver, list_of_act)
                is_parsed_arbitr = dict_of_data.get('Арбитражный управляющий')

                if dict_of_data is None:
                    logging.warning(f'Не удалось определить статус (search_act): {dict_of_data}')
                    continue

                found_au_doc = dict_of_data.get('Арбитражный управляющий')
                if found_au_doc is None:
                    logging.info(f'В начале не смог найти акт о смене')
                    dict_of_data = search_au_doc(driver, list_of_act, dict_of_data)
                else:
                    logging.info('Акт о смене есть')

                is_parsed_arbitr = dict_of_data.get('Арбитражный управляющий')
                logging.info(f'is_parsed_arbitr: {is_parsed_arbitr}')

                prepered_data = prepare_data_for_db(dict_of_data)
                if is_parsed_arbitr is None:
                    error_db = status_updating(prepered_data)
                    if error_db:
                        missing_data.append(error_db)
                else:
                    error_db = status_au_updating(prepered_data)
                    if error_db:
                        missing_data.append(error_db)


            except Exception as e:
                missing_data.append({'ИНН АУ': str(inn_au), 'Должник ссылка': str(link_debtor), 'Причина': str(e)})
                driver = restart_driver(driver)

        if missing_data:
            save_missing_data_to_excel(missing_data, missing_file_path)

    except Exception as e:
        logging.error(f"Ошибка в основной функции: {e}")
    finally:
        if 'driver' in locals() and driver:
            driver.quit()
            logging.info("WebDriver закрыт.")

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
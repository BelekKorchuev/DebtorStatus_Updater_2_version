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
        self.root.title("Актуализация Должников")
        self.input_file_path = ""
        self.missing_file_path = "missing_data.xlsx"
        self.selected_column = None  # Хранение выбранного столбца
        self.df_headers = []
        self.stop_processing = False  # Флаг остановки обработки
        self.create_widgets()

    def create_widgets(self):
        # Основной фрейм для организации
        main_frame = tk.Frame(self.root)
        main_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

        # Левый фрейм (для кнопок и выбора файлов)
        left_frame = tk.Frame(main_frame)
        left_frame.grid(row=0, column=0, sticky="nw")

        # Правый фрейм (для логов)
        right_frame = tk.Frame(main_frame)
        right_frame.grid(row=0, column=1, sticky="ne", padx=10)

        # Выбор файла для обработки
        tk.Label(left_frame, text="Файл Excel с данными:").pack(pady=5, anchor="w")
        self.file_entry = tk.Entry(left_frame, width=50)
        self.file_entry.pack(pady=5, anchor="w")
        tk.Button(left_frame, text="Выбрать файл", command=self.select_file).pack(pady=5, anchor="w")

        # Выбор столбцов
        tk.Label(left_frame, text="Выберите столбцы:").pack(pady=5, anchor="w")
        tk.Label(left_frame, text="ИНН АУ:").pack(anchor="w")
        self.inn_combobox = ttk.Combobox(left_frame, state="readonly", width=50)
        self.inn_combobox.pack(pady=5, anchor="w")

        tk.Label(left_frame, text="Ссылка на должника:").pack(anchor="w")
        self.link_combobox = ttk.Combobox(left_frame, state="readonly", width=50)
        self.link_combobox.pack(pady=5, anchor="w")

        # Выбор файла для сохранения пропущенных данных
        tk.Label(left_frame, text="Файл для сохранения пропущенных данных:").pack(pady=5, anchor="w")
        self.missing_file_entry = tk.Entry(left_frame, width=50)
        self.missing_file_entry.insert(0, self.missing_file_path)
        self.missing_file_entry.pack(pady=5, anchor="w")
        tk.Button(left_frame, text="Выбрать файл", command=self.select_missing_file).pack(pady=5, anchor="w")

        # Прогресс-бар
        tk.Label(left_frame, text="Прогресс:").pack(pady=5, anchor="w")
        self.progress_bar = ttk.Progressbar(left_frame, orient="horizontal", mode="determinate", length=300)
        self.progress_bar.pack(pady=5, anchor="w")
        self.progress_label = tk.Label(left_frame, text="0%")
        self.progress_label.pack(pady=5, anchor="w")

        # Кнопки управления
        button_frame = tk.Frame(left_frame)
        button_frame.pack(pady=10, anchor="w")

        tk.Button(button_frame, text="Запустить обработку", command=self.start_processing).pack(side="left", padx=5)
        tk.Button(button_frame, text="Остановить обработку", command=self.stop_processing_action).pack(side="left",
                                                                                                       padx=5)

        # Поле для логов
        tk.Label(right_frame, text="Логи:").pack(pady=5)
        self.log_text = ScrolledText(right_frame, width=75, height=25)
        self.log_text.pack(pady=5)

        # Настройка логов
        handler = LogHandler(self.log_text)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.INFO)

    def stop_processing_action(self):
        self.stop_processing = True
        logging.info("Остановка обработки данных...")

    def select_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if file_path:
            self.input_file_path = file_path
            self.file_entry.delete(0, tk.END)
            self.file_entry.insert(0, file_path)
            self.load_columns()

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

    def load_columns(self):
        try:
            df = pd.read_excel(self.input_file_path, nrows=0)  # Читаем только заголовки
            self.df_headers = df.columns.tolist()

            # Обновляем список столбцов в выпадающем меню
            self.inn_combobox['values'] = self.df_headers
            self.link_combobox['values'] = self.df_headers
            self.inn_combobox.current(0)  # Устанавливаем первый элемент как выбранный
            self.link_combobox.current(1)  # Устанавливаем второй элемент как выбранный
        except Exception as e:
            logging.error(f"Ошибка при чтении столбцов: {e}")
            messagebox.showerror("Ошибка", f"Не удалось загрузить столбцы из файла: {e}")

    def start_processing(self):
        if not self.input_file_path:
            messagebox.showerror("Ошибка", "Выберите файл для обработки!")
            return

        inn_column = self.inn_combobox.get()
        link_column = self.link_combobox.get()
        if not inn_column or not link_column:
            messagebox.showerror("Ошибка", "Выберите столбцы для ИНН АУ и Ссылки на должника!")
            return

        if not self.missing_file_path:
            messagebox.showerror("Ошибка", "Выберите файл для сохранения пропущенных данных!")
            return

        self.stop_processing = False  # Сбрасываем флаг остановки
        # Запуск обработки в отдельном потоке
        thread = Thread(target=self.run_main, args=(inn_column, link_column))
        thread.start()

    def run_main(self, inn_column, link_column):
        try:
            logging.info(f"Начало обработки файла: {self.input_file_path}")
            logging.info(f"Выбранные столбцы: ИНН АУ - {inn_column}, Ссылка должник - {link_column}")
            logging.info(f"Пропущенные данные будут сохранены в файл: {self.missing_file_path}")

            main(self.input_file_path, self.missing_file_path, inn_column, link_column, self.update_progress, self)
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
def main(input_file_path, missing_file_path, inn_column, link_column, update_progress, app_instance):
    try:
        logging.info(f"Начало обработки файла: {input_file_path}")
        logging.info(f"Пропущенные данные будут сохранены в файл: {missing_file_path}")

        driver = create_webdriver()
        if not driver or not is_browser_alive(driver):
            driver = restart_driver(driver)

        if not os.path.exists(input_file_path):
            raise FileNotFoundError(f"Файл {input_file_path} не найден.")

        df = pd.read_excel(input_file_path, dtype=str)
        logging.info(f"Загружено {len(df)} строк с выбранными столбцами.")

        if df.empty:
            raise ValueError(f"Файл {input_file_path} пуст.")

        # Переименование выбранных столбцов
        df = df.rename(columns={inn_column: 'ИНН АУ', link_column: 'Должник ссылка'})
        logging.info(f"Столбцы переименованы: {inn_column} -> 'ИНН АУ', {link_column} -> 'Должник ссылка'")

        missing_data = []
        total_rows = len(df)
        for index, row in df.iterrows():
            if app_instance.stop_processing:  # Проверка флага остановки
                logging.info("Обработка остановлена пользователем.")
                break

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
                logging.info(prepered_data)
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
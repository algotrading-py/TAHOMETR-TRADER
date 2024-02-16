import csv
import os.path
import sqlite3
import time
from datetime import datetime

start_time = time.time()  # Засекаем время начала выполнения программы
print(
    "Эта программа создаст (или обновит ранее созданную) базу данных SQLite с котировками всех акций Мосбиржи. Программа должна находиться рядом с каталогом 'historical_data', содержащим файлы с котировками акций."
)
input("Нажмите Enter для продолжения...")
folder_path = "historical_data"  # папка с файлами исторических данных
# Формируем путь к файлу базы данных в этом каталоге
db_file_path = os.path.join("sql_algo.db")
# Подключение к базе данных
with sqlite3.connect(db_file_path) as conn:
    cur = conn.cursor()
    print("Подключение к базе данных SQLite прошло успешно")
    # Создание таблицы stocks, если ее не существует
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS stocks (
        SECID TEXT PRIMARY KEY,
        BOARDID TEXT,
        SHORTNAME TEXT,
        PREVPRICE REAL,
        LOTSIZE REAL,
        FACEVALUE REAL,
        STATUS TEXT,
        BOARDNAME TEXT,
        DECIMALS INTEGER,
        SECNAME TEXT,
        REMARKS TEXT,
        MARKETCODE TEXT,
        INSTRID TEXT,
        SECTORID TEXT,
        MINSTEP REAL,
        PREVWAPRICE REAL,
        FACEUNIT TEXT,
        PREVDATE TEXT,
        ISSUESIZE REAL,
        ISIN TEXT,
        LATNAME TEXT,
        REGNUMBER TEXT,
        PREVLEGALCLOSEPRICE REAL,
        CURRENCYID TEXT,
        SECTYPE TEXT,
        LISTLEVEL INTEGER,
        SETTLEDATE TEXT)
        """
    )
    # Открытие файла CSV со списком акций и вставка выбранных данных в таблицу
    file_list_tools_path = os.path.join(folder_path, "list_tools.txt")
    with open(file_list_tools_path, "r", encoding="utf-8") as file:
        csv_reader = csv.reader(file, delimiter="\t")
        next(csv_reader)  # Пропускаем первую строку с названием колонок
        for row in csv_reader:
            cur.execute(
                """
            INSERT OR IGNORE INTO stocks (SECID, BOARDID, SHORTNAME, PREVPRICE, LOTSIZE, FACEVALUE, STATUS, BOARDNAME, DECIMALS, SECNAME, REMARKS, MARKETCODE, INSTRID, SECTORID, MINSTEP, PREVWAPRICE, FACEUNIT, PREVDATE, ISSUESIZE, ISIN, LATNAME, REGNUMBER, PREVLEGALCLOSEPRICE, CURRENCYID, SECTYPE, LISTLEVEL, SETTLEDATE)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                row,
            )
        print("Таблица stocks с информацией по акциям создана/обновлена")

    # -- ! создаем таблицы для котировок для всех SECID (акций)
    # Получение всех уникальных значений поля SECID из таблицы stocks
    cur.execute("SELECT SECID FROM stocks")
    rezult_SECIDs = cur.fetchall()

    # Динамическое создание таблиц
    print("Работаем с таблицами котировок...")
    for secid in rezult_SECIDs:
        table_name = secid[0].replace(
            "-", "_"
        )  # Название таблицы равно значению SECID + замена дефисов
        table_name_period = [
            table_name + "_D",
            table_name + "_60",
            table_name + "_10",
            table_name + "_1",
        ]
        for table_name in table_name_period:
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                start_time INTEGER PRIMARY KEY,
                                end_time INTEGER,
                                open_price REAL,
                                high_price REAL,
                                low_price REAL,
                                close_price REAL,
                                volume REAL,
                                trades REAL
                            )"""
            )
            # -- ! в цикле создания таблиц для котировок наполняем их данными
            file_name = f"{table_name.replace('_', '-')}.txt"  # Создаем название файла с данными
            file_path = os.path.join(folder_path, file_name)
            # Проверяем существует ли файл в каталоге historical_data
            if not os.path.exists(file_path):
                print(f"Файл {file_path} не найден в каталоге historical_data")
            else:
                print(
                    f"Файл {file_path} в каталоге historical_data найден, обновляем таблицу {table_name}..."
                )
                with open(file_path, "r", encoding="utf-8") as file:
                    reader = csv.reader(file, delimiter="\t")
                    next(reader)  # Пропускаем первую строку с заголовками
                    # Вставляем данные в таблицу
                    for row in reader:
                        # Преобразование в строке даты и времени open и close в Unix Time
                        unixtime_row = (
                            int(
                                time.mktime(
                                    datetime.strptime(
                                        row[0], "%Y-%m-%d %H:%M:%S"
                                    ).timetuple()
                                )
                            ),
                            int(
                                time.mktime(
                                    datetime.strptime(
                                        row[1], "%Y-%m-%d %H:%M:%S"
                                    ).timetuple()
                                )
                            ),
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            row[6],
                            row[7],
                        )

                        # Вставка данных в таблицу
                        cur.execute(
                            f"INSERT OR REPLACE INTO {table_name} VALUES (?,?,?,?,?,?,?,?)",
                            unixtime_row,
                        )

    # Сохранение изменений в базе данных
    conn.commit()
end_time = time.time()  # Засекаем время окончания выполнения программы
execution_time = end_time - start_time  # Вычисляем время выполнения программы
hours = int(execution_time // 3600)  # Получаем часы
minutes = int((execution_time % 3600) // 60)  # Получаем минуты
seconds = int(execution_time % 60)  # Получаем секунды
print(
    f"Программа выполнена за {hours} часов {minutes} минут {seconds} секунд"
)  # Выводим время выполнения программы в терминал
input("Нажмите Enter чтобы закрыть программу")

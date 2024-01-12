"""
Программа "ТАХОМЕТР ТРЕЙДЕРА" v1.0
Автор: Сергей Тункин
Дата: 11 января 2024 г.
Описание программы:
Эта программа предназначенадля 'добычи' исторических рыночных данных акций Мосбиржи. 
Программа работает с использованием официальной библиотеки биржи 'moexalgo' для AlgoPack API.
"""
import csv
import os.path
import re
import sys
import time as t_time
import webbrowser
from datetime import date, datetime, time, timedelta

import pandas as pd
from tqdm import tqdm

import moexalgo as moex
from moexalgo import Market, Ticker


# ------------ФУНКЦИИ------------------------
def console_title(title):
    """Функция отображения названия окна консоли"""
    if os.name == "nt":
        # Для Windows используем команду 'title'
        os.system("title " + title)
    else:
        # Для других операционных систем используем команду 'echo'
        os.system("echo -n -e '\033]0;" + title + "\a' > /dev/tty")


def log_plus(text):
    """Функция для записи в логфайл и вывода в консоль"""
    with open(filelog_path, "a", encoding="utf-8") as logfile:
        # Получаем текущее время
        current_time = datetime.now().replace(microsecond=0)
        # Форматируем строку для записи в лог
        log_entry = f"{current_time}: {text}\n"
        # # Записываем строку в файл
        logfile.write(log_entry)
        print(log_entry, end="")


def secid_candles():
    """Функция для скачивания исторических данных по акциям"""
    # ===Главный цикл последовательного сбора данных по каждой акции из списка===
    for SECID, FIRST_DATE in tuple_list:
        svech = 0
        # date = datetime.strptime(FIRST_DATE, "%Y-%m-%d %H:%M:%S").replace(
        #     hour=0, minute=0, second=0
        # )
        # дата конца диапазона выдачи данных - текущее время, в каждой акции обновляем чтобы максимально свежее данные были, особенно минутки
        till_date = datetime.now().replace(microsecond=0)
        log_plus(f"Занимаемся акцией - '{SECID}'")
        # проверяем есть ли файл с котировками для данной акции, если нет - создаем
        filestocks_path = os.path.join(folder_path, f"{SECID}-{period}.txt")
        if os.path.exists(filestocks_path):
            log_plus(
                f"Файл '{filestocks_path}' для хранениния котировок акций '{SECID}' с таимфреймом {period} уже существует"
            )
            # читаем файл с котировками, смотрим не пустой ли, если нет - вытаскиваем последнюю сохраненную дату
            with open(filestocks_path, "r", encoding="utf-8") as file:
                reader = csv.reader(file)
                row_count = sum(1 for row in reader)  # считаем количество строк
                if row_count > 1:
                    # Перемещаем указатель файла в начало
                    file.seek(
                        0
                    )  # перемещаем указатель в начало иначе след строка вернет пустое значение
                    last_row = file.readlines()[-1]  # считываем последнюю строку
                    last_time = last_row.split("\t")[
                        0
                    ]  # берем первый элемент строки - дату
                    date = datetime.strptime(
                        last_time, "%Y-%m-%d %H:%M:%S"
                    )  # преобразуем str в datetime
                    log_plus(
                        f"Последние сохраненные данные от {date}, в файле имеется {row_count} строк"
                    )
                else:
                    log_plus(f"файл '{filestocks_path}' пустой")
        else:
            log_plus(
                f"Файл {filestocks_path} для хранениния котировок акций '{SECID}'  с таимфреймом {period} отсутствует и будет создан после сбора данных"
            )
            # создаем пустый файл для котировок с заголовками в первой строке
            df_load = pd.DataFrame(
                columns=[
                    "begin",
                    "end",
                    "open",
                    "high",
                    "low",
                    "close",
                    "value",
                    "volume",
                ]
            )
            df_load.to_csv(
                filestocks_path, mode="w", sep="\t", index=False, header=True
            )
            # берем за начальную дату FIRST_DATE из tuple_list на основе list_tools_listlevel.txt, т.к. ранее скачанных свечей не было
            # с проверкой не является ли FIRST_DATE пустым - последствие обработки ошибки Мосбиржи, в частности для VEON-RX
            if FIRST_DATE != FIRST_DATE:
                date = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            else:
                date = datetime.strptime(FIRST_DATE, "%Y-%m-%d %H:%M:%S").replace(
                    hour=0, minute=0, second=0
                )

        #  =====!качаем свечи!=====
        log_plus(
            f"Качаем свечи для акции '{SECID}' с таймфреймом {period} начиная с {date}"
        )
        real_limit = limit
        # цикл до момента когда количество скачанных свечек не будет меньше заданного limit
        while limit == real_limit:
            try:
                df_load = Ticker(SECID).candles(
                    date=date, till_date=till_date, period=period, limit=limit
                )
            # обработка ошибки, была на момент написания кода на акции VEON-RX
            except:
                log_plus(
                    f"Ошибка от Мосбиржи при получении данных для акции {SECID} с таймфреймом {period}. Продолжаем сбор данных."
                )
                real_limit = 0
                continue  # Переход к следующей итерации цикла

            # полученный <class 'generator'> преобразуем в DataFrame
            df_load = pd.DataFrame(
                df_load,
                columns=[
                    "begin",
                    "end",
                    "open",
                    "high",
                    "low",
                    "close",
                    "value",
                    "volume",
                ],
            )
            real_limit = df_load.shape[0]  # кол-во полученных строк
            if not df_load.empty:  # не пустой ли DataFrame
                svech = (
                    df_load.shape[0] + svech
                )  # кол-во полученных строк по акции всего
                # при дневках в колонке "begin" нет %H:%M:%S - исправляем
                df_load["begin"] = df_load["begin"] + pd.Timedelta(
                    hours=0, minutes=0, seconds=0
                )
                df_load["begin"] = df_load["begin"].dt.strftime("%Y-%m-%d %H:%M:%S")
                # получаем время "begin" у последней свечки
                date = pd.to_datetime(df_load.iloc[-1]["begin"])
                # добавляем к дате период таймфрейм для следующей свечи в следующей итерации цикла
                date = date + timeframe
            if svech > 1:
                # записываем скачанные данные в файл, добавление к существующему в файле содержимому.
                df_load.to_csv(
                    filestocks_path, mode="a", sep="\t", index=False, header=False
                )
            print(
                f"получили данные до {date}. Продолжаем качать ⌛.....", end="\r"
            )  # отображаем процесс скачивания
        if svech > 1:
            log_plus(
                f"скачано {svech} свечей с таймфреймом {period} для акции '{SECID}'"
            )
            log_plus(
                f"все данные записаны в '{filestocks_path}', размер файла {round(os.path.getsize(filestocks_path)/ (1024 * 1024), 4) } МБ."
            )
        else:
            log_plus(f"Новых данных для акции '{SECID}' не обнаружено")
    return


# ---------------КОНЕЦ ФУНКЦИЙ--------------------------------------

# ---------------Начало программы--------------------------------------
console_title("ТАХОМЕТР ТРЕЙДЕРА v1.0")
print("Познавательно-образовательный блог https://алготрейдинг.рф/ ")
print("================================================")
# ASCII art
print(
    """
████████╗ █████╗ ██╗  ██╗ ██████╗ ███╗   ███╗███████╗████████╗██████╗ 
╚══██╔══╝██╔══██╗██║  ██║██╔═══██╗████╗ ████║██╔════╝╚══██╔══╝██╔══██╗
   ██║   ███████║███████║██║   ██║██╔████╔██║█████╗     ██║   ██████╔╝
   ██║   ██╔══██║██╔══██║██║   ██║██║╚██╔╝██║██╔══╝     ██║   ██╔══██╗
   ██║   ██║  ██║██║  ██║╚██████╔╝██║ ╚═╝ ██║███████╗   ██║   ██║  ██║
   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝"""
)
print()
print("================================================")
print('Программа "ТАХОМЕТР ТРЕЙДЕРА" v1.0')
print("================================================")
# запоминаем время для анализа продолжительности работы программы
start = datetime.now()
folder_path = "historical_data"  # папка для хранения данных
filelog_path = os.path.join(folder_path, "logfile.log")  # путь к логфайлу
print(
    "Привет! Это программа для 'добычи' исторических рыночных данных акций Мосбиржи. \nПрограмма работает с использованием официальной библиотеки биржи 'moexalgo' для AlgoPack API.\nЧтобы ПОДДЕРЖАТЬ автора этой программы не забудьте подписаться на его Телеграм-канал \n\033[30;47m'АЛГОТРЕЙДИНГ на PYTHON'\033[0m - https://t.me/algotrading_step_to_step/."
)
# проверяем наличие папки, если ее нет - создаем
if not os.path.exists(folder_path):
    while True:
        answer = input("Открыть ссылку на канал? Введите 'y' или 'n': ")
        if answer.lower() == "д" or answer.lower() == "y":
            print(
                "Вы выбрали 'да' - отлично! В браузере открыта ссылка на канал. Спасибо за подписку!"
            )
            webbrowser.open_new("https://t.me/algotrading_step_to_step/")
            break
        elif answer.lower() == "н" or answer.lower() == "n":
            print(
                "Вы выбрали 'нет', очень жаль. \nВсе равно скопируйте себе ссылку на телеграм-канал, в нем будет много полезной и интересной для Вас информации."
            )
            break
        else:
            print("Некорректный ввод")
    # создаем каталог для хранения файлов с котировками
    os.makedirs(folder_path)
    with open(filelog_path, "a", encoding="utf-8") as logfile:
        log_plus(
            f"Каталог '{folder_path}' для хранения файлов с котировками успешно создан"
        )
        log_plus("Файл логов 'logfile.log' создан")
    log_plus("Старт программы")
    log_plus(
        "Начинаем сканировать список доступных акций Мосбиржи. Подождите пару минут."
    )

    # настройка входных данных
    # берем заведомо старую дату, чтобы найти самые ранние свечи
    Data_start = datetime.strptime("1992-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    market_tools = Market("stocks").tickers()  # получаем данные по ВСЕМ акциям
    market_tools_df = pd.DataFrame(market_tools)  # помещаем в датафрейм
    market_tools_df.to_csv(
        f"{folder_path}/list_tools.txt", sep="\t", index=False
    )  # записываем полученные данные в файл
    # создаем новую колонку с датой самых ранних данных
    market_tools_df["FIRST_DATE"] = ""
    # изменяем порядок колонок и сокращаем их количество
    market_tools_df = market_tools_df.reindex(
        columns=[
            "SECID",
            "FIRST_DATE",
            "LISTLEVEL",
            "SECNAME",
        ]
    )
    # Цикл для каждого тикера в колонке "SECID" - перебираем все акции
    for SECID in tqdm(market_tools_df["SECID"], colour="green"):
        try:
            # вытаскиваем FIRST_DATE для каждого тикера - дата первой свечи
            d1 = pd.DataFrame(
                Ticker(SECID).candles(
                    date=Data_start,
                    till_date="today",
                    period=1,
                    limit=10,
                )
            ).loc[0, "begin"]
            # добавляем в колонку FIRST_DATE фактическое значение первой даты d1
            current_index = market_tools_df[market_tools_df["SECID"] == SECID].index[0]
            market_tools_df.at[current_index, "FIRST_DATE"] = d1
        except:
            # обработка потенциальной ошибки Мосбиржи, была на момент написания кода на акции VEON-RX
            print(
                f"\nОшибка от Мосбиржи при получении данных для акции {SECID}. Продолжаем работу."
            )
            # в случае ошибки добавляем в колонку FIRST_DATE значение NaT
            current_index = market_tools_df[market_tools_df["SECID"] == SECID].index[0]
            market_tools_df.at[current_index, "FIRST_DATE"] = pd.NaT

    # сортируем таблицу по LISTLEVEL в порядке возрастания
    market_tools_df.sort_values(
        "LISTLEVEL", ascending=True, inplace=True, na_position="first"
    )
    market_tools_df.to_csv(
        f"{folder_path}/list_tools_listlevel.txt", sep="\t", index=False
    )  # записываем таблицу акций с FIRST_DATE в файл
    log_plus(
        "Внимание! в папке 'historical_data' имеются файлы 'list_tools.txt' и 'list_tools_listlevel.txt'. \nФайл 'list_tools.txt' содержит справочную информацию по всем акциям Мосбиржи для ознакомления. Расшифровку названий колонок можно посмотреть здесь https://алготрейдинг.рф/moex/column-name-value/ \n\nВАЖНО!!! Файл 'list_tools_listlevel.txt' содержит список всех акции с указанием уровня листинга и первой датой, которая сейчас фактически доступна для скачивания с Мосбиржи. Добыча данных будет происходить по данным этого файла. Вы можете ничего не менять и скачать вообще все доступные данные - это будет долгий процесс. Вы также можете сейчас удалить строки с ненужными акциями. Вы можете изменить даты начала исторических данных по каждой акции в отдельности. Отредактируйте при необходимости этот файл. В дальнейшем программа будет ориентироваться именно на него. Файл никогда не удаляйте! \nДалее мы приступаем к скачиванию данных. Продолжительность зависит от количества акций и первых дат в файле 'list_tools_listlevel.txt'. Для продолжения работы Убедитесь, что файл 'list_tools_listlevel.txt' в папке 'historical_data' закрыт."
    )
    while True:
        answer = input("Для продолжения введите 'y': ")
        if answer.lower() == "д" or answer.lower() == "y":
            break
        else:
            print("Вы ввели что-то другое. Пожалуйста, попробуйте снова.")
    # спросим еще про таймфреймы
    log_plus(
        "Мы можем скачать исторические данные с различным таймфреймом. Введите '1' - если Вам нужны минутки, '2' - если нужнен таймфрейм в 10 минут, '3' - если нужнен таймфрейм в 60 минут, '4' - если нужнен таймфрейм в 1 день, или введите '0' для выбора всех таймфреймов."
    )
    while True:
        number_period = input("Введите 1, 2, 3, 4 или 0: ")

        if number_period in ["0", "1", "2", "3", "4"]:
            number_period = int(number_period)
            break
        else:
            print("Неправильный ответ.")

    log_plus(f"Спасибо! Вы ввели: {number_period}")

else:
    print("================================================")
    log_plus("Старт программы")
    log_plus(
        "Директория 'historical_data' существует. Это означает, что ранее Вы уже запускали программу. В этом сеансе мы будем докачивать все, что ранее не успели скачать, а также скачаем все вновь появившиеся исторические данные."
    )
    print("================================================")
    # пауза для пользователя, чтобы он понял, что происходит и затем он может нажать Enter
    while True:
        user_input = input("Нажмите Enter для продолжения работы: ")
        if user_input == "":
            break

    # ищем из файла логов какие таймфреймы ранее были определены пользователем
    with open(filelog_path, "r", encoding="utf-8") as file:
        content = file.read()

    pattern = r"Спасибо! Вы ввели: ([0-4])"
    poisk = re.findall(pattern, content)

    if poisk:
        number_period = int(poisk[-1])
    else:
        number_period = 0  # если не нашли в логах - качаем все теймфреймы

# читаем файл list_tools_listlevel.txt и создаем tuple_list с акциями и датами для скачивания
df_list = pd.read_csv(f"{folder_path}/list_tools_listlevel.txt", sep="\t")
tuple_list = tuple(zip(df_list["SECID"], df_list["FIRST_DATE"]))

# ------------Переменные-константы-----------------------
# Количество записей данных, полученных за один раз. Максимум 50000
limit = 50000
periods = ["D", 60, 10, 1]
timeframes = [
    timedelta(hours=24),
    timedelta(hours=1),
    timedelta(minutes=10),
    timedelta(minutes=1),
]
# ------------конец переменных-констант-----------------------

# определяем нужные таймфреймы в зависимости от введенного числа пользователем
if number_period == 1:
    period = 1
    timeframe = timedelta(minutes=1)
    secid_candles()  # качаем исторические данные
elif number_period == 2:
    period = 10
    timeframe = timedelta(minutes=10)
    secid_candles()
elif number_period == 3:
    period = 60
    timeframe = timedelta(hours=1)
    secid_candles()
elif number_period == 4:
    period = "D"
    timeframe = timedelta(hours=24)
    secid_candles()
elif number_period == 0:
    # Цикл последовательного сбора исторических данных, если по выбору пользователя нужны все таймфреймы
    for period, timeframe in zip(periods, timeframes):
        secid_candles()  # качаем исторические данные

# считаем время работы программы
vremia = datetime.now() - start
days = vremia.days
hours, remainder = divmod(vremia.seconds, 3600)
minutes, seconds = divmod(remainder, 60)

formatted_vremia = ""
if days > 0:
    formatted_vremia += f"{days} дней "
if hours > 0:
    formatted_vremia += f"{hours} часов "
if minutes > 0:
    formatted_vremia += f"{minutes} минут "
if seconds > 0:
    formatted_vremia += f"{seconds} секунд"

log_plus(
    f"Все доступные данные добыты! Продолжительность сбора информации составляет {formatted_vremia}. Данные сохранены в файлах и находятся в папке 'historical_data'. При повторных запусках программы информация будет обновляться."
)
print("================================================")
log_plus(
    "О появлении новых версий программы Вы можете узнать в Телеграм-канале 'АЛГОТРЕЙДИНГ на PYTHON' :https://t.me/algotrading_step_to_step/"
)
print(
    "Автор канала делится информацией и ведет блог о том, как с нуля самостоятельно стал изучать Python для алгоритмического трейдинга (создания торгового робота и автоматизированного анализа рынка с целью генерации торговых сигналов)."
)
print("================================================")

while True:
    user_input = input(
        "Спасибо за использование программы! Работа завершена. Нажмите Enter для выхода."
    )
    if user_input == "":
        break

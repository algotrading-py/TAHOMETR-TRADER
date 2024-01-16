import os
import os.path

folder_path = "historical_data"
file_names_tah = ["list_tools_listlevel.txt", "list_tools.txt", "lodfile.txt"]
file_names = os.listdir(folder_path)
for element in file_names_tah:
    if element in file_names:
        file_names.remove(element)

file_names_D = [file_name for file_name in file_names if "-D.txt" in file_name]
file_names_60 = [file_name for file_name in file_names if "-60.txt" in file_name]
file_names_10 = [file_name for file_name in file_names if "-10.txt" in file_name]
file_names_1 = [file_name for file_name in file_names if "-1.txt" in file_name]

stroki_D = stroki_60 = stroki_10 = stroki_1 = size_D = size_60 = size_10 = size_1 = 0
print("Сейчас я подготовлю для Вас статистику по собранным данным.")
for list in file_names_D:
    size_D += (os.path.getsize("historical_data/" + list)) / (1024 * 1024)
    with open("historical_data/" + list) as file:
        stroki_D += sum(1 for line in file)
print(
    f"С таймфреймом -D по всем {len(file_names_D)} акциям сохранено {stroki_D} строк,\nОбъем файлов составил {round(size_D, 1)} Мб."
)
for list in file_names_60:
    size_60 += (os.path.getsize("historical_data/" + list)) / (1024 * 1024)
    with open("historical_data/" + list) as file:
        stroki_60 += sum(1 for line in file)
print(
    f"С таймфреймом -60 по всем {len(file_names_60)} акциям сохранено {stroki_60} строк,\nОбъем файлов составил {round(size_60, 1)} Мб."
)
for list in file_names_10:
    size_10 += (os.path.getsize("historical_data/" + list)) / (1024 * 1024)
    with open("historical_data/" + list) as file:
        stroki_10 += sum(1 for line in file)
print(
    f"С таймфреймом -10 по всем {len(file_names_10)} акциям сохранено {stroki_10} строк,\nОбъем файлов составил {round(size_10, 1)} Мб."
)
for list in file_names_1:
    size_1 += (os.path.getsize("historical_data/" + list)) / (1024 * 1024)
    with open("historical_data/" + list) as file:
        stroki_1 += sum(1 for line in file)
print(
    f"С таймфреймом -1 по всем {len(file_names_1)} акциям сохранено {stroki_1} строк,\nОбъем файлов составил {round(size_1, 1)} Мб."
)

size = size_D + size_60 + size_10 + size_1
stroki = stroki_D + stroki_60 + stroki_10 + stroki_1
failov = len(file_names_D) + len(file_names_60) + len(file_names_10) + len(file_names_1)
print(
    f"Вообще всего в {failov} файлах исторических данных сохранено {stroki} строк(свечек),\nобщий объем сохраненных данных {round(size, 1)} Мб."
)
input("Нажмите Enter для выхода...")

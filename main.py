import functions

if __name__ == '__main__':
    timestamp = functions.get_timestamp()
    functions.setup_logging()

    fileName = 'test_data.csv'

    # чтение данных из файла
    data = functions.read_data_from_csv(fileName)

    # предоработка данных
    functions.preproc_data(data)

    # добавление статистических значений
    functions.add_statistics_to_data(data)

    # установка статуса
    functions.set_status(data)

    # сортировка данных по возрастанию timestamp
    functions.sort_data(data)

    # запись в файлы
    functions.save_data(data, timestamp)

    # пример просмотра докстринга метода
    # print(functions.read_data_from_csv.__doc__)
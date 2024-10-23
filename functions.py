import pandas as pd
from datetime import datetime
import sys
import logging


def setup_logging():
    """Настраивает логирование, создавая новый файл для логов"""
    log_filename = f"proc.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='w'
    )


def read_data_from_csv(filePath):
    """
    Читает данные из CSV файла и возвращает их в виде DataFrame.

    Args:
        filePath: Путь к файлу CSV. Должен быть непустой строкой.

    Returns:
        pd.DataFrame: Данные из CSV файла в формате DataFrame.

    Raises:
        SystemExit: Программа завершится, если возникнет ошибка при чтении файла.
    """
    try:
        df = pd.read_csv(filePath, sep=';', index_col=0)
        logging.info("Данные из файла считаны успешно")
        return df
    except Exception as e:
        logging.error(f"Ошибка при чтении файла {filePath}: {e} ")
        sys.exit(1)


def preproc_data(data):
    """Заполняет пропуски в DataFrame значениями из предыдущих строк."""
    logging.info("Предобработка данных")

    if data.empty:
        logging.warning("DataFrame пустой")
    data.ffill(inplace=True)


def sort_data(data):
    """Сортирует DataFrame по возрастанию 'timestamp'."""
    logging.info("Сортировка данных")

    if data.empty:
        logging.error("DataFrame пустой")
        return
    data.sort_values('timestamp', ascending=True, inplace=True)


def get_status(data):
    if ((data['temperature'] > data['temperature_mean'] + 0.3 * data['utilization_max']) or
            (data['utilization'] > data['utilization_mean'] + 0.3 * data['utilization_max'])):
        return "WARNING"
    else:
        return "OK"


def set_status(data):
    """
    Устанавливает статус для каждой строки в переданном DataFrame.

    Функция применяет функцию `get_status` к каждой строке
    DataFrame `data`, записывая значение столбца 'status'.

    Args:
    data: Входной DataFrame

    Returns:
    None: Функция изменяет переданный DataFrame и не возвращает
          никаких значений.
    """
    logging.info("Добавление данных о статусе")

    if data.empty:
        logging.error("DataFrame пустой. Обработка не может быть выполнена")
        return
    data['status'] = data.apply(get_status, axis=1)


def add_statistics_to_data(data):
    """
    Рассчитывает минимальные, максимальные и средние значения
    для столбцов 'temperature' и 'utilization', а затем добавляет
    эти статистики в DataFrame в виде новых столбцов

    Args:
    data: Входной DataFrame

    Returns:
    DataFrame: Обновленный DataFrame с добавленными статистическими
                показателями.
    """
    logging.info("Добавление статистических показателей")

    if data.empty:
        logging.error("DataFrame пустой. Обработка не может быть выполнена")
        return

    stats_temperature = data['temperature'].agg(['min', 'max', 'mean'])
    stats_utilization = data['utilization'].agg(['min', 'max', 'mean'])

    data['temperature_min'] = stats_temperature['min']
    data['temperature_max'] = stats_temperature['max']
    data['temperature_mean'] = stats_temperature['mean']

    data['utilization_min'] = stats_utilization['min']
    data['utilization_max'] = stats_utilization['max']
    data['utilization_mean'] = stats_utilization['mean']
    return data


def get_timestamp():
    return datetime.now()


def save_data_to_csv(data, timestamp):
    """
    Сохраняет данные в формате CSV.

    Args:
        data (pandas.DataFrame): Входной DataFrame
        timestamp (datetime): Временная метка, используемая для формирования имени файла.

    Returns:
        None: Функция выполняет запись файла и не возвращает значения.
    """
    fmt = '%H_%M_%S_%d%m%Y'
    filename = timestamp.strftime(fmt) + str('.csv')
    try:
        data.to_csv(filename, index=False)
        logging.info(f"Данные успешно записаны в файл {filename}")
    except Exception as e:
        logging.error(f"Ошибка при записи в файл {filename}: {e}")


def save_data_to_json(data, timestamp):
    """
    Сохраняет данные в формате JSON.

    Args:
        data (pandas.DataFrame): Входной DataFrame
        timestamp (datetime): Временная метка, используемая для формирования имени файла.

    Returns:
        None: Функция выполняет запись файла и не возвращает значения.
    """
    fmt = '%H_%M_%S_%d%m%Y'
    filename = timestamp.strftime(fmt) + str('.json')
    try:
        data.to_json(filename, orient='records', lines=True)
        logging.info(f"Данные успешно записаны в файл {filename}")
    except Exception as e:
        logging.error(f"Ошибка при записи в файл {filename}: {e}")


def save_data(data, timestamp):
    """Сохраняет данные в формате CSV и JSON"""
    save_data_to_csv(data, timestamp)
    save_data_to_json(data, timestamp)

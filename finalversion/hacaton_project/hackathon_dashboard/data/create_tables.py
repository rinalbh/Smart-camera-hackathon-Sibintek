import requests
import json
import base64

# Конфигурация
CLICKHOUSE_URL = "https://iqydclkqtr.us-east1.gcp.clickhouse.cloud:8443/"
USERNAME = "default"
PASSWORD = "zrh0w4W_gzVFO"

def execute_query(query):
    """Выполнение SQL запроса через HTTP API"""
    try:
        # Базовая аутентификация
        credentials = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
        
        headers = {
            'Authorization': f'Basic {credentials}',
            'Content-Type': 'text/plain'
        }
        
        response = requests.post(
            CLICKHOUSE_URL,
            data=query,
            headers=headers,
            verify=True
        )
        
        if response.status_code == 200:
            return True, response.text
        else:
            return False, f"HTTP {response.status_code}: {response.text}"
            
    except Exception as e:
        return False, str(e)

def drop_tables():
    """Удаление существующих таблиц"""
    print("🗑️ Удаление существующих таблиц...")
    
    # Удаляем таблицу поездов если существует
    drop_train_query = 'DROP TABLE IF EXISTS train_events'
    success, result = execute_query(drop_train_query)
    if success:
        print("✅ Таблица train_events удалена")
    else:
        print(f"❌ Ошибка удаления train_events: {result}")
        return False
    
    # Удаляем таблицу людей если существует
    drop_people_query = 'DROP TABLE IF EXISTS people_events'
    success, result = execute_query(drop_people_query)
    if success:
        print("✅ Таблица people_events удалена")
    else:
        print(f"❌ Ошибка удаления people_events: {result}")
        return False
    
    return True

def create_tables():
    """Создание таблиц"""
    print("🗃️ Создание таблиц...")
    
    # Таблица для поездов
    train_table_query = '''
    CREATE TABLE train_events (
        train_id UInt32,
        filename String,
        video_link String,
        video_id String,
        arrival_sec Nullable(Float64),
        arrival_dt Nullable(String),
        stop_start_sec Nullable(Float64),
        stop_start_dt Nullable(String),
        stop_end_sec Nullable(Float64),
        stop_end_dt Nullable(String),
        departure_sec Nullable(Float64),
        departure_dt Nullable(String),
        stopped UInt8,
        train_number Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY (train_id, video_id)
    '''
    
    success, result = execute_query(train_table_query)
    if success:
        print("✅ Таблица train_events создана")
    else:
        print(f"❌ Ошибка создания train_events: {result}")
        return False
    
    # Таблица для людей
    people_table_query = '''
    CREATE TABLE people_events (
        person_id UInt32,
        filename String,
        video_link String,
        video_id String,
        start_sec Float64,
        end_sec Float64,
        start_dt String,
        end_dt String,
        status String,
        zone Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY (person_id, video_id, start_sec)
    '''
    
    success, result = execute_query(people_table_query)
    if success:
        print("✅ Таблица people_events создана")
    else:
        print(f"❌ Ошибка создания people_events: {result}")
        return False
    
    return True

def convert_value(value):
    """Конвертация значений в формат ClickHouse"""
    if value is None:
        return 'NULL'
    elif isinstance(value, str):
        # Экранируем кавычки
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
    elif isinstance(value, bool):
        return '1' if value else '0'
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return 'NULL'

def load_train_data():
    """Загрузка данных поездов"""
    print("🚆 Загрузка данных поездов...")
    
    try:
        with open('train_events_video_01 (1).json', 'r', encoding='utf-8') as f:
            train_data = json.load(f)
        
        # Формируем один большой INSERT запрос
        values = []
        for item in train_data:
            train_id = item.get('train_id')
            filename = item.get('filename')
            video_link = item.get('video_link')
            video_id = item.get('video_id')
            arrival_sec = item.get('arrival_sec')
            arrival_dt = item.get('arrival_dt')
            stop_start_sec = item.get('stop_start_sec')
            stop_start_dt = item.get('stop_start_dt')
            stop_end_sec = item.get('stop_end_sec')
            stop_end_dt = item.get('stop_end_dt')
            departure_sec = item.get('departure_sec')
            departure_dt = item.get('departure_dt')
            stopped = item.get('stopped', False)
            train_number = item.get('номер')  # Оригинальное поле из JSON
            
            value_str = (f"({convert_value(train_id)}, "
                        f"{convert_value(filename)}, "
                        f"{convert_value(video_link)}, "
                        f"{convert_value(video_id)}, "
                        f"{convert_value(arrival_sec)}, "
                        f"{convert_value(arrival_dt)}, "
                        f"{convert_value(stop_start_sec)}, "
                        f"{convert_value(stop_start_dt)}, "
                        f"{convert_value(stop_end_sec)}, "
                        f"{convert_value(stop_end_dt)}, "
                        f"{convert_value(departure_sec)}, "
                        f"{convert_value(departure_dt)}, "
                        f"{convert_value(stopped)}, "
                        f"{convert_value(train_number)})")
            
            values.append(value_str)
        
        if values:
            insert_query = f'INSERT INTO train_events VALUES {", ".join(values)}'
            print(f"📝 Выполняем INSERT запрос для {len(values)} записей...")
            success, result = execute_query(insert_query)
            if success:
                print(f"✅ Загружено {len(train_data)} записей поездов")
            else:
                print(f"❌ Ошибка вставки поездов: {result}")
                return False
        else:
            print("⚠️ Нет данных поездов для загрузки")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных поездов: {e}")
        return False

def load_people_data():
    """Загрузка данных людей"""
    print("👥 Загрузка данных людей...")
    
    try:
        with open('people_events_video_01 (1).json', 'r', encoding='utf-8') as f:
            people_data = json.load(f)
        
        # Формируем один большой INSERT запрос
        values = []
        for item in people_data:
            person_id = item.get('person_id')
            filename = item.get('filename')
            video_link = item.get('video_link')
            video_id = item.get('video_id')
            start_sec = item.get('start_sec', 0.0)
            end_sec = item.get('end_sec', 0.0)
            start_dt = item.get('start_dt')
            end_dt = item.get('end_dt')
            status = item.get('status')
            zone = item.get('zone')
            
            value_str = (f"({convert_value(person_id)}, "
                        f"{convert_value(filename)}, "
                        f"{convert_value(video_link)}, "
                        f"{convert_value(video_id)}, "
                        f"{convert_value(start_sec)}, "
                        f"{convert_value(end_sec)}, "
                        f"{convert_value(start_dt)}, "
                        f"{convert_value(end_dt)}, "
                        f"{convert_value(status)}, "
                        f"{convert_value(zone)})")
            
            values.append(value_str)
        
        if values:
            insert_query = f'INSERT INTO people_events VALUES {", ".join(values)}'
            print(f"📝 Выполняем INSERT запрос для {len(values)} записей...")
            success, result = execute_query(insert_query)
            if success:
                print(f"✅ Загружено {len(people_data)} записей людей")
            else:
                print(f"❌ Ошибка вставки людей: {result}")
                return False
        else:
            print("⚠️ Нет данных людей для загрузки")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных людей: {e}")
        return False

def check_data():
    """Проверка загруженных данных"""
    print("🔍 Проверка данных...")
    
    # Проверка поездов
    success, result = execute_query("SELECT COUNT(*) FROM train_events")
    if success:
        print(f"✅ Записей в train_events: {result.strip()}")
    else:
        print(f"❌ Ошибка проверки train_events: {result}")
    
    # Проверка людей
    success, result = execute_query("SELECT COUNT(*) FROM people_events")
    if success:
        print(f"✅ Записей в people_events: {result.strip()}")
    else:
        print(f"❌ Ошибка проверки people_events: {result}")
    
    # Пример данных поездов
    success, result = execute_query("SELECT train_id, video_id, train_number, stopped FROM train_events LIMIT 3")
    if success:
        print(f"📋 Пример поездов:\n{result}")
    
    # Пример данных людей
    success, result = execute_query("SELECT person_id, video_id, status, zone FROM people_events LIMIT 3")
    if success:
        print(f"📋 Пример людей:\n{result}")

def test_connection():
    """Тест подключения"""
    print("🔌 Тестирование подключения...")
    success, result = execute_query("SELECT 1")
    if success:
        print("✅ Подключение к ClickHouse успешно")
        return True
    else:
        print(f"❌ Ошибка подключения: {result}")
        return False

def main():
    print("🚀 Начало загрузки данных в ClickHouse через HTTP API...")
    
    # Тест подключения
    if not test_connection():
        return
    
    # Удаление существующих таблиц
    if not drop_tables():
        return
    
    # Создание таблиц
    if not create_tables():
        return
    
    # Загрузка данных
    if not load_train_data():
        return
        
    if not load_people_data():
        return
    
    # Проверка данных
    check_data()
    
    print("🎉 Все данные успешно загружены!")

if __name__ == "__main__":
    main()
import requests
import random
from datetime import datetime, timedelta

# Конфигурация подключения
CLICKHOUSE_CONFIG = {
    'host': 'iqydclkqtr.us-east1.gcp.clickhouse.cloud',
    'port': 8443,
    'username': 'default',
    'password': 'zrh0w4W_gzVFO',
    'secure': True
}

def get_clickhouse_url():
    """Возвращает URL для подключения к ClickHouse"""
    return f"https://{CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}"

def execute_query(query, params=None):
    """Выполняет SQL запрос к ClickHouse Cloud"""
    try:
        response = requests.post(
            get_clickhouse_url(),
            auth=(CLICKHOUSE_CONFIG['username'], CLICKHOUSE_CONFIG['password']),
            data=query,
            params=params,
            headers={'Content-Type': 'text/plain'},
            verify=True,
            timeout=30
        )
        
        if response.status_code == 200:
            return {'success': True, 'data': response.text}
        else:
            error_msg = f"Ошибка запроса: {response.status_code} - {response.text}"
            print(f"❌ {error_msg}")
            return {'success': False, 'error': error_msg}
            
    except Exception as e:
        error_msg = f"Ошибка выполнения запроса: {e}"
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg}

def init_database():
    """Создает таблицы в ClickHouse Cloud"""
    try:
        # Таблица Train - информация о поездах
        create_train_query = '''
        CREATE TABLE IF NOT EXISTS Train (
            id UUID DEFAULT generateUUIDv4(),
            camera_id String,
            filename String,
            filepath_s3 String,
            start_time DateTime,
            end_time DateTime,
            train_number String,
            status String,
            upload_time DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY upload_time
        '''
        
        result = execute_query(create_train_query)
        if not result['success']:
            print(f"❌ Ошибка создания таблицы Train: {result['error']}")
            return False
        
        # Таблица People - информация о людях
        create_people_query = '''
        CREATE TABLE IF NOT EXISTS People (
            id UUID DEFAULT generateUUIDv4(),
            camera_id String,
            filename String,
            filepath_s3 String,
            start_time DateTime,
            end_time DateTime,
            train_number String,
            status String,
            activity_status String,
            zone String,
            upload_time DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY upload_time
        '''
        
        result = execute_query(create_people_query)
        if not result['success']:
            print(f"❌ Ошибка создания таблицы People: {result['error']}")
            return False
        
        print("✅ Таблицы Train и People созданы/проверены")
        return True
            
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")
        return False

def save_train_metadata(camera_id, filename, filepath_s3, start_time, end_time, train_number, status):
    """Сохраняет метаданные в таблицу Train"""
    try:
        # Экранируем специальные символы
        camera_id_escaped = camera_id.replace("'", "")
        filename_escaped = filename.replace("'", "")
        filepath_s3_escaped = filepath_s3.replace("'", "")
        train_number_escaped = train_number.replace("'", "")
        status_escaped = status.replace("'", "")
        
        # Форматируем время для ClickHouse
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
        
        query = f"""
        INSERT INTO Train (camera_id, filename, filepath_s3, start_time, end_time, train_number, status) 
        VALUES ('{camera_id_escaped}', '{filename_escaped}', '{filepath_s3_escaped}', '{start_time_str}', '{end_time_str}', '{train_number_escaped}', '{status_escaped}')
        """
        
        result = execute_query(query)
        if result['success']:
            print(f"✅ Данные сохранены в таблицу Train: {filename}")
            return True
        else:
            print(f"❌ Ошибка сохранения в Train: {result['error']}")
            return False
        
    except Exception as e:
        print(f"❌ Исключение при сохранении в Train: {e}")
        return False

def save_people_metadata(camera_id, filename, filepath_s3, start_time, end_time, train_number, status, activity_status, zone):
    """Сохраняет метаданные в таблицу People"""
    try:
        # Экранируем специальные символы
        camera_id_escaped = camera_id.replace("'", "")
        filename_escaped = filename.replace("'", "")
        filepath_s3_escaped = filepath_s3.replace("'", "")
        train_number_escaped = train_number.replace("'", "")
        status_escaped = status.replace("'", "")
        activity_status_escaped = activity_status.replace("'", "")
        zone_escaped = zone.replace("'", "")
        
        # Форматируем время для ClickHouse
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
        
        query = f"""
        INSERT INTO People (camera_id, filename, filepath_s3, start_time, end_time, train_number, status, activity_status, zone) 
        VALUES ('{camera_id_escaped}', '{filename_escaped}', '{filepath_s3_escaped}', '{start_time_str}', '{end_time_str}', '{train_number_escaped}', '{status_escaped}', '{activity_status_escaped}', '{zone_escaped}')
        """
        
        result = execute_query(query)
        if result['success']:
            print(f"✅ Данные сохранены в таблицу People: {filename}")
            return True
        else:
            print(f"❌ Ошибка сохранения в People: {result['error']}")
            return False
        
    except Exception as e:
        print(f"❌ Исключение при сохранении в People: {e}")
        return False

def get_unique_filenames():
    """Получает уникальные имена файлов из обеих таблиц"""
    try:
        # Используем UNION ALL и DISTINCT для совместимости с ClickHouse
        query = '''
        SELECT DISTINCT filename FROM (
            SELECT filename FROM Train
            UNION ALL
            SELECT filename FROM People
        )
        ORDER BY filename
        '''
        
        result = execute_query(query)
        if result['success']:
            lines = result['data'].strip().split('\n')
            return [line.strip() for line in lines if line.strip()]
        
        # Если запрос с UNION не работает, пробуем отдельные запросы
        print("⚠️ UNION запрос не сработал, используем отдельные запросы")
        train_result = execute_query('SELECT DISTINCT filename FROM Train ORDER BY filename')
        people_result = execute_query('SELECT DISTINCT filename FROM People ORDER BY filename')
        
        filenames = set()
        
        if train_result['success']:
            lines = train_result['data'].strip().split('\n')
            filenames.update([line.strip() for line in lines if line.strip()])
        
        if people_result['success']:
            lines = people_result['data'].strip().split('\n')
            filenames.update([line.strip() for line in lines if line.strip()])
        
        return sorted(list(filenames))
        
    except Exception as e:
        print(f"❌ Ошибка получения уникальных файлов: {e}")
        return []

def get_videos_by_filename(filename):
    """Получает все видео по имени файла"""
    try:
        filename_escaped = filename.replace("'", "")
        
        # Сначала пробуем получить из Train
        query = f"""
        SELECT camera_id, filename, filepath_s3, start_time, end_time, train_number, status
        FROM Train 
        WHERE filename = '{filename_escaped}'
        ORDER BY start_time
        """
        
        result = execute_query(query)
        videos = []
        if result['success']:
            lines = result['data'].strip().split('\n')
            for line in lines:
                if line and '\t' in line:
                    parts = line.split('\t')
                    if len(parts) >= 7:
                        camera_id, filename, filepath_s3, start_time_str, end_time_str, train_number, status = parts
                        
                        # Парсим время
                        try:
                            start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
                            end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            start_time = start_time_str
                            end_time = end_time_str
                            
                        videos.append({
                            'camera_id': camera_id,
                            'filename': filename,
                            'filepath_s3': filepath_s3,
                            'start_time': start_time,
                            'end_time': end_time,
                            'train_number': train_number,
                            'status': status
                        })
        
        # Если в Train нет данных, пробуем из People
        if not videos:
            query = f"""
            SELECT camera_id, filename, filepath_s3, start_time, end_time, train_number, status
            FROM People 
            WHERE filename = '{filename_escaped}'
            ORDER BY start_time
            """
            
            result = execute_query(query)
            if result['success']:
                lines = result['data'].strip().split('\n')
                for line in lines:
                    if line and '\t' in line:
                        parts = line.split('\t')
                        if len(parts) >= 7:
                            camera_id, filename, filepath_s3, start_time_str, end_time_str, train_number, status = parts
                            
                            try:
                                start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
                                end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
                            except ValueError:
                                start_time = start_time_str
                                end_time = end_time_str
                                
                            videos.append({
                                'camera_id': camera_id,
                                'filename': filename,
                                'filepath_s3': filepath_s3,
                                'start_time': start_time,
                                'end_time': end_time,
                                'train_number': train_number,
                                'status': status
                            })
        
        return videos
        
    except Exception as e:
        print(f"❌ Ошибка получения видео по файлу: {e}")
        return []

def get_people_data(filename, filepath_s3, start_date=None, end_date=None):
    """Получает данные People с фильтрами"""
    try:
        filename_escaped = filename.replace("'", "")
        filepath_s3_escaped = filepath_s3.replace("'", "")
        
        where_conditions = [
            f"filename = '{filename_escaped}'",
            f"filepath_s3 = '{filepath_s3_escaped}'"
        ]
        
        if start_date:
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            where_conditions.append(f"start_time >= '{start_date_str}'")
        
        if end_date:
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
            where_conditions.append(f"end_time <= '{end_date_str}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT camera_id, filename, filepath_s3, start_time, end_time, train_number, status, activity_status, zone
        FROM People 
        WHERE {where_clause}
        ORDER BY start_time
        """
        
        result = execute_query(query)
        people_data = []
        if result['success']:
            lines = result['data'].strip().split('\n')
            for line in lines:
                if line and '\t' in line:
                    parts = line.split('\t')
                    if len(parts) >= 9:
                        camera_id, filename, filepath_s3, start_time_str, end_time_str, train_number, status, activity_status, zone = parts
                        
                        try:
                            start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
                            end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            start_time = start_time_str
                            end_time = end_time_str
                            
                        people_data.append({
                            'camera_id': camera_id,
                            'filename': filename,
                            'filepath_s3': filepath_s3,
                            'start_time': start_time,
                            'end_time': end_time,
                            'train_number': train_number,
                            'status': status,
                            'activity_status': activity_status,
                            'zone': zone
                        })
        return people_data
        
    except Exception as e:
        print(f"❌ Ошибка получения данных People: {e}")
        return []

def clear_database():
    """Полностью очищает все таблицы в базе данных"""
    try:
        print("🧹 Очищаем таблицу Train...")
        result1 = execute_query('TRUNCATE TABLE Train')
        
        print("🧹 Очищаем таблицу People...")
        result2 = execute_query('TRUNCATE TABLE People')
        
        if result1['success'] and result2['success']:
            print("✅ База данных полностью очищена!")
            return True
        else:
            error_msg = ""
            if not result1['success']:
                error_msg += f"Ошибка очистки Train: {result1.get('error')}. "
            if not result2['success']:
                error_msg += f"Ошибка очистки People: {result2.get('error')}"
            print(f"❌ {error_msg}")
            return False
    except Exception as e:
        print(f"❌ Ошибка очистки БД: {e}")
        return False
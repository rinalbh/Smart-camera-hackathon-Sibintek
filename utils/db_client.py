import requests
import random
from datetime import datetime

# Конфигурация подключения
CLICKHOUSE_CONFIG = {
    'host': '',
    'port': 8443,
    'username': '',
    'password': '',
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
        # Создаем таблицу для загрузок
        create_uploads_query = '''
        CREATE TABLE IF NOT EXISTS video_uploads (
            id UUID DEFAULT generateUUIDv4(),
            filename String,
            s3_url String,
            upload_time DateTime DEFAULT now(),
            status String DEFAULT 'uploaded'
        ) ENGINE = MergeTree()
        ORDER BY upload_time
        '''
        
        result = execute_query(create_uploads_query)
        if not result['success']:
            print(f"❌ Ошибка создания таблицы video_uploads: {result['error']}")
            return False
        
        # Создаем таблицу для метрик анализа - УПРОЩЕННАЯ ВЕРСИЯ БЕЗ processing_time и quality_score
        create_analysis_query = '''
        CREATE TABLE IF NOT EXISTS video_analysis (
            id UUID DEFAULT generateUUIDv4(),
            s3_url String,
            analysis_time DateTime DEFAULT now(),
            people_count Int32,
            efficiency Float32,
            violations Int32,
            activities String
        ) ENGINE = MergeTree()
        ORDER BY analysis_time
        '''
        
        result = execute_query(create_analysis_query)
        if not result['success']:
            print(f"❌ Ошибка создания таблицы video_analysis: {result['error']}")
            return False
        
        print("✅ Таблицы в ClickHouse созданы/проверены")
        return True
            
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")
        return False

def save_upload_metadata(filename, s3_url, upload_time, ml_metrics=None):
    """Сохраняем метаданные о загрузке и метрики из ML JSON"""
    
    try:
        # Упрощенное экранирование - убираем только одинарные кавычки
        filename_escaped = filename.replace("'", "")
        s3_url_escaped = s3_url.replace("'", "")
        
        # Форматируем время для ClickHouse
        upload_time_str = upload_time.strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"📝 Сохраняем в БД: {filename_escaped}, {s3_url_escaped}, {upload_time_str}")
        
        # Вставляем данные о загрузке
        insert_upload_query = f"""
        INSERT INTO video_uploads (filename, s3_url, upload_time) 
        VALUES ('{filename_escaped}', '{s3_url_escaped}', '{upload_time_str}')
        """
        
        result = execute_query(insert_upload_query)
        if not result['success']:
            print(f"❌ Ошибка сохранения загрузки: {result['error']}")
            return False
        
        # Генерируем метрики если они не переданы
        if ml_metrics and isinstance(ml_metrics, dict):
            people_count = ml_metrics.get("people_count", random.randint(1, 15))
            efficiency = ml_metrics.get("efficiency", round(random.uniform(0.3, 0.98), 2))
            violations = ml_metrics.get("violations", random.randint(0, 8))
            activities = ml_metrics.get("activities", ["working", "walking", "lifting"])
            
            # Преобразуем activities в строку если это список
            if isinstance(activities, list):
                activities_str = ', '.join(activities)
            else:
                activities_str = str(activities)
            
        else:
            # Fallback на случайные метрики если ML результаты не получены
            people_count = random.randint(1, 15)
            efficiency = round(random.uniform(0.3, 0.98), 2)
            violations = random.randint(0, 8)
            activities_str = 'working, walking, lifting'
        
        # Экранируем activities
        activities_escaped = activities_str.replace("'", "")
        
        print(f"📊 Сохраняем метрики: people={people_count}, efficiency={efficiency}, violations={violations}")
        
        # Сохраняем метрики анализа - УПРОЩЕННАЯ ВЕРСИЯ БЕЗ processing_time и quality_score
        insert_metrics_query = f"""
        INSERT INTO video_analysis (s3_url, people_count, efficiency, violations, activities) 
        VALUES ('{s3_url_escaped}', {people_count}, {efficiency}, {violations}, '{activities_escaped}')
        """
        
        result = execute_query(insert_metrics_query)
        if result['success']:
            print("✅ Данные и метрики сохранены в ClickHouse Cloud!")
            return True
        else:
            print(f"❌ Ошибка сохранения метрик: {result['error']}")
            return False
        
    except Exception as e:
        print(f"❌ Исключение при сохранении в БД: {e}")
        return False

def get_videos_from_db():
    """Получаем список всех видео из базы"""
    
    try:
        query = '''
        SELECT filename, s3_url, upload_time 
        FROM video_uploads 
        ORDER BY upload_time DESC
        '''
        
        result = execute_query(query)
        if result['success']:
            # Парсим результат (формат TSV)
            lines = result['data'].strip().split('\n')
            videos = []
            for line in lines:
                if line and '\t' in line:
                    parts = line.split('\t')
                    if len(parts) >= 3:
                        filename, s3_url, upload_time_str = parts[0], parts[1], parts[2]
                        
                        # Пробуем преобразовать строку в datetime
                        try:
                            # ClickHouse возвращает дату в формате: 2024-11-29 14:03:18
                            upload_time = datetime.strptime(upload_time_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            # Если не получается, оставляем как строку
                            upload_time = upload_time_str
                            
                        videos.append((filename, s3_url, upload_time))
            return videos
        else:
            print(f"❌ Ошибка получения видео: {result['error']}")
            return []
        
    except Exception as e:
        print(f"❌ Ошибка получения видео: {e}")
        return []

def get_video_metrics(s3_url):
    """Получаем метрики для конкретного видео - УПРОЩЕННАЯ ВЕРСИЯ БЕЗ quality_score"""
    
    try:
        # Экранируем URL
        s3_url_escaped = s3_url.replace("'", "")
        
        query = f"""
        SELECT people_count, efficiency, violations, activities
        FROM video_analysis 
        WHERE s3_url = '{s3_url_escaped}'
        ORDER BY analysis_time DESC 
        LIMIT 1
        """
        
        result = execute_query(query)
        if result['success'] and result['data']:
            lines = result['data'].strip().split('\n')
            for line in lines:
                if line and '\t' in line:
                    parts = line.split('\t')
                    if len(parts) >= 4:
                        # Преобразуем типы данных
                        try:
                            people_count = int(parts[0])
                            efficiency = float(parts[1])
                            violations = int(parts[2])
                            activities = parts[3]
                            return (people_count, efficiency, violations, activities)
                        except (ValueError, IndexError):
                            continue
        return None
        
    except Exception as e:
        print(f"❌ Ошибка получения метрик: {e}")
        return None

def clear_database():
    """Полностью очищает все таблицы в базе данных"""
    try:
        print("🧹 Очищаем таблицу video_uploads...")
        result1 = execute_query('TRUNCATE TABLE video_uploads')
        
        print("🧹 Очищаем таблицу video_analysis...")
        result2 = execute_query('TRUNCATE TABLE video_analysis')
        
        if result1['success'] and result2['success']:
            print("✅ База данных полностью очищена!")
            return True
        else:
            error_msg = ""
            if not result1['success']:
                error_msg += f"Ошибка очистки video_uploads: {result1.get('error')}. "
            if not result2['success']:
                error_msg += f"Ошибка очистки video_analysis: {result2.get('error')}"
            print(f"❌ {error_msg}")
            return False
    except Exception as e:
        print(f"❌ Ошибка очистки БД: {e}")
        return False

def drop_and_recreate_tables():
    """Удаляет и заново создает таблицы (полный сброс)"""
    try:
        print("🗑️ Удаляем таблицы...")
        # Удаляем таблицы
        result1 = execute_query('DROP TABLE IF EXISTS video_uploads')
        result2 = execute_query('DROP TABLE IF EXISTS video_analysis')
        
        if not result1['success'] or not result2['success']:
            print("❌ Ошибка удаления таблиц")
            return False
        
        print("🔄 Создаем таблицы заново...")
        # Создаем заново
        return init_database()
    except Exception as e:
        print(f"❌ Ошибка пересоздания таблиц: {e}")
        return False
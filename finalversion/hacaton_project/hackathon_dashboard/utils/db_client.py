import streamlit as st
from clickhouse_driver import Client
import pandas as pd
import requests
import base64

def get_clickhouse_client():
    """Создание клиента ClickHouse через HTTP API"""
    try:
        client = Client(
            host='iqydclkqtr.us-east1.gcp.clickhouse.cloud',
            port=9440,  # Изменен порт на 9440 для secure соединения
            user='default',
            password='zrh0w4W_gzVFO',
            database='default',
            secure=True,  # Включено безопасное соединение
            verify=False  # Отключена проверка SSL для избежания ошибок
        )
        
        # Проверяем подключение
        client.execute('SELECT 1')
        return client
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

def execute_http_query(query):
    """Выполнение запроса через HTTP API как запасной вариант"""
    try:
        CLICKHOUSE_URL = "https://iqydclkqtr.us-east1.gcp.clickhouse.cloud:8443/"
        USERNAME = "default"
        PASSWORD = "zrh0w4W_gzVFO"
        
        credentials = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
        
        headers = {
            'Authorization': f'Basic {credentials}',
            'Content-Type': 'text/plain'
        }
        
        response = requests.post(
            CLICKHOUSE_URL,
            data=query,
            headers=headers,
            verify=False,
            timeout=30
        )
        
        if response.status_code == 200:
            return response.text
        else:
            raise Exception(f"HTTP {response.status_code}: {response.text}")
            
    except Exception as e:
        raise Exception(f"HTTP query failed: {e}")

def get_people_data():
    """Получение данных о людях из таблицы people_events"""
    try:
        client = get_clickhouse_client()
        if client:
            query = """
            SELECT 
                person_id, filename, video_link, video_id,
                start_sec, end_sec, start_dt, end_dt,
                status, zone
            FROM people_events 
            ORDER BY start_sec 
            LIMIT 1000
            """
            result = client.execute(query)
            
            df = pd.DataFrame(result, columns=[
                'person_id', 'filename', 'video_link', 'video_id',
                'start_sec', 'end_sec', 'start_dt', 'end_dt',
                'status', 'zone'
            ])
            return df
        else:
            # Пробуем через HTTP API
            query = """
            SELECT 
                person_id, filename, video_link, video_id,
                start_sec, end_sec, start_dt, end_dt,
                status, zone
            FROM people_events 
            ORDER BY start_sec 
            LIMIT 1000
            FORMAT JSON
            """
            result = execute_http_query(query)
            # Парсим JSON результат
            import json
            data = json.loads(result)
            df = pd.DataFrame(data['data'])
            return df
            
    except Exception as e:
        st.error(f"Error loading people data: {e}")
        return None

def get_train_data():
    """Получение данных о поездах из таблицы train_events"""
    try:
        client = get_clickhouse_client()
        if client:
            query = """
            SELECT 
                train_id, filename, video_link, video_id,
                arrival_sec, arrival_dt, stop_start_sec, stop_start_dt,
                stop_end_sec, stop_end_dt, departure_sec, departure_dt,
                stopped, train_number
            FROM train_events 
            ORDER BY arrival_sec 
            LIMIT 1000
            """
            result = client.execute(query)
            
            df = pd.DataFrame(result, columns=[
                'train_id', 'filename', 'video_link', 'video_id',
                'arrival_sec', 'arrival_dt', 'stop_start_sec', 'stop_start_dt',
                'stop_end_sec', 'stop_end_dt', 'departure_sec', 'departure_dt',
                'stopped', 'train_number'
            ])
            return df
        else:
            # Пробуем через HTTP API
            query = """
            SELECT 
                train_id, filename, video_link, video_id,
                arrival_sec, arrival_dt, stop_start_sec, stop_start_dt,
                stop_end_sec, stop_end_dt, departure_sec, departure_dt,
                stopped, train_number
            FROM train_events 
            ORDER BY arrival_sec 
            LIMIT 1000
            FORMAT JSON
            """
            result = execute_http_query(query)
            import json
            data = json.loads(result)
            df = pd.DataFrame(data['data'])
            return df
            
    except Exception as e:
        st.error(f"Error loading train data: {e}")
        return None

def get_people_stats():
    """Статистика по людям из таблицы people_events"""
    try:
        client = get_clickhouse_client()
        if client:
            query = """
            SELECT 
                COUNT(*) as total_people,
                COUNT(DISTINCT video_id) as unique_cameras,
                COUNT(DISTINCT zone) as unique_zones
            FROM people_events
            """
            result = client.execute(query)
            
            return {
                'total_people': result[0][0],
                'unique_cameras': result[0][1],
                'unique_zones': result[0][2]
            }
        else:
            # Пробуем через HTTP API
            query = """
            SELECT 
                COUNT(*) as total_people,
                COUNT(DISTINCT video_id) as unique_cameras,
                COUNT(DISTINCT zone) as unique_zones
            FROM people_events
            FORMAT JSON
            """
            result = execute_http_query(query)
            import json
            data = json.loads(result)
            stats = data['data'][0]
            return {
                'total_people': stats['total_people'],
                'unique_cameras': stats['unique_cameras'],
                'unique_zones': stats['unique_zones']
            }
            
    except Exception as e:
        st.error(f"Error loading people stats: {e}")
        return {
            'total_people': 0,
            'unique_cameras': 0,
            'unique_zones': 0
        }

def get_train_stats():
    """Статистика по поездам из таблицы train_events"""
    try:
        client = get_clickhouse_client()
        if client:
            # Считаем уникальные поезда по train_number, исключая NULL значения
            query = """
            SELECT 
                COUNT(*) as total_trains,
                COUNT(DISTINCT video_id) as unique_cameras,
                COUNT(DISTINCT train_number) as unique_trains
            FROM train_events
            WHERE train_number IS NOT NULL
            """
            result = client.execute(query)
            
            return {
                'total_trains': result[0][0],
                'unique_cameras': result[0][1],
                'unique_trains': result[0][2]
            }
        else:
            # Пробуем через HTTP API
            query = """
            SELECT 
                COUNT(*) as total_trains,
                COUNT(DISTINCT video_id) as unique_cameras,
                COUNT(DISTINCT train_number) as unique_trains
            FROM train_events
            WHERE train_number IS NOT NULL
            FORMAT JSON
            """
            result = execute_http_query(query)
            import json
            data = json.loads(result)
            stats = data['data'][0]
            return {
                'total_trains': stats['total_trains'],
                'unique_cameras': stats['unique_cameras'],
                'unique_trains': stats['unique_trains']
            }
            
    except Exception as e:
        st.error(f"Error loading train stats: {e}")
        return {
            'total_trains': 0,
            'unique_cameras': 0,
            'unique_trains': 0
        }

def test_connection():
    """Тестирование подключения к ClickHouse"""
    try:
        client = get_clickhouse_client()
        if client:
            result = client.execute('SELECT 1')
            return True, "✅ Connected via clickhouse-driver"
        else:
            # Пробуем HTTP
            result = execute_http_query('SELECT 1 FORMAT JSON')
            return True, "✅ Connected via HTTP API"
    except Exception as e:
        return False, f"❌ Connection failed: {e}"
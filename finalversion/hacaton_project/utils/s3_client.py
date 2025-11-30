import os
import tempfile
import shutil
import json
import random
from datetime import datetime

try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    print("⚠️ boto3 не установлен, используем локальное хранилище")

# Конфигурация Яндекс.Облака
YANDEX_S3_CONFIG = {
    'endpoint_url': 'https://storage.yandexcloud.net',
    'bucket': 'hack-ai-29',
    'region': 'ru-central1',
    'aws_access_key_id': 'YCAJEp8aNEWVf-fpvpskkLi_Z',
    'aws_secret_access_key': 'YCOppzw9MkgCihcSr5zkBLB-GV_UcRDL9hrtcoi0'
}

# Локальное хранилище как fallback
LOCAL_S3_DIR = "local_s3_storage"
os.makedirs(LOCAL_S3_DIR, exist_ok=True)

def get_s3_client():
    """Создает клиент для работы с Яндекс S3"""
    if not BOTO3_AVAILABLE:
        return None
    
    try:
        return boto3.client(
            's3',
            endpoint_url=YANDEX_S3_CONFIG['endpoint_url'],
            region_name=YANDEX_S3_CONFIG['region'],
            aws_access_key_id=YANDEX_S3_CONFIG['aws_access_key_id'],
            aws_secret_access_key=YANDEX_S3_CONFIG['aws_secret_access_key']
        )
    except Exception as e:
        print(f"❌ Ошибка создания S3 клиента: {e}")
        return None

def upload_to_s3(file):
    """Загружает файл в Яндекс.Облако S3 и локально как резервную копию"""
    
    # Всегда сохраняем локально
    local_url = _upload_to_local(file)
    
    # Пытаемся загрузить в Яндекс.Облако
    yandex_url = _upload_to_yandex_cloud(file)
    
    # Возвращаем URL Яндекс.Облака если удалось, иначе локальный
    return yandex_url if yandex_url else local_url

def _upload_to_yandex_cloud(file):
    """Загружает файл в Яндекс.Облако S3"""
    if not BOTO3_AVAILABLE:
        return None
        
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return None
            
        bucket = YANDEX_S3_CONFIG['bucket']
        
        # Создаем уникальное имя файла
        filename = f"videos/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.name}"
        
        # Загружаем файл в S3
        s3_client.upload_fileobj(
            file,
            bucket,
            filename
        )
        
        # Возвращаем S3 URL
        s3_url = f"s3://{bucket}/{filename}"
        print(f"✅ Видео загружено в Яндекс.Облако: {s3_url}")
        return s3_url
        
    except Exception as e:
        print(f"❌ Ошибка загрузки в Яндекс.Облако: {e}")
        return None

def _upload_to_local(file):
    """Сохраняем файл в локальную папку как резервную копию"""
    filename = f"videos/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.name}"
    local_path = os.path.join(LOCAL_S3_DIR, filename)
    
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    with open(local_path, "wb") as f:
        f.write(file.getvalue())
    
    local_url = f"local_s3://{filename}"
    print(f"✅ Видео сохранено локально: {local_url}")
    return local_url

def download_from_s3(s3_url):
    """Скачивает файл для просмотра (приоритет у локальной копии)"""
    
    print(f"🔍 Попытка загрузки: {s3_url}")
    
    # Сначала пробуем найти локально
    if s3_url.startswith("local_s3://"):
        local_path = _download_from_local(s3_url)
        if local_path:
            print(f"✅ Видео загружено из локального хранилища: {s3_url}")
            return local_path
        else:
            print(f"❌ Локальный файл не найден: {s3_url}")
    
    # Для фрагментов - всегда используем локальную копию исходного файла
    if "_fragment_" in s3_url:
        # Извлекаем оригинальное имя файла до _fragment_
        original_s3_url = s3_url.split('_fragment_')[0]
        print(f"🔄 Фрагмент обнаружен, используем оригинальный файл: {original_s3_url}")
        
        # Пробуем загрузить оригинальный файл
        if original_s3_url.startswith("local_s3://"):
            return _download_from_local(original_s3_url)
        elif original_s3_url.startswith("s3://"):
            # Пробуем найти локальную копию оригинала
            local_original_url = _convert_s3_to_local_url(original_s3_url)
            local_path = _download_from_local(local_original_url)
            if local_path:
                return local_path
            
            # Если локальной копии нет, пробуем скачать оригинал из S3
            return _download_from_yandex(original_s3_url)
    
    # Обычная логика для не-фрагментов
    elif s3_url.startswith("s3://"):
        # Пытаемся найти локальную копию по тому же имени
        local_url = _convert_s3_to_local_url(s3_url)
        local_path = _download_from_local(local_url)
        if local_path:
            print(f"✅ Видео загружено из локальной копии: {s3_url}")
            return local_path
        
        # Если локальной копии нет, пробуем скачать из S3
        return _download_from_yandex(s3_url)
    
    print(f"❌ Не удалось загрузить видео: {s3_url}")
    return None

def _convert_s3_to_local_url(s3_url):
    """Конвертирует S3 URL в локальный URL"""
    if s3_url.startswith("s3://"):
        path = s3_url[5:]
        # Убираем bucket name если он есть
        if '/' in path:
            path = path[path.find('/') + 1:]
        return f"local_s3://{path}"
    return s3_url

def _download_from_yandex(s3_url):
    """Скачивает файл из Яндекс.Облака"""
    if not BOTO3_AVAILABLE:
        print("⚠️ boto3 не доступен, невозможно скачать из Яндекс.Облака")
        return None
        
    try:
        s3_client = get_s3_client()
        if not s3_client:
            print("❌ Не удалось создать S3 клиент")
            return None
            
        # Парсим S3 URL
        if s3_url.startswith("s3://"):
            path = s3_url[5:]
            first_slash = path.find('/')
            if first_slash != -1:
                bucket = path[:first_slash]
                key = path[first_slash + 1:]
            else:
                bucket = path
                key = ""
        else:
            print(f"❌ Неверный S3 URL формат: {s3_url}")
            return None
        
        # Проверяем существует ли файл
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"❌ Файл не найден в Яндекс.Облаке: {s3_url}")
                return None
            else:
                print(f"❌ Ошибка при проверке файла в Яндекс.Облаке: {e}")
                return None
        
        # Создаем временный файл в отдельной папке для лучшего управления
        temp_dir = "temp_videos"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4', dir=temp_dir)
        
        s3_client.download_file(bucket, key, temp_file.name)
        print(f"✅ Видео скачано из Яндекс.Облака: {s3_url}")
        return temp_file.name
        
    except Exception as e:
        print(f"❌ Ошибка скачивания из Яндекс.Облака: {e}")
        return None

def _download_from_local(s3_url):
    """Скачивает файл из локального хранилища"""
    filename = s3_url.replace("local_s3://", "")
    local_path = os.path.join(LOCAL_S3_DIR, filename)
    
    if not os.path.exists(local_path):
        # Пробуем найти файл без временной метки (для фрагментов)
        if "_fragment_" in filename:
            # Ищем любой файл с таким же именем до _fragment_
            base_name = filename.split('_fragment_')[0]
            for file in os.listdir(os.path.dirname(local_path)):
                if file.startswith(base_name) and not file.endswith('.json'):
                    found_path = os.path.join(os.path.dirname(local_path), file)
                    print(f"🔄 Найден похожий файл для фрагмента: {found_path}")
                    
                    # Создаем временную копию
                    temp_dir = "temp_videos"
                    os.makedirs(temp_dir, exist_ok=True)
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4', dir=temp_dir)
                    shutil.copy2(found_path, temp_file.name)
                    return temp_file.name
        
        return None
    
    # Создаем временный файл в отдельной папке
    temp_dir = "temp_videos"
    os.makedirs(temp_dir, exist_ok=True)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4', dir=temp_dir)
    shutil.copy2(local_path, temp_file.name)
    return temp_file.name

def get_ml_results(s3_url):
    """Генерирует тестовые ML результаты (пока локально)"""
    import time
    
    # Имитируем задержку обработки
    time.sleep(2)
    
    # Генерируем тестовые метрики
    ml_results = {
        "people_count": random.randint(1, 15),
        "efficiency": round(random.uniform(0.3, 0.98), 2),
        "violations": random.randint(0, 8),
        "activities": ["working", "walking", "lifting", "talking", "sitting"],
        "timestamp": datetime.now().isoformat()
    }
    
    # Сохраняем JSON локально для последующего использования
    if s3_url.startswith("local_s3://"):
        json_filename = s3_url.replace("local_s3://", "").replace('.mp4', '.json')
        json_path = os.path.join(LOCAL_S3_DIR, json_filename)
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        with open(json_path, 'w') as f:
            json.dump(ml_results, f, indent=2)
    
    print(f"✅ Сгенерированы ML результаты для: {s3_url}")
    return ml_results

def clear_yandex_cloud():
    """Очищает все файлы в Яндекс.Облаке"""
    if not BOTO3_AVAILABLE:
        print("⚠️ boto3 не установлен, очистка Яндекс.Облака невозможна")
        return False
        
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
            
        bucket = YANDEX_S3_CONFIG['bucket']
        
        # Получаем список всех объектов
        objects_to_delete = []
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket):
            if 'Contents' in page:
                objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])
        
        # Удаляем все объекты
        if objects_to_delete:
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects_to_delete}
            )
            print(f"✅ Удалено {len(objects_to_delete)} объектов из Яндекс.Облака")
            return True
        else:
            print("ℹ️ В Яндекс.Облаке нет объектов для удаления")
            return True
            
    except Exception as e:
        print(f"❌ Ошибка очистки Яндекс.Облака: {e}")
        return False

def clear_local_storage():
    """Очищает локальное хранилище"""
    try:
        if os.path.exists(LOCAL_S3_DIR):
            shutil.rmtree(LOCAL_S3_DIR)
            os.makedirs(LOCAL_S3_DIR, exist_ok=True)
            print("✅ Локальное хранилище очищено")
            return True
        return True
    except Exception as e:
        print(f"❌ Ошибка очистки локального хранилища: {e}")
        return False

def clear_temp_files():
    """Очищает временные файлы"""
    try:
        temp_dir = "temp_videos"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)
            print("✅ Временные файлы очищены")
            return True
        return True
    except Exception as e:
        print(f"❌ Ошибка очистки временных файлов: {e}")
        return False
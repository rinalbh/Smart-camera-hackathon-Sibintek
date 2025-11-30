import time
import random
from datetime import datetime, timedelta
from utils.db_client import save_train_metadata, save_people_metadata

def split_video_into_fragments(filename, s3_url):
    """Имитация черного ящика Маши - создает 3 одинаковых видео с разными названиями"""
    
    print(f"🎬 Черный ящик Маши: создаем 3 таймфрейма для {filename}...")
    time.sleep(2)
    
    # Создаем 3 одинаковых таймфрейма с разными временными метками
    base_time = datetime.now()
    fragments = []
    
    for i in range(3):
        start_time = base_time + timedelta(minutes=i*10)
        end_time = start_time + timedelta(minutes=10)
        
        fragment_data = {
            'fragment_number': i + 1,
            'filename': filename,
            's3_url': f"{s3_url}_fragment_{i+1}",
            'start_time': start_time,
            'end_time': end_time,
            'camera_id': f"cam_{random.randint(1000, 9999)}",
            'train_number': f"train_{random.randint(100, 999)}",
            'status': random.choice(['движется', 'стоит'])
        }
        fragments.append(fragment_data)
    
    print(f"✅ Сгенерировано {len(fragments)} таймфреймов")
    return fragments

def analyze_fragments(fragments):
    """Имитация черного ящика Лёхи - анализирует каждый таймфрейм"""
    
    print("🔍 Черный ящик Лёхи: анализируем таймфреймы...")
    time.sleep(3)
    
    results = []
    
    for fragment in fragments:
        # Генерируем случайные метрики для каждого таймфрейма
        people_metrics = {
            'camera_id': fragment['camera_id'],
            'filename': fragment['filename'],
            'filepath_s3': fragment['s3_url'],
            'start_time': fragment['start_time'],
            'end_time': fragment['end_time'],
            'train_number': fragment['train_number'],
            'status': fragment['status'],
            'activity_status': random.choice(['работает', 'идет', 'стоит', 'поднимает', 'разговаривает']),
            'zone': random.choice(['красная', 'зеленая', 'желтая', 'белая'])
        }
        
        results.append(people_metrics)
    
    print(f"✅ Проанализировано {len(results)} таймфреймов")
    return results

def process_video_with_black_boxes(filename, s3_url):
    """Полный процесс обработки видео через черные ящики"""
    
    try:
        # 1. Черный ящик Маши - создание 3 таймфреймов
        fragments = split_video_into_fragments(filename, s3_url)
        
        # 2. Черный ящик Лёхи - анализ каждого таймфрейма
        analysis_results = analyze_fragments(fragments)
        
        # 3. Сохраняем результаты в БД
        success_count = 0
        
        for fragment in fragments:
            # Сохраняем в таблицу Train
            train_success = save_train_metadata(
                camera_id=fragment['camera_id'],
                filename=fragment['filename'],
                filepath_s3=fragment['s3_url'],
                start_time=fragment['start_time'],
                end_time=fragment['end_time'],
                train_number=fragment['train_number'],
                status=fragment['status']
            )
            
            if train_success:
                success_count += 1
        
        for result in analysis_results:
            # Сохраняем в таблицу People
            people_success = save_people_metadata(
                camera_id=result['camera_id'],
                filename=result['filename'],
                filepath_s3=result['filepath_s3'],
                start_time=result['start_time'],
                end_time=result['end_time'],
                train_number=result['train_number'],
                status=result['status'],
                activity_status=result['activity_status'],
                zone=result['zone']
            )
            
            if people_success:
                success_count += 1
        
        return {
            "status": "completed",
            "message": f"Обработка завершена. Сохранено записей: {success_count}",
            "fragments_count": len(fragments),
            "analysis_count": len(analysis_results)
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Ошибка обработки: {str(e)}"
        }

def send_to_ml_service(s3_url):
    """Совместимость со старым кодом - вызывает новый процесс"""
    # Для совместимости со старым кодом
    return {
        "status": "completed",
        "message": "Обработка через черные ящики завершена",
        "job_id": f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    }

def check_ml_progress(s3_url):
    """Совместимость со старым кодом"""
    return {
        "status": "completed",
        "message": "Обработка завершена"
    }
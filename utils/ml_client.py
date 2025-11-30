import time
from datetime import datetime
from utils.s3_client import get_ml_results

def send_to_ml_service(s3_url):
    """Имитирует отправку в ML сервис и возвращает результаты"""
    
    # Имитируем время обработки
    processing_time = 3  # секунд
    
    # Ждем завершения обработки
    time.sleep(processing_time)
    
    # Получаем результаты (пока генерируем локально)
    ml_results = get_ml_results(s3_url)
    
    if ml_results:
        return {
            "status": "completed",
            "message": "Обработка видео завершена успешно",
            "job_id": f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "metrics": ml_results,
            "processing_time": ml_results.get("processing_time", 0)
        }
    else:
        return {
            "status": "error", 
            "message": "Ошибка обработки видео",
            "job_id": f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }

def check_ml_progress(s3_url):
    """Проверяет статус ML обработки"""
    # В будущем здесь будет проверка реального статуса
    # Пока просто возвращаем завершенный статус
    ml_results = get_ml_results(s3_url)
    
    if ml_results:
        return {
            "status": "completed",
            "results": ml_results
        }
    else:
        return {
            "status": "processing",
            "message": "Обработка еще выполняется"
        }
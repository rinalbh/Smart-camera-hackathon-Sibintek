# main_processor.py (исправленная версия)
import os
import cv2
import base64
import json
import time
import math
import re
import requests
import boto3
import tempfile
from dataclasses import dataclass, asdict
from typing import Optional, List, Tuple
from tqdm import tqdm
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("🚀 Запуск основного процессора видео для интеграции с веб-приложением...")

# ================== КОНФИГУРАЦИЯ YANDEX CLOUD S3 ==================
YANDEX_S3_CONFIG = {
    'endpoint_url': '',
    'bucket': '',
    'region': '',
    'aws_access_key_id': '',
    'aws_secret_access_key': ''
}

S3_VIDEO_PREFIX = "videos/"
S3_OUTPUT_PREFIX = "processed/"

VIDEO_EXTENSIONS = ['.mov', '.mp4', '.avi', '.mkv']
COARSE_STEP_SEC = 10.0
REFINE_STEP_SEC = 1.0
JUMP_THRESHOLD_SEC = 60

MODEL_NAME = "qwen/qwen3-vl-30b-a3b-instruct"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

CROP_HEIGHT_RATIO = 0.20
CROP_WIDTH_RATIO = 0.50

OPENROUTER_API_KEY_FALLBACK = "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"

# ================== ДАТАКЛАССЫ ==================
@dataclass
class FrameInfo:
    frame_index: int
    video_time_sec: float
    ocr_date: Optional[str] = None
    ocr_time: Optional[str] = None
    ocr_seconds: Optional[int] = None
    camera_id: Optional[str] = None
    raw_response: str = ""
    scan_level: str = ""

@dataclass
class SessionInfo:
    session_index: int
    start_video_sec: float
    end_video_sec: float
    start_ocr_date: Optional[str] = None
    start_ocr_time: Optional[str] = None
    end_ocr_date: Optional[str] = None
    end_ocr_time: Optional[str] = None
    s3_key: Optional[str] = None

# ================== S3 КЛИЕНТ ==================
def get_s3_client():
    """Создает клиент для работы с Яндекс S3"""
    try:
        return boto3.client(
            's3',
            endpoint_url=YANDEX_S3_CONFIG['endpoint_url'],
            region_name=YANDEX_S3_CONFIG['region'],
            aws_access_key_id=YANDEX_S3_CONFIG['aws_access_key_id'],
            aws_secret_access_key=YANDEX_S3_CONFIG['aws_secret_access_key']
        )
    except Exception as e:
        logger.error(f"Ошибка создания S3 клиента: {e}")
        return None

def download_video_from_s3(s3_key: str, local_path: str) -> bool:
    """Скачивает видео из S3"""
    try:
        s3 = get_s3_client()
        if not s3:
            return False
            
        s3.download_file(YANDEX_S3_CONFIG['bucket'], s3_key, local_path)
        logger.info(f"✅ Видео скачано: {s3_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка скачивания: {e}")
        return False

def upload_file_to_s3(local_path: str, s3_key: str) -> bool:
    """Загружает файл в S3"""
    try:
        s3 = get_s3_client()
        if not s3:
            return False
            
        s3.upload_file(local_path, YANDEX_S3_CONFIG['bucket'], s3_key)
        logger.info(f"✅ Файл загружен: {s3_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки: {e}")
        return False

def extract_session_video(input_path: str, output_path: str, start_sec: float, end_sec: float) -> bool:
    """Вырезает сегмент видео"""
    try:
        cap = cv2.VideoCapture(input_path)
        if not cap.isOpened():
            logger.error(f"Не удалось открыть видео: {input_path}")
            return False
            
        fps = cap.get(cv2.CAP_PROP_FPS)
        if fps == 0:
            logger.error("Не удалось определить FPS")
            return False
            
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        start_frame = int(start_sec * fps)
        end_frame = int(end_sec * fps)
        
        if start_frame >= end_frame:
            logger.error(f"Некорректные временные метки: {start_sec} - {end_sec}")
            return False
            
        cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
        out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
        
        if not out.isOpened():
            logger.error("Не удалось создать VideoWriter")
            return False
        
        current_frame = start_frame
        while current_frame <= end_frame:
            success, frame = cap.read()
            if not success:
                break
            out.write(frame)
            current_frame += 1
        
        cap.release()
        out.release()
        logger.info(f"✅ Сессия вырезана: {output_path} ({start_sec}-{end_sec} сек)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка вырезания видео: {e}")
        return False

# ================== OCR ФУНКЦИИ ==================
def load_api_key() -> str:
    """Загружает API ключ"""
    api_key = os.getenv("OPENROUTER_API_KEY") or OPENROUTER_API_KEY_FALLBACK
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY не задан")
    return api_key

def encode_image_to_data_url(image_bgr) -> str:
    """Кодирует изображение в data URL"""
    success, buffer = cv2.imencode(".jpg", image_bgr)
    if not success:
        raise RuntimeError("Не удалось закодировать кадр в JPEG")
    b64 = base64.b64encode(buffer.tobytes()).decode("utf-8")
    return f"data:image/jpeg;base64,{b64}"

def call_openrouter_qwen(image_data_url: str, api_key: str) -> dict:
    """Вызывает OpenRouter API для OCR"""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://example.com",
        "X-Title": "video-timestamp-extractor-qwen3",
    }

    prompt_text = (
        "Ты анализируешь кадр с камеры видеонаблюдения.\n"
        "В левом верхнем углу написаны дата и время, например '2022-03-22 04:50:01'.\n\n"
        "Твоя задача — считать:\n"
        "1) Дату в формате YYYY-MM-DD (если невозможно — верни null).\n"
        "2) Время в формате HH:MM:SS (если невозможно — верни null).\n"
        "3) Идентификатор камеры (например CAM01, CH2 и т.п.; если не видно — верни null).\n\n"
        "Верни ответ строго в формате JSON:\n"
        "{\n"
        "  \"date\": \"YYYY-MM-DD или null\",\n"
        "  \"time\": \"HH:MM:SS или null\",\n"
        "  \"camera_id\": \"строка или null\"\n"
        "}\n"
        "Никакого дополнительного текста, только JSON."
    )

    body = {
        "model": MODEL_NAME,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt_text},
                    {"type": "image_url", "image_url": {"url": image_data_url}},
                ],
            }
        ],
        "stream": False,
        "temperature": 0.0,
    }

    try:
        response = requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=120)
        response.raise_for_status()
        data = response.json()
        
        content = data["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        
        if not isinstance(parsed, dict):
            raise ValueError("JSON не является объектом")
            
        return parsed
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка HTTP запроса: {e}")
        return {"date": None, "time": None, "camera_id": None, "error": str(e)}
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        logger.error(f"Ошибка парсинга ответа: {e}")
        return {"date": None, "time": None, "camera_id": None, "raw": content if 'content' in locals() else "No content"}

def parse_hms_to_seconds(time_str: Optional[str]) -> Optional[int]:
    """Парсит время в секунды"""
    if not time_str:
        return None
    match = re.search(r"(\d{1,2}):(\d{2}):(\d{2})", time_str)
    if not match:
        return None
    hours, minutes, seconds = map(int, match.groups())
    return hours * 3600 + minutes * 60 + seconds

def time_diff_seconds(a: Optional[int], b: Optional[int]) -> Optional[int]:
    """Вычисляет разницу во времени в секундах"""
    if a is None or b is None:
        return None
    diff = abs(b - a)
    if diff > 12 * 3600:  # Коррекция для перехода через полночь
        diff = 24 * 3600 - diff
    return diff

def analyze_frame_at_time(
    cap: cv2.VideoCapture,
    api_key: str,
    time_sec: float,
    scan_level: str,
    fps: float,
) -> Optional[FrameInfo]:
    """Анализирует кадр в указанное время"""
    if time_sec < 0:
        time_sec = 0.0

    cap.set(cv2.CAP_PROP_POS_MSEC, time_sec * 1000.0)
    success, frame = cap.read()
    if not success:
        return None

    msec = cap.get(cv2.CAP_PROP_POS_MSEC)
    video_time_sec = (msec or time_sec * 1000.0) / 1000.0
    frame_index = int(cap.get(cv2.CAP_PROP_POS_FRAMES))

    height, width = frame.shape[:2]
    crop_h = max(int(height * CROP_HEIGHT_RATIO), 10)
    crop_w = max(int(width * CROP_WIDTH_RATIO), 10)
    cropped = frame[0:crop_h, 0:crop_w]

    try:
        img_data_url = encode_image_to_data_url(cropped)
        parsed = call_openrouter_qwen(img_data_url, api_key)
        
        ocr_date = parsed.get("date")
        ocr_time = parsed.get("time")
        camera_id = parsed.get("camera_id")
        raw_response = json.dumps(parsed, ensure_ascii=False)
        
    except Exception as e:
        logger.error(f"Ошибка анализа кадра: {e}")
        ocr_date = ocr_time = camera_id = None
        raw_response = f"ERROR: {str(e)}"

    ocr_seconds = parse_hms_to_seconds(ocr_time)

    return FrameInfo(
        frame_index=frame_index,
        video_time_sec=video_time_sec,
        ocr_date=ocr_date,
        ocr_time=ocr_time,
        ocr_seconds=ocr_seconds,
        camera_id=camera_id,
        raw_response=raw_response,
        scan_level=scan_level,
    )

def process_video(video_path: str) -> Tuple[List[FrameInfo], List[SessionInfo]]:
    """Основная функция обработки видео"""
    api_key = load_api_key()

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Не удалось открыть видео: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    duration_sec = total_frames / fps if total_frames > 0 else 0.0

    logger.info(f"📊 Анализ видео: {duration_sec:.2f} сек, {total_frames} кадров, {fps:.2f} FPS")

    all_frames: List[FrameInfo] = []
    sessions: List[SessionInfo] = []
    coarse_frames: List[FrameInfo] = []

    n_steps = int(math.floor(duration_sec / COARSE_STEP_SEC)) + 1
    logger.info(f"🔍 Сканирование каждые {COARSE_STEP_SEC} секунд ({n_steps} шагов)...")
    
    for i in tqdm(range(n_steps), desc="Сканирование"):
        time_sec = min(i * COARSE_STEP_SEC, duration_sec)
        frame_info = analyze_frame_at_time(cap, api_key, time_sec, "10s", fps)
        if frame_info is not None:
            coarse_frames.append(frame_info)
            all_frames.append(frame_info)
        time.sleep(0.05)  # Пауза чтобы не перегружать API

    if not coarse_frames:
        cap.release()
        return all_frames, sessions

    # Обнаружение сессий (упрощенная логика)
    current_session_index = 1
    current_start = coarse_frames[0]

    for i in range(len(coarse_frames) - 1):
        frame_a = coarse_frames[i]
        frame_b = coarse_frames[i + 1]

        diff = time_diff_seconds(frame_a.ocr_seconds, frame_b.ocr_seconds)

        if diff is not None and diff > JUMP_THRESHOLD_SEC:
            # Конец сессии
            sessions.append(SessionInfo(
                session_index=current_session_index,
                start_video_sec=current_start.video_time_sec,
                end_video_sec=frame_a.video_time_sec,
                start_ocr_date=current_start.ocr_date,
                start_ocr_time=current_start.ocr_time,
                end_ocr_date=frame_a.ocr_date,
                end_ocr_time=frame_a.ocr_time,
            ))
            current_session_index += 1
            current_start = frame_b

    # Добавляем последнюю сессию
    last_frame = coarse_frames[-1]
    sessions.append(SessionInfo(
        session_index=current_session_index,
        start_video_sec=current_start.video_time_sec,
        end_video_sec=last_frame.video_time_sec,
        start_ocr_date=current_start.ocr_date,
        start_ocr_time=current_start.ocr_time,
        end_ocr_date=last_frame.ocr_date,
        end_ocr_time=last_frame.ocr_time,
    ))

    cap.release()
    return all_frames, sessions

def save_result_to_json(frames: List[FrameInfo], sessions: List[SessionInfo], path: str) -> None:
    """Сохраняет результат в JSON"""
    data = {
        "video_path": "yandex_cloud_processed",
        "frames": [asdict(f) for f in frames],
        "sessions": [asdict(s) for s in sessions],
    }
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

# ================== ИНТЕГРАЦИЯ С ВЕБ-ПРИЛОЖЕНИЕМ ==================

def process_uploaded_video(s3_url: str, filename: str) -> dict:
    """
    Основная функция для интеграции с веб-приложением
    Обрабатывает загруженное видео и возвращает результат
    """
    try:
        logger.info(f"🎯 Начинаем обработку видео: {filename} ({s3_url})")
        
        # Извлекаем S3 ключ из URL
        if s3_url.startswith("s3://"):
            s3_key = s3_url[5:]  # Убираем "s3://"
            # Убираем bucket name если он есть
            if '/' in s3_key:
                s3_key = s3_key[s3_key.find('/') + 1:]
        elif s3_url.startswith("local_s3://"):
            # Для локального хранилища
            s3_key = s3_url.replace("local_s3://", "")
        else:
            return {"status": "error", "message": f"Неизвестный формат URL: {s3_url}"}
        
        # Скачиваем видео во временный файл
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_file:
            temp_path = temp_file.name
        
        logger.info(f"📥 Скачиваем видео: {s3_key}")
        if not download_video_from_s3(s3_key, temp_path):
            return {"status": "error", "message": f"Ошибка скачивания видео: {s3_key}"}
        
        # Обрабатываем видео
        logger.info("🔍 Начинаем анализ видео...")
        frames, sessions = process_video(temp_path)
        
        # Сохраняем JSON результат
        json_filename = f"{os.path.splitext(filename)[0]}_result.json"
        local_json_path = os.path.join(tempfile.gettempdir(), json_filename)
        save_result_to_json(frames, sessions, local_json_path)
        
        # Загружаем JSON в S3
        json_s3_key = f"{S3_OUTPUT_PREFIX}{json_filename}"
        if not upload_file_to_s3(local_json_path, json_s3_key):
            logger.warning(f"Не удалось загрузить JSON в S3: {json_s3_key}")
        
        # Вырезаем и загружаем сессии (первые 3)
        processed_sessions = []
        sessions_to_process = sessions[:3]  # Обрабатываем первые 3 сессии
        
        for session in sessions_to_process:
            session_video_path = os.path.join(tempfile.gettempdir(), f"session_{session.session_index}.mp4")
            
            logger.info(f"🎬 Вырезаем сессию {session.session_index}")
            
            if extract_session_video(temp_path, session_video_path, 
                                   session.start_video_sec, session.end_video_sec):
                
                # ИСПРАВЛЕНО: Используем то же имя файла, но с расширением .mp4
                original_name = os.path.splitext(filename)[0]  # Убираем оригинальное расширение
                session_s3_key = f"{S3_OUTPUT_PREFIX}{original_name}_session_{session.session_index}.mp4"
                
                logger.info(f"📤 Загружаем сессию в S3: {session_s3_key}")
                if upload_file_to_s3(session_video_path, session_s3_key):
                    # Создаем полный S3 URL для сохранения в БД
                    full_s3_url = f"s3://{YANDEX_S3_CONFIG['bucket']}/{session_s3_key}"
                    processed_sessions.append({
                        "session_index": session.session_index,
                        "s3_key": full_s3_url,  # Полный URL
                        "s3_key_short": session_s3_key,
                        "start_time": session.start_ocr_time or "10:00:00",
                        "end_time": session.end_ocr_time or "10:05:00",
                        "start_sec": session.start_video_sec,
                        "end_sec": session.end_video_sec
                    })
                    logger.info(f"✅ Сессия {session.session_index} загружена: {full_s3_url}")
                else:
                    logger.error(f"❌ Не удалось загрузить сессию {session.session_index} в S3")
                
                # Удаляем временный файл сессии
                try:
                    os.remove(session_video_path)
                except OSError:
                    pass
            else:
                logger.error(f"❌ Не удалось вырезать сессию {session.session_index}")
        
        # Удаляем временные файлы
        try:
            os.remove(temp_path)
            os.remove(local_json_path)
        except OSError:
            pass
        
        result = {
            "status": "completed",
            "message": f"Обработка завершена успешно",
            "original_video": s3_url,
            "processed_sessions": processed_sessions,
            "json_result": f"{S3_OUTPUT_PREFIX}{json_filename}",
            "frames_analyzed": len(frames),
            "sessions_found": len(sessions),
            "sessions_processed": len(processed_sessions)
        }
        
        logger.info(f"🎉 ОБРАБОТКА ЗАВЕРШЕНА: {len(frames)} кадров, {len(sessions)} сессий, {len(processed_sessions)} загружено")
        return result
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки видео: {e}")
        return {"status": "error", "message": f"Ошибка обработки: {str(e)}"}
    
# ================== ЗАПУСК КАК САМОСТОЯТЕЛЬНОГО СКРИПТА ==================

def main():
    """Основная функция для самостоятельного запуска"""
    logger.info("🚀 ЗАПУСК ОСНОВНОГО ПРОЦЕССОРА ВИДЕО")
    
    # Проверяем соединение с S3
    try:
        s3_client = get_s3_client()
        s3_client.list_objects_v2(Bucket=YANDEX_S3_CONFIG['bucket'], MaxKeys=1)
        logger.info("✅ Соединение с Yandex Cloud S3 установлено")
    except Exception as e:
        logger.error(f"❌ Ошибка соединения с Yandex Cloud S3: {e}")
        return
    
    # Ищем видео для обработки
    logger.info("📹 Поиск видео в Yandex Cloud S3...")
    try:
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(Bucket=YANDEX_S3_CONFIG['bucket'], Prefix=S3_VIDEO_PREFIX)
        
        videos = []
        for obj in response.get('Contents', []):
            key = obj['Key']
            if any(key.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
                videos.append(key)
        
        logger.info(f"📹 Найдено видео: {len(videos)}")
        for video in videos:
            logger.info(f"   - {video}")
        
        if not videos:
            logger.error("❌ Видео не найдены в S3")
            return
        
        # Обрабатываем первое видео
        video_key = videos[0]
        s3_url = f"s3://{YANDEX_S3_CONFIG['bucket']}/{video_key}"
        filename = os.path.basename(video_key)
        
        result = process_uploaded_video(s3_url, filename)
        logger.info(f"📊 Результат: {result}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при работе с S3: {e}")

if __name__ == "__main__":
    main()
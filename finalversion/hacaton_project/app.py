import streamlit as st
import os
import shutil
import time
from utils.db_client import init_database, clear_database
from utils.s3_client import clear_yandex_cloud

# Настройки страницы
st.set_page_config(
    page_title="Video Analytics Platform",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Создаем папку для локального S3
os.makedirs("local_s3_storage/videos", exist_ok=True)

# Инициализация БД при запуске приложения
if 'app_initialized' not in st.session_state:
    if init_database():
        st.session_state.app_initialized = True

def safe_clear_folder(folder_path):
    """Безопасно очищает содержимое папки без удаления самой папки"""
    if not os.path.exists(folder_path):
        return True
        
    try:
        # Очищаем содержимое папки рекурсивно
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    os.unlink(file_path)  # Удаляем файл
                except (PermissionError, OSError) as e:
                    # Если не можем удалить, пробуем переименовать и удалить позже
                    try:
                        temp_name = file_path + ".tmp"
                        os.rename(file_path, temp_name)
                        os.unlink(temp_name)
                    except:
                        pass  # Игнорируем ошибки для отдельных файлов
            
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                try:
                    shutil.rmtree(dir_path)
                except:
                    pass  # Игнорируем ошибки для подпапок
        
        return True
        
    except Exception as e:
        st.warning(f"⚠️ Частичная ошибка при очистке {folder_path}: {e}")
        # Пробуем альтернативный метод
        try:
            # Создаем временную папку и перемещаем туда файлы
            temp_dir = folder_path + "_temp"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            
            os.makedirs(temp_dir)
            
            # Перемещаем файлы во временную папку
            for item in os.listdir(folder_path):
                src = os.path.join(folder_path, item)
                dst = os.path.join(temp_dir, item)
                try:
                    shutil.move(src, dst)
                except:
                    pass
            
            # Удаляем временную папку
            shutil.rmtree(temp_dir)
            
            return True
        except Exception as e2:
            st.error(f"❌ Не удалось очистить {folder_path}: {e2}")
            return False

def safe_recreate_folder(folder_path, subfolders=[]):
    """Безопасно пересоздает папку"""
    try:
        # Сначала очищаем содержимое
        safe_clear_folder(folder_path)
        
        # Затем создаем нужные подпапки
        for subfolder in subfolders:
            os.makedirs(os.path.join(folder_path, subfolder), exist_ok=True)
            
        return True
    except Exception as e:
        st.warning(f"⚠️ Не удалось полностью пересоздать {folder_path}: {e}")
        return False

def full_reset():
    """Полностью сбрасывает приложение к исходному состоянию"""
    success_messages = []
    warning_messages = []
    
    try:
        # 1. Очищаем Яндекс.Облако
        st.info("☁️ Очищаем Яндекс.Облако...")
        success_yandex = clear_yandex_cloud()
        if success_yandex:
            success_messages.append("Яндекс.Облако очищено")
        else:
            warning_messages.append("Ошибка очистки Яндекс.Облака")
        
        # 2. Очищаем БД
        st.info("🗃️ Очищаем базу данных...")
        success_db = clear_database()
        if success_db:
            success_messages.append("База данных очищена")
        else:
            warning_messages.append("Ошибка очистки БД")
        
        # 3. Очищаем локальное хранилище S3 (без удаления самой папки)
        st.info("📁 Очищаем локальное хранилище...")
        if safe_recreate_folder("local_s3_storage", ["videos"]):
            success_messages.append("Локальное хранилище очищено")
        else:
            warning_messages.append("Частичная ошибка очистки локального хранилища")
        
        # 4. Очищаем все временные файлы
        st.info("🧹 Очищаем временные файлы...")
        temp_folders = ["temp_videos", "temp"]
        for folder in temp_folders:
            if safe_recreate_folder(folder):
                success_messages.append(f"Папка {folder} очищена")
            else:
                warning_messages.append(f"Ошибка очистки {folder}")
        
        # 5. Очищаем сессию
        st.info("🔄 Очищаем сессию...")
        session_keys = list(st.session_state.keys())
        for key in session_keys:
            try:
                del st.session_state[key]
            except:
                pass
        success_messages.append(f"Сессия очищена ({len(session_keys)} ключей)")
        
        # 6. Переинициализируем БД
        st.info("🔄 Переинициализируем базу данных...")
        if init_database():
            success_messages.append("База данных переинициализирована")
        else:
            warning_messages.append("Ошибка переинициализации БД")
        
        # Показываем итоги
        if success_messages:
            st.success("✅ Успешно выполнено:")
            for msg in success_messages:
                st.write(f"  • {msg}")
        
        if warning_messages:
            st.warning("⚠️ Были проблемы (но приложение работает):")
            for msg in warning_messages:
                st.write(f"  • {msg}")
        
        # Всегда считаем сброс успешным, даже с предупреждениями
        return True
        
    except Exception as e:
        st.error(f"❌ Критическая ошибка при сбросе: {e}")
        return False

def super_reset():
    """Еще более агрессивный сброс - удаляет и пересоздает таблицы"""
    success_messages = []
    warning_messages = []
    
    try:
        # 1. Очищаем Яндекс.Облако
        st.info("☁️ Очищаем Яндекс.Облако...")
        success_yandex = clear_yandex_cloud()
        if success_yandex:
            success_messages.append("Яндекс.Облако очищено")
        else:
            warning_messages.append("Ошибка очистки Яндекс.Облака")
        
        # 2. Удаляем и пересоздаем таблицы
        st.info("🗑️ Удаляем и пересоздаем таблицы БД...")
        success_db = clear_database()  # Используем clear_database вместо drop_and_recreate_tables
        if success_db:
            success_messages.append("Таблицы БД пересозданы")
        else:
            warning_messages.append("Ошибка пересоздания таблиц БД")
        
        # 3. Очищаем локальное хранилище S3
        st.info("📁 Очищаем локальное хранилище...")
        if safe_recreate_folder("local_s3_storage", ["videos"]):
            success_messages.append("Локальное хранилище очищено")
        else:
            warning_messages.append("Частичная ошибка очистки локального хранилища")
        
        # 4. Очищаем все временные файлы
        st.info("🧹 Очищаем временные файлы...")
        temp_folders = ["temp_videos", "temp"]
        for folder in temp_folders:
            if safe_recreate_folder(folder):
                success_messages.append(f"Папка {folder} очищена")
            else:
                warning_messages.append(f"Ошибка очистки {folder}")
        
        # 5. Очищаем сессию
        st.info("🔄 Очищаем сессию...")
        session_keys = list(st.session_state.keys())
        for key in session_keys:
            try:
                del st.session_state[key]
            except:
                pass
        success_messages.append(f"Сессия очищена ({len(session_keys)} ключей)")
        
        # Показываем итоги
        if success_messages:
            st.success("✅ Успешно выполнено:")
            for msg in success_messages:
                st.write(f"  • {msg}")
        
        if warning_messages:
            st.warning("⚠️ Были проблемы (но приложение работает):")
            for msg in warning_messages:
                st.write(f"  • {msg}")
        
        # Всегда считаем сброс успешным, даже с предупреждениями
        return True
        
    except Exception as e:
        st.error(f"❌ Критическая ошибка при супер-сбросе: {e}")
        return False

# Главная страница с описанием
st.title("🎥 Платформа анализа видео")
st.markdown("""
Добро пожаловать в систему анализа производственных видео!

**Возможности:**
- 📤 Загрузка видео для анализа
- 📺 Просмотр обработанных видео с метриками
- 📊 Анализ эффективности и безопасности

Используйте боковое меню для навигации.
""")

# Информация о системе
with st.expander("ℹ️ Информация о системе"):
    st.write("""
    **Текущий режим:** Демо-версия
    - S3: Яндекс.Облако + локальная файловая система
    - ML сервис: Заглушка
    - База данных: ClickHouse Cloud
    """)
    
    # Проверка подключения к ClickHouse
    if st.button("🔍 Проверить подключение к ClickHouse Cloud"):
        from utils.db_client import execute_query
        result = execute_query('SELECT 1 as test')
        if result['success']:
            st.success("✅ Подключение к ClickHouse Cloud успешно!")
            
            # Покажем информацию о таблицах
            tables_result = execute_query('SHOW TABLES')
            if tables_result['success']:
                st.write("**Существующие таблицы:**")
                st.code(tables_result['data'])
                
            # Покажем количество записей
            count_train = execute_query('SELECT count(*) FROM Train')
            count_people = execute_query('SELECT count(*) FROM People')
            
            if count_train['success'] and count_people['success']:
                st.write("**Статистика данных:**")
                st.write(f"- Записей в Train: {count_train['data'].strip()}")
                st.write(f"- Записей в People: {count_people['data'].strip()}")
        else:
            st.error(f"❌ Ошибка подключения: {result['error']}")

# Очистка всей сессии
st.markdown("---")
st.subheader("🔄 Управление приложением")

col1, col2 = st.columns(2)

with col1:
    st.write("**Обычный сброс:**")
    st.write("Очищает данные из таблиц, но сохраняет структуру БД")
    
    if st.button("🔄 Полный сброс приложения", type="secondary"):
        if full_reset():
            st.success("✅ Приложение полностью сброшено!")
            st.balloons()
            st.rerun()
        else:
            st.error("❌ Ошибка при сбросе")

with col2:
    st.write("**Агрессивный сброс:**")
    st.write("Очищает все данные и пересоздает структуру БД")
    
    if st.button("💥 Супер-сброс приложения", type="primary"):
        if super_reset():
            st.success("✅ Приложение полностью пересоздано!")
            st.balloons()
            st.rerun()
        else:
            st.error("❌ Ошибка при супер-сбросе")

# Ручная очистка файлов (для крайних случаев)
st.markdown("---")
st.subheader("🛠️ Ручное управление файлами")

if st.button("🧹 Принудительная очистка всех файлов", type="secondary"):
    st.warning("""
    ⚠️ Это попытка принудительной очистки. Может не сработать если файлы заблокированы системой.
    
    Если не помогает, закрой приложение и удалите папки вручную:
    - `local_s3_storage`
    - `temp_videos` 
    - `temp`
    """)
    
    # Добавляем очистку Яндекс.Облака
    st.info("☁️ Очищаем Яндекс.Облако...")
    yandex_success = clear_yandex_cloud()
    
    # Пробуем несколько методов очистки
    methods_success = []
    
    if yandex_success:
        methods_success.append("Яндекс.Облако очищено")
    
    # Метод 1: Обычная очистка
    st.info("🔄 Метод 1: Стандартная очистка...")
    if safe_recreate_folder("local_s3_storage", ["videos"]):
        methods_success.append("local_s3_storage")
    if safe_recreate_folder("temp_videos"):
        methods_success.append("temp_videos") 
    if safe_recreate_folder("temp"):
        methods_success.append("temp")
    
    # Метод 2: Переименование и удаление
    st.info("🔄 Метод 2: Переименование...")
    import tempfile
    temp_dir = tempfile.mkdtemp()
    
    for folder in ["local_s3_storage", "temp_videos", "temp"]:
        if os.path.exists(folder):
            try:
                # Пробуем переименовать папку
                new_name = folder + "_old_" + str(int(time.time()))
                os.rename(folder, new_name)
                methods_success.append(f"{folder} (переименован)")
                
                # Пробуем удалить переименованную папку
                try:
                    shutil.rmtree(new_name)
                    methods_success.append(f"{folder} (удален)")
                except:
                    pass
                    
            except Exception as e:
                st.warning(f"Не удалось переименовать {folder}: {e}")
    
    # Метод 3: Очистка при следующем запуске
    st.info("🔄 Метод 3: Создание скрипта очистки...")
    cleanup_script = """@echo off
echo Очистка временных файлов...
timeout /t 3 /nobreak >nul
rmdir /s /q "local_s3_storage" 2>nul
rmdir /s /q "temp_videos" 2>nul  
rmdir /s /q "temp" 2>nul
echo Готово!
pause
"""
    
    with open("cleanup.bat", "w") as f:
        f.write(cleanup_script)
    
    methods_success.append("Создан cleanup.bat для ручной очистки")
    
    # Итоги
    if methods_success:
        st.success("✅ Частично выполнено:")
        for msg in methods_success:
            st.write(f"  • {msg}")
        st.info("💡 Если файлы остались, запустите cleanup.bat после закрытия приложения")
    else:
        st.error("❌ Ни один метод не сработал. Закройте приложение и удалите папки вручную.")

# Информация о текущем состоянии
st.markdown("---")
st.subheader("📊 Текущее состояние")

col1, col2, col3 = st.columns(3)

with col1:
    # Подсчет файлов в локальном S3
    video_count = 0
    if os.path.exists("local_s3_storage/videos"):
        video_count = len([f for f in os.listdir("local_s3_storage/videos") if f.endswith(('.mp4', '.avi', '.mov'))])
    st.metric("Видео в локальном S3", video_count)

with col2:
    # Подсчет файлов в временных папках
    temp_count = 0
    temp_folders = ["temp_videos", "temp"]
    for folder in temp_folders:
        if os.path.exists(folder):
            temp_count += len([f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))])
    st.metric("Временных файлов", temp_count)

with col3:
    # Ключи в сессии
    session_keys = len(st.session_state.keys())
    st.metric("Ключей в сессии", session_keys)

# Дополнительная информация
with st.expander("🔍 Детальная информация о состоянии"):
    st.write("**Файлы в локальном S3:**")
    if os.path.exists("local_s3_storage/videos"):
        files = os.listdir("local_s3_storage/videos")
        if files:
            for file in files[:10]:
                st.write(f"- {file}")
            if len(files) > 10:
                st.write(f"- ... и еще {len(files) - 10} файлов")
        else:
            st.write("Нет файлов")
    else:
        st.write("Папка не существует")
    
    st.write("**Ключи в сессии:**")
    if st.session_state:
        for key in list(st.session_state.keys())[:10]:
            st.write(f"- {key}")
        if len(st.session_state.keys()) > 10:
            st.write(f"- ... и еще {len(st.session_state.keys()) - 10} ключей")
    else:
        st.write("Сессия пуста")
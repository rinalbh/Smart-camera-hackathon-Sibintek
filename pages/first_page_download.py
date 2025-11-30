import streamlit as st
from datetime import datetime
from utils.s3_client import upload_to_s3
from utils.ml_client import send_to_ml_service
from utils.db_client import save_upload_metadata, init_database

st.set_page_config(page_title="Загрузка видео", layout="centered")
st.title("📤 Загрузка видео")

# Инициализация состояния
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = []

# Инициализация БД
if 'db_initialized' not in st.session_state:
    if init_database():
        st.session_state.db_initialized = True
        st.success("✅ База данных инициализирована")
    else:
        st.error("❌ Ошибка инициализации базы данных")

# Информация о состоянии
st.sidebar.markdown("---")
st.sidebar.write(f"📊 Загружено файлов: **{len(st.session_state.uploaded_files)}**")

uploaded_file = st.file_uploader("Выберите видео файл", type=['mp4', 'avi', 'mov', 'mkv'])

if uploaded_file:
    # Показываем превью
    st.video(uploaded_file)
    
    # Проверяем, не загружали ли уже этот файл
    file_already_uploaded = any(
        f['name'] == uploaded_file.name and 
        f['size'] == uploaded_file.size 
        for f in st.session_state.uploaded_files
    )
    
    if not file_already_uploaded:
        if st.button("Загрузить видео", type="primary"):
            with st.spinner("Загружаем видео в облако..."):
                try:
                    # 1. Загружаем в S3 (Яндекс.Облако + локальная копия)
                    s3_url = upload_to_s3(uploaded_file)
                    
                    if s3_url.startswith("s3://"):
                        st.success("✅ Видео загружено в Яндекс.Облако")
                    else:
                        st.warning("⚠️ Видео сохранено локально (Яндекс.Облако недоступно)")
                    
                    # 2. Отправляем в ML сервис
                    with st.spinner("Анализируем видео..."):
                        ml_response = send_to_ml_service(s3_url)
                    
                    if ml_response.get("status") == "completed":
                        st.success("🎯 Анализ завершен")
                        
                        # 3. Сохраняем метаданные в БД
                        with st.spinner("Сохраняем в базу данных..."):
                            db_success = save_upload_metadata(
                                uploaded_file.name, 
                                s3_url, 
                                datetime.now(),
                                ml_response.get("metrics", {})
                            )
                        
                        if db_success:
                            st.balloons()
                            st.success("✅ Данные успешно сохранены в базу данных!")
                            
                            # Сохраняем информацию о файле
                            file_info = {
                                'name': uploaded_file.name,
                                'size': uploaded_file.size,
                                'type': uploaded_file.type,
                                's3_url': s3_url,
                                'timestamp': datetime.now(),
                                'processed': True,
                                'ml_status': 'completed'
                            }
                            st.session_state.uploaded_files.append(file_info)
                            
                            # Показываем метрики
                            if ml_response.get("metrics"):
                                with st.expander("📊 Просмотр метрик анализа"):
                                    st.json(ml_response["metrics"])
                        else:
                            st.error("❌ Ошибка сохранения в базу данных")
                            st.info("💡 Проверьте консоль для подробной информации об ошибке")
                    
                    else:
                        st.error("❌ Ошибка анализа видео")
                        
                except Exception as e:
                    st.error(f"❌ Ошибка при загрузке: {str(e)}")
    else:
        st.info("ℹ️ Это видео уже было загружено в текущей сессии")

# Показ деталей выбранного файла
if 'selected_file_details' in st.session_state:
    file_info = st.session_state.selected_file_details
    
    st.markdown("---")
    st.subheader(f"📄 Детали файла: {file_info['name']}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"**Размер:** {file_info['size']} байт")
        st.write(f"**Тип:** {file_info['type']}")
        st.write(f"**S3 URL:** `{file_info['s3_url']}`")
    
    with col2:
        status = "✅ Обработано" if file_info.get('processed') else "⏳ В очереди"
        st.write(f"**Статус:** {status}")
        timestamp = file_info['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        st.write(f"**Время загрузки:** {timestamp}")
    
    if st.button("← Назад к списку", key="back_to_list"):
        del st.session_state.selected_file_details
        st.rerun()

# Показываем историю загруженных файлов (только если не просматриваем детали)
if 'selected_file_details' not in st.session_state and st.session_state.uploaded_files:
    st.markdown("---")
    st.subheader("📋 История загрузок:")
    
    for i, file_info in enumerate(st.session_state.uploaded_files):
        col1, col2 = st.columns([3, 1])
        
        with col1:
            # Статус обработки
            if file_info.get('ml_status') == 'completed':
                status = "✅ Проанализировано"
            else:
                status = "⏳ В обработке"
            
            # Тип хранения
            storage = "☁️ Яндекс.Облако" if file_info['s3_url'].startswith("s3://") else "💾 Локально"
            
            timestamp = file_info['timestamp'].strftime('%H:%M:%S')
            
            st.write(f"**{file_info['name']}**")
            st.write(f"{status} | {storage} | {timestamp}")
        
        with col2:
            if st.button("Подробнее", key=f"details_{i}"):
                st.session_state.selected_file_details = file_info
                st.rerun()
        
        st.divider()

# Информация о загрузке
with st.expander("ℹ️ О загрузке видео"):
    st.markdown("""
    **Процесс загрузки:**
    1. Видео загружается в **Яндекс.Облако S3** 
    2. Создается **локальная резервная копия**
    3. Видео анализируется ML алгоритмами
    4. Результаты сохраняются в базу данных
    
    **Поддерживаемые форматы:** MP4, AVI, MOV, MKV
    
    **Метрики анализа:**
    - 👥 Количество людей в кадре
    - 📈 Эффективность рабочих процессов
    - ⚠️ Обнаруженные нарушения
    - 🎭 Активности сотрудников
    """)
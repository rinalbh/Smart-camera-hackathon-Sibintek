import streamlit as st
from datetime import datetime
from utils.s3_client import upload_to_s3
from utils.ml_client import process_video_with_black_boxes
from utils.db_client import init_database

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
                    # 1. Загружаем в S3
                    s3_url = upload_to_s3(uploaded_file)
                    
                    if s3_url.startswith("s3://"):
                        st.success("✅ Видео загружено в Яндекс.Облако")
                    else:
                        st.warning("⚠️ Видео сохранено локально")
                    
                    # 2. Обрабатываем через черные ящики
                    with st.spinner("Обрабатываем видео через ML сервисы..."):
                        ml_response = process_video_with_black_boxes(uploaded_file.name, s3_url)
                    
                    if ml_response.get("status") == "completed":
                        st.success("🎯 Обработка завершена!")
                        
                        # Сохраняем информацию о файле
                        file_info = {
                            'name': uploaded_file.name,
                            'size': uploaded_file.size,
                            'type': uploaded_file.type,
                            's3_url': s3_url,
                            'timestamp': datetime.now(),
                            'processed': True,
                            'fragments_count': ml_response.get("fragments_count", 0),
                            'analysis_count': ml_response.get("analysis_count", 0)
                        }
                        st.session_state.uploaded_files.append(file_info)
                        
                        st.balloons()
                        
                        # Показываем статистику
                        with st.expander("📊 Статистика обработки"):
                            st.write(f"**Таймфреймов создано:** {ml_response.get('fragments_count')}")
                            st.write(f"**Анализов выполнено:** {ml_response.get('analysis_count')}")
                            st.write(f"**Сообщение:** {ml_response.get('message')}")
                    
                    else:
                        st.error(f"❌ Ошибка обработки: {ml_response.get('message')}")
                        
                except Exception as e:
                    st.error(f"❌ Ошибка при загрузке: {str(e)}")
    else:
        st.info("ℹ️ Это видео уже было загружено в текущей сессии")

# Показываем историю загруженных файлов
if st.session_state.uploaded_files:
    st.markdown("---")
    st.subheader("📋 История загрузок:")
    
    for i, file_info in enumerate(st.session_state.uploaded_files):
        col1, col2 = st.columns([3, 1])
        
        with col1:
            status = "✅ Обработано" if file_info.get('processed') else "⏳ В обработке"
            storage = "☁️ Яндекс.Облако" if file_info['s3_url'].startswith("s3://") else "💾 Локально"
            timestamp = file_info['timestamp'].strftime('%H:%M:%S')
            
            st.write(f"**{file_info['name']}**")
            st.write(f"{status} | {storage} | {timestamp}")
            
            if file_info.get('fragments_count'):
                st.write(f"Таймфреймов: {file_info['fragments_count']}")
        
        with col2:
            if st.button("Подробнее", key=f"details_{i}"):
                st.session_state.selected_file_details = file_info
                st.rerun()
        
        st.divider()

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
        status = "✅ Обработано" if file_info.get('processed') else "⏳ В обработке"
        st.write(f"**Статус:** {status}")
        timestamp = file_info['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        st.write(f"**Время загрузки:** {timestamp}")
        if file_info.get('fragments_count'):
            st.write(f"**Таймфреймов создано:** {file_info['fragments_count']}")
        if file_info.get('analysis_count'):
            st.write(f"**Анализов выполнено:** {file_info['analysis_count']}")
    
    if st.button("← Назад к списку", key="back_to_list"):
        del st.session_state.selected_file_details
        st.rerun()

# Информация о загрузке
with st.expander("ℹ️ О загрузке видео"):
    st.markdown("""
    **Процесс загрузки:**
    1. Видео загружается в **Яндекс.Облако S3** 
    2. **Черный ящик Маши** создает 3 таймфрейма
    3. **Черный ящик Лёхи** анализирует каждый таймфрейм
    4. Результаты сохраняются в таблицы **Train** и **People**
    
    **Поддерживаемые форматы:** MP4, AVI, MOV, MKV
    
    **Создаваемые данные:**
    - **Train:** информация о поездах и камерах
    - **People:** информация о людях и их активностях
    """)
import streamlit as st
from datetime import datetime, timedelta
from utils.db_client import get_unique_filenames, get_videos_by_filename, get_people_data
from utils.s3_client import download_from_s3

st.set_page_config(page_title="Просмотр видео", layout="wide")
st.title("📺 Просмотр видео с фильтрами")

# Принудительно обновляем данные при загрузке страницы
if 'data_refreshed' not in st.session_state:
    st.session_state.data_refreshed = True
    st.rerun()

# Боковая панель с фильтрами
st.sidebar.header("🔍 Фильтры")

# Получаем список файлов из БД
filenames = get_unique_filenames()

# Отладочная информация
st.sidebar.write(f"📊 Найдено файлов в БД: {len(filenames)}")

if not filenames:
    st.sidebar.warning("📝 Нет загруженных файлов. Перейдите на страницу загрузки!")
else:
    st.sidebar.success(f"✅ Найдено файлов: {len(filenames)}")

# Фильтр: Файл
selected_filename = st.sidebar.selectbox(
    "📁 Файл",
    options=filenames,
    index=0 if filenames else None,
    help="Выберите файл для просмотра"
)

# Фильтр: Видео (таймфреймы)
videos = []
if selected_filename:
    videos = get_videos_by_filename(selected_filename)
    st.sidebar.write(f"🎬 Найдено таймфреймов: {len(videos)}")

# Создаем понятные названия для таймфреймов
video_options = []
if videos:
    for i, video in enumerate(videos):
        try:
            if isinstance(video['start_time'], str):
                start_time = datetime.strptime(video['start_time'], '%Y-%m-%d %H:%M:%S')
            else:
                start_time = video['start_time']
            
            if isinstance(video['end_time'], str):
                end_time = datetime.strptime(video['end_time'], '%Y-%m-%d %H:%M:%S')
            else:
                end_time = video['end_time']
                
            time_range = f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}"
        except:
            time_range = "время не указано"
        
        display_name = f"Таймфрейм {i+1} ({time_range})"
        video_options.append(display_name)

# Фильтр: Выбор видео
selected_video_index = None
if video_options:
    selected_video_index = st.sidebar.selectbox(
        "🎥 Видео",
        options=range(len(video_options)),
        format_func=lambda x: video_options[x],
        index=0,
        help="Выберите таймфрейм для просмотра"
    )

# Фильтр: Календарь
st.sidebar.subheader("📅 Диапазон дат")
col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input(
        "Начало",
        value=datetime.now() - timedelta(days=7),
        help="Начальная дата фильтра"
    )
with col2:
    end_date = st.date_input(
        "Конец", 
        value=datetime.now(),
        help="Конечная дата фильтра"
    )

# Кнопка применения фильтров
apply_filters = st.sidebar.button("🔍 Применить фильтры", type="primary")

# Кнопка обновления данных
if st.sidebar.button("🔄 Обновить данные"):
    st.session_state.data_refreshed = False
    st.rerun()

# Основная область контента
if selected_filename and videos and selected_video_index is not None:
    selected_video = videos[selected_video_index]
    
    st.header(f"📹 {selected_filename}")
    
    # Форматируем время для отображения
    try:
        if isinstance(selected_video['start_time'], str):
            start_time = datetime.strptime(selected_video['start_time'], '%Y-%m-%d %H:%M:%S')
        else:
            start_time = selected_video['start_time']
        
        if isinstance(selected_video['end_time'], str):
            end_time = datetime.strptime(selected_video['end_time'], '%Y-%m-%d %H:%M:%S')
        else:
            end_time = selected_video['end_time']
            
        time_display = f"{start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}"
        full_time_display = f"{start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
    except Exception as e:
        time_display = "время не указано"
        full_time_display = "время не указано"
    
    st.subheader(f"Таймфрейм: {time_display}")
    
    # Информация о видео
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📷 Камера", selected_video['camera_id'])
    with col2:
        st.metric("🚆 Номер поезда", selected_video['train_number'])
    with col3:
        st.metric("⚡ Статус", selected_video['status'])
    with col4:
        st.metric("🕒 Таймфрейм", time_display)
    
    # Показ видео
    st.markdown("---")
    st.subheader("🎥 Видео")
    
    with st.spinner("Загружаем видео..."):
        try:
            video_path = download_from_s3(selected_video['filepath_s3'])
            
            if video_path:
                st.video(video_path)
                st.success("✅ Видео загружено успешно")
            else:
                st.error("❌ Не удалось загрузить видео")
                st.info(f"Попытка загрузки из: {selected_video['filepath_s3']}")
        except Exception as e:
            st.error(f"❌ Ошибка при загрузке видео: {str(e)}")
    
    # Данные People для этого таймфрейма
    st.markdown("---")
    st.subheader("👥 Данные о людях")
    
    if apply_filters or st.session_state.get('filters_applied', False):
        st.session_state.filters_applied = True
        
        try:
            people_data = get_people_data(
                selected_filename, 
                selected_video['filepath_s3'],
                datetime.combine(start_date, datetime.min.time()),
                datetime.combine(end_date, datetime.max.time())
            )
            
            if people_data:
                # Статистика
                st.success(f"✅ Найдено записей: {len(people_data)}")
                
                col1, col2, col3, col4 = st.columns(4)
                
                zones = [p['zone'] for p in people_data]
                activities = [p['activity_status'] for p in people_data]
                
                with col1:
                    st.metric("📊 Всего записей", len(people_data))
                with col2:
                    st.metric("🎯 Уникальные зоны", len(set(zones)))
                with col3:
                    st.metric("🔧 Уникальные активности", len(set(activities)))
                with col4:
                    most_common_zone = max(set(zones), key=zones.count) if zones else "нет данных"
                    st.metric("📍 Частая зона", most_common_zone)
                
                # Детальная таблица
                st.markdown("---")
                st.subheader("📋 Детальные данные")
                
                for i, person in enumerate(people_data):
                    with st.expander(f"👤 Запись {i+1}: {person['activity_status']} ({person['zone']})", expanded=False):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write(f"**📷 Камера:** {person['camera_id']}")
                            st.write(f"**🚆 Номер поезда:** {person['train_number']}")
                            st.write(f"**⚡ Статус движения:** {person['status']}")
                        
                        with col2:
                            st.write(f"**🔧 Активность:** {person['activity_status']}")
                            st.write(f"**📍 Зона:** {person['zone']}")
                            try:
                                if isinstance(person['start_time'], str):
                                    person_start = datetime.strptime(person['start_time'], '%Y-%m-%d %H:%M:%S')
                                else:
                                    person_start = person['start_time']
                                
                                if isinstance(person['end_time'], str):
                                    person_end = datetime.strptime(person['end_time'], '%Y-%m-%d %H:%M:%S')
                                else:
                                    person_end = person['end_time']
                                    
                                person_time = f"{person_start.strftime('%H:%M')}-{person_end.strftime('%H:%M')}"
                            except:
                                person_time = "время не указано"
                            st.write(f"**🕒 Время:** {person_time}")
            else:
                st.info("📝 Нет данных People для выбранных фильтров")
                st.write("Попробуйте:")
                st.write("- Изменить диапазон дат")
                st.write("- Выбрать другой таймфрейм")
                st.write("- Проверить, что файл был обработан через черные ящики")
        
        except Exception as e:
            st.error(f"❌ Ошибка при загрузке данных People: {str(e)}")
    
    else:
        st.info("🔍 Нажмите 'Применить фильтры' для загрузки данных о людях")

else:
    if not filenames:
        st.warning("💡 Нет загруженных видео. Перейдите на страницу загрузки!")
    elif not videos:
        st.warning("💡 Для выбранного файла нет таймфреймов.")
        st.write("Возможные причины:")
        st.write("- Файл еще обрабатывается черными ящиками")
        st.write("- Ошибка при сохранении данных в БД")
        st.write("- Проблема с подключением к БД")
    else:
        st.info("📝 Выберите файл и видео для просмотра")

# Отладочная информация
with st.expander("🔧 Отладочная информация"):
    st.write("**📊 Файлы из БД:**", filenames)
    if selected_filename:
        st.write(f"**🎬 Видео для '{selected_filename}':**", len(videos))
        for i, video in enumerate(videos):
            st.write(f"  {i+1}. {video['filepath_s3']}")
    
    st.write("**💾 Сессия:**", list(st.session_state.keys()))

# Информация о системе
with st.expander("ℹ️ О системе"):
    st.markdown("""
    **Архитектура данных:**
    - **Таблица Train:** информация о поездах, камерах и временных промежутках
    - **Таблица People:** детальная информация о людях, их активностях и зонах
    
    **Фильтры:**
    - **Файл:** исходное загруженное видео
    - **Видео:** конкретный таймфрейм (3 таймфрейма на файл)
    - **Дата:** диапазон времени для фильтрации данных
    
    **Процесс обработки:**
    1. Черный ящик Маши создает 3 таймфрейма
    2. Черный ящик Лёхи анализирует каждый таймфрейм
    3. Данные сохраняются в соответствующие таблицы
    
    **Если данные не отображаются:**
    - Нажмите кнопку "Обновить данные"
    - Проверьте, что файл был успешно обработан
    - Убедитесь, что БД доступна
    """)
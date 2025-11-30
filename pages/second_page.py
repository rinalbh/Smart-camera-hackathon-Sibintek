import streamlit as st
from utils.db_client import get_videos_from_db, get_video_metrics
from utils.s3_client import download_from_s3
from datetime import datetime

st.set_page_config(page_title="Просмотр видео", layout="wide")
st.title("📺 Просмотр видео")

# Получаем список видео из БД
videos = get_videos_from_db()

if videos:
    st.header("Доступные видео:")
    
    for i, (filename, s3_url, upload_time) in enumerate(videos):
        col1, col2, col3 = st.columns([3, 1, 1])
        
        with col1:
            st.write(f"**{filename}**")
            
            # Форматируем время загрузки
            if isinstance(upload_time, str):
                try:
                    if 'T' in upload_time:
                        dt = datetime.fromisoformat(upload_time.replace('Z', ''))
                    else:
                        dt = datetime.strptime(upload_time, '%Y-%m-%d %H:%M:%S')
                    display_time = dt.strftime('%Y-%m-%d %H:%M')
                except:
                    display_time = upload_time
            elif isinstance(upload_time, datetime):
                display_time = upload_time.strftime('%Y-%m-%d %H:%M')
            else:
                display_time = str(upload_time)
                
            st.write(f"Загружено: {display_time}")
        
        with col2:
            if st.button("Смотреть", key=f"watch_{i}"):
                st.session_state.selected_video = {
                    'filename': filename,
                    's3_url': s3_url
                }
                st.rerun()
        
        with col3:
            if st.button("Метрики", key=f"metrics_{i}"):
                st.session_state.show_metrics_for = s3_url
                st.rerun()
        
        st.divider()

# Показ метрик для конкретного видео
if 'show_metrics_for' in st.session_state:
    s3_url = st.session_state.show_metrics_for
    metrics = get_video_metrics(s3_url)
    
    st.subheader("📊 Метрики анализа видео")
    
    if metrics:
        people, efficiency, violations, activities = metrics
        
        # Основные метрики
        col1, col2, col3 = st.columns(3)
        col1.metric("Количество людей", people)
        col2.metric("Эффективность", f"{efficiency*100:.1f}%")
        col3.metric("Нарушения", violations)
        
        # Детальная информация
        st.markdown("---")
        st.write(f"**Распознанные активности:** {activities}")
        
        # Визуализация эффективности
        st.subheader("📈 Визуализация показателей")
        progress_col1, progress_col2 = st.columns(2)
        
        with progress_col1:
            st.write("**Уровень эффективности:**")
            st.progress(efficiency)
            
        with progress_col2:
            st.write("**Заполненность кадра:**")
            people_fill = min(people / 15, 1.0)  # Предполагаем макс 15 человек
            st.progress(people_fill)
    
    else:
        st.warning("⚠️ Метрики анализа не найдены для этого видео")
    
    if st.button("← Назад к списку", key="back_from_metrics"):
        del st.session_state.show_metrics_for
        st.rerun()

# Показ выбранного видео
if 'selected_video' in st.session_state:
    video_data = st.session_state.selected_video
    
    # Заголовок с кнопкой назад
    col_title, col_back = st.columns([4, 1])
    with col_title:
        st.header(f"🎥 {video_data['filename']}")
    with col_back:
        if st.button("← Назад", key="back_from_video_top"):
            del st.session_state.selected_video
            st.rerun()
    
    # Загрузка и показ видео
    with st.spinner("Загружаем видео..."):
        video_path = download_from_s3(video_data['s3_url'])
        
        if video_path:
            # Показываем видео
            st.video(video_path)
            
            # Информация о видео
            st.markdown("---")
            st.subheader("📊 Аналитика видео")
            
            # Получаем и показываем метрики
            metrics = get_video_metrics(video_data['s3_url'])
            if metrics:
                people, efficiency, violations, activities = metrics
                
                # Основные метрики в карточках
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric(
                        label="👥 Людей в кадре", 
                        value=people,
                        help="Количество обнаруженных людей"
                    )
                
                with col2:
                    st.metric(
                        label="📈 Эффективность", 
                        value=f"{efficiency*100:.1f}%",
                        help="Общий показатель эффективности работы"
                    )
                
                with col3:
                    st.metric(
                        label="⚠️ Нарушения", 
                        value=violations,
                        help="Количество обнаруженных нарушений"
                    )
                
                with col4:
                    # Берем первую активность для краткого отображения
                    first_activity = activities.split(',')[0].strip() if activities else "N/A"
                    st.metric(
                        label="🎯 Основная активность", 
                        value=first_activity,
                        help="Основная распознанная активность"
                    )
                
                # Детальная информация об активностях
                st.markdown("**🎭 Все активности:**")
                if activities:
                    activities_list = [act.strip() for act in activities.split(',')]
                    for activity in activities_list:
                        st.write(f"- {activity}")
                else:
                    st.write("Активности не обнаружены")
                
                # Визуализация
                st.markdown("---")
                st.subheader("📊 Визуализация данных")
                
                # Простые индикаторы прогресса
                progress_col1, progress_col2, progress_col3 = st.columns(3)
                
                with progress_col1:
                    st.write("**Эффективность**")
                    st.progress(efficiency)
                    st.write(f"{efficiency*100:.1f}%")
                
                with progress_col2:
                    st.write("**Заполненность**")
                    occupancy = min(people / 15, 1.0)
                    st.progress(occupancy)
                    st.write(f"{people}/15 человек")
                
                with progress_col3:
                    st.write("**Безопасность**")
                    safety = max(1.0 - (violations / 8), 0.0)  # Предполагаем макс 8 нарушений
                    st.progress(safety)
                    st.write(f"{violations} нарушений")
            
            else:
                st.info("📝 Метрики анализа пока не доступны для этого видео")
            
            # Кнопка для возврата к списку внизу
            st.markdown("---")
            if st.button("← Вернуться к списку видео", key="back_from_video_bottom"):
                del st.session_state.selected_video
                st.rerun()
        else:
            st.error("❌ Не удалось загрузить видео")

else:
    st.info("📝 Пока нет загруженных видео. Перейдите на страницу загрузки!")

# Дополнительная информация
with st.expander("ℹ️ О просмотре видео"):
    st.markdown("""
    **Возможности страницы просмотра:**
    
    - 🎥 **Просмотр видео** - загрузка и воспроизведение видео из облачного хранилища
    - 📊 **Анализ метрик** - просмотр показателей эффективности, безопасности и активностей
    - 📈 **Визуализация** - графическое представление ключевых показателей
    
    **Отображаемые метрики:**
    - **Количество людей** - число обнаруженных людей в кадре
    - **Эффективность** - общий показатель продуктивности работы
    - **Нарушения** - количество обнаруженных нарушений правил
    - **Активности** - распознанные действия сотрудников
    
    Для начала работы загрузите видео на странице загрузки.
    """)
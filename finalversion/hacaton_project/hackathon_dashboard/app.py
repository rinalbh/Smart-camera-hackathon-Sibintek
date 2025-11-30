import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.db_client import get_people_data, get_train_data, get_people_stats, get_train_stats

# Настройка страницы
st.set_page_config(
    page_title="Industrial Analytics Dashboard",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Цветовая схема: темный фон, белый текст
COLORS = {
    'background': '#1E1E1E',
    'card_bg': '#2D2D2D', 
    'text': '#FFFFFF',
    'border': '#444444',
    'primary': '#FF6B6B',
    'secondary': '#4ECDC4',
    'accent': '#45B7D1',
    'yellow_zone': '#FFD700',
    'gray_null': '#888888'
}

# CSS для темной темы
st.markdown(f"""
<style>
    .main {{
        background-color: {COLORS['background']};
        color: {COLORS['text']};
    }}
    
    h1, h2, h3, h4, h5, h6 {{
        color: {COLORS['text']} !important;
    }}
    
    p, div, span, label {{
        color: {COLORS['text']} !important;
    }}
    
    [data-testid="stMetricValue"], [data-testid="stMetricLabel"] {{
        color: {COLORS['text']} !important;
    }}
    
    .chart-card {{
        background-color: {COLORS['card_bg']};
        border-radius: 15px;
        padding: 25px;
        margin: 15px 0;
        border: 1px solid {COLORS['border']};
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }}
    
    .metric-card {{
        background-color: {COLORS['card_bg']};
        border-radius: 12px;
        padding: 20px;
        border: 1px solid {COLORS['border']};
        text-align: center;
    }}
    
    .css-1d391kg {{
        background-color: {COLORS['card_bg']} !important;
    }}
    
    .stAlert {{
        background-color: {COLORS['card_bg']} !important;
        color: {COLORS['text']} !important;
        border: 1px solid {COLORS['border']} !important;
        border-radius: 10px;
    }}
    
    .dataframe {{
        background-color: {COLORS['card_bg']} !important;
        color: {COLORS['text']} !important;
    }}
    
    .stButton button {{
        background-color: {COLORS['primary']} !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 10px 20px !important;
        font-weight: 600 !important;
    }}
    
    .stButton button:hover {{
        background-color: #E55A5A !important;
        transform: translateY(-2px);
        transition: all 0.3s ease;
    }}
</style>
""", unsafe_allow_html=True)

# Заголовок
st.markdown(f"""
<div style='text-align: center; padding: 30px 0; background: linear-gradient(135deg, {COLORS['card_bg']}, {COLORS['background']}); border-radius: 20px; margin-bottom: 30px;'>
    <h1 style='color: {COLORS["text"]}; font-size: 3em; margin-bottom: 10px; font-weight: 800;'>🏭 INDUSTRIAL ANALYTICS</h1>
    <p style='color: {COLORS["text"]}; font-size: 1.3em; opacity: 0.9; font-weight: 300;'>Railway Maintenance Operations Monitoring</p>
</div>
""", unsafe_allow_html=True)

# Загрузка статистики
with st.spinner("🔄 Loading system statistics..."):
    people_stats = get_people_stats()
    train_stats = get_train_stats()

# Основные метрики
st.markdown("### 📊 OPERATIONAL OVERVIEW")
col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    total_people = people_stats['total_people'] if people_stats else 0
    st.markdown(f"""
    <div class="metric-card">
        <div style="font-size: 2em; color: {COLORS['primary']}; margin-bottom: 5px;">👥</div>
        <div style="font-size: 1.8em; font-weight: bold; color: {COLORS['text']};">{total_people}</div>
        <div style="font-size: 0.9em; color: {COLORS['text']}; opacity: 0.8;">PEOPLE ACTIVITIES</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    total_trains = train_stats['total_trains'] if train_stats else 0
    st.markdown(f"""
    <div class="metric-card">
        <div style="font-size: 2em; color: {COLORS['secondary']}; margin-bottom: 5px;">🚆</div>
        <div style="font-size: 1.8em; font-weight: bold; color: {COLORS['text']};">{total_trains}</div>
        <div style="font-size: 0.9em; color: {COLORS['text']}; opacity: 0.8;">TRAIN EVENTS</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    people_cameras = people_stats['unique_cameras'] if people_stats else 0
    st.markdown(f"""
    <div class="metric-card">
        <div style="font-size: 2em; color: {COLORS['accent']}; margin-bottom: 5px;">📷</div>
        <div style="font-size: 1.8em; font-weight: bold; color: {COLORS['text']};">{people_cameras}</div>
        <div style="font-size: 0.9em; color: {COLORS['text']}; opacity: 0.8;">ACTIVE CAMERAS</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    train_cameras = train_stats['unique_cameras'] if train_stats else 0
    st.markdown(f"""
    <div class="metric-card">
        <div style="font-size: 2em; color: {COLORS['primary']}; margin-bottom: 5px;">📹</div>
        <div style="font-size: 1.8em; font-weight: bold; color: {COLORS['text']};">{train_cameras}</div>
        <div style="font-size: 0.9em; color: {COLORS['text']}; opacity: 0.8;">TRAIN CAMERAS</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    zones = people_stats['unique_zones'] if people_stats else 0
    st.markdown(f"""
    <div class="metric-card">
        <div style="font-size: 2em; color: {COLORS['secondary']}; margin-bottom: 5px;">📍</div>
        <div style="font-size: 1.8em; font-weight: bold; color: {COLORS['text']};">{zones}</div>
        <div style="font-size: 0.9em; color: {COLORS['text']}; opacity: 0.8;">MONITORED ZONES</div>
    </div>
    """, unsafe_allow_html=True)

with col6:
    unique_trains = train_stats['unique_trains'] if train_stats else 0
    st.markdown(f"""
    <div class="metric-card">
        <div style="font-size: 2em; color: {COLORS['accent']}; margin-bottom: 5px;">🔢</div>
        <div style="font-size: 1.8em; font-weight: bold; color: {COLORS['text']};">{unique_trains}</div>
        <div style="font-size: 0.9em; color: {COLORS['text']}; opacity: 0.8;">TRAIN NUMBERS</div>
    </div>
    """, unsafe_allow_html=True)

# Кнопки загрузки данных
st.markdown("---")
st.markdown("### 📥 DATA MANAGEMENT")

col1, col2, col3 = st.columns([1, 1, 2])

with col1:
    if st.button("🔄 LOAD PEOPLE DATA", use_container_width=True, type="primary"):
        with st.spinner("Loading people analytics..."):
            people_df = get_people_data()
            if people_df is not None and not people_df.empty:
                st.session_state.people_data = people_df
                st.success("✅ People data loaded successfully!")
            else:
                st.error("❌ Failed to load people data")

with col2:
    if st.button("🔄 LOAD TRAIN DATA", use_container_width=True, type="primary"):
        with st.spinner("Loading train operations..."):
            train_df = get_train_data()
            if train_df is not None and not train_df.empty:
                st.session_state.train_data = train_df
                st.success("✅ Train data loaded successfully!")
            else:
                st.error("❌ Failed to load train data")

with col3:
    if st.button("🔄 LOAD ALL DATA", use_container_width=True):
        with st.spinner("Loading complete dataset..."):
            people_df = get_people_data()
            train_df = get_train_data()
            if people_df is not None and not people_df.empty:
                st.session_state.people_data = people_df
            if train_df is not None and not train_df.empty:
                st.session_state.train_data = train_df
            st.success("✅ All operational data loaded!")

st.markdown("---")

# Визуализации
st.markdown("### 📈 OPERATIONAL ANALYTICS")

# Проверка наличия данных
people_loaded = 'people_data' in st.session_state
train_loaded = 'train_data' in st.session_state

if not people_loaded and not train_loaded:
    st.info("""
    🎯 **READY FOR ANALYSIS**
    
    Click the data loading buttons above to visualize:
    - Maintenance crew activity patterns
    - Train operations timeline  
    - Work zone distribution
    - Operational insights
    """)
else:
    # PEOPLE ANALYTICS
    if people_loaded:
        st.markdown("#### 👥 MAINTENANCE CREW ACTIVITY")
        people_df = st.session_state.people_data
        
        col1, col2 = st.columns(2)
        
        with col1:
            with st.container():
                st.markdown('<div class="chart-card">', unsafe_allow_html=True)
                if 'zone' in people_df.columns:
                    # Добавляем данные о зонах с NULL значениями
                    zone_data = people_df.copy()
                    zone_data['zone_display'] = zone_data['zone'].fillna('No Zone')
                    
                    zone_counts = zone_data['zone_display'].value_counts()
                    
                    # Специальные цвета для зон
                    zone_colors = {
                        'Yellow zone': COLORS['yellow_zone'],
                        'No Zone': COLORS['gray_null']
                    }
                    
                    fig = px.pie(
                        values=zone_counts.values, 
                        names=zone_counts.index, 
                        title="📍 Activity Location Distribution",
                        color=zone_counts.index,
                        color_discrete_map=zone_colors
                    )
                    fig.update_layout(
                        paper_bgcolor=COLORS['background'],
                        font=dict(color=COLORS['text'], size=12),
                        height=450,
                        showlegend=True,
                        legend=dict(
                            orientation="v", 
                            yanchor="middle", 
                            y=0.5, 
                            xanchor="left", 
                            x=1.1,
                            font=dict(size=11)
                        )
                    )
                    fig.update_traces(
                        textposition='inside', 
                        textinfo='percent+label',
                        textfont=dict(size=11, color=COLORS['text']),
                        marker=dict(line=dict(color=COLORS['border'], width=1))
                    )
                    st.plotly_chart(fig, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            with st.container():
                st.markdown('<div class="chart-card">', unsafe_allow_html=True)
                if 'status' in people_df.columns:
                    # Подсчитываем количество действий по типам с русскими названиями
                    activity_counts = people_df['status'].value_counts()
                    
                    # Цвета для разных типов активности
                    activity_colors = {
                        'Осматривает': COLORS['primary'],    # Красный для осмотра
                        'Идет': COLORS['secondary'],         # Голубой для движения
                        'Стоит': COLORS['accent']           # Синий для стояния
                    }
                    
                    fig = px.bar(
                        x=activity_counts.index, 
                        y=activity_counts.values,
                        title="📊 Activity Type Distribution",
                        color=activity_counts.index,
                        color_discrete_map=activity_colors,
                        labels={'x': 'Activity Type', 'y': 'Number of Records'},
                        text=activity_counts.values
                    )
                    fig.update_layout(
                        paper_bgcolor=COLORS['background'],
                        plot_bgcolor=COLORS['background'],
                        font=dict(color=COLORS['text'], size=12),
                        xaxis=dict(
                            color=COLORS['text'], 
                            gridcolor=COLORS['border'],
                            title_font=dict(size=12)
                        ),
                        yaxis=dict(
                            color=COLORS['text'], 
                            gridcolor=COLORS['border'],
                            title_font=dict(size=12)
                        ),
                        height=450,
                        showlegend=False
                    )
                    fig.update_traces(
                        texttemplate='%{text}',
                        textposition='outside',
                        marker=dict(line=dict(color=COLORS['border'], width=1))
                    )
                    st.plotly_chart(fig, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

    # TRAIN ANALYTICS
    if train_loaded:
        st.markdown("#### 🚆 TRAIN OPERATIONS TIMELINE")
        train_df = st.session_state.train_data
        
        col1, col2 = st.columns(2)
        
        with col1:
            with st.container():
                st.markdown('<div class="chart-card">', unsafe_allow_html=True)
                if 'arrival_sec' in train_df.columns:
                    # Анализ времени прибытия поездов с лучшим форматированием
                    arrival_times = train_df['arrival_sec'].dropna()
                    if not arrival_times.empty:
                        # Создаем более информативную гистограмму
                        fig = px.histogram(
                            arrival_times, 
                            title="⏱️ Train Arrival Time Distribution",
                            color_discrete_sequence=[COLORS['accent']],
                            nbins=8,
                            labels={'value': 'Time from Video Start (seconds)', 'count': 'Number of Trains'},
                            opacity=0.8
                        )
                        
                        fig.update_layout(
                            paper_bgcolor=COLORS['background'],
                            plot_bgcolor=COLORS['background'],
                            font=dict(color=COLORS['text'], size=12),
                            xaxis=dict(
                                color=COLORS['text'], 
                                gridcolor=COLORS['border'],
                                title_font=dict(size=11)
                            ),
                            yaxis=dict(
                                color=COLORS['text'], 
                                gridcolor=COLORS['border'],
                                title_font=dict(size=11)
                            ),
                            height=450,
                            bargap=0.1
                        )
                        fig.update_traces(
                            marker=dict(
                                line=dict(color=COLORS['border'], width=1)
                            )
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No arrival time data available")
                else:
                    st.info("Arrival data not available")
                st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            with st.container():
                st.markdown('<div class="chart-card">', unsafe_allow_html=True)
                if people_loaded and 'person_id' in people_df.columns:
                    # Анализ активности по персонам с лучшим форматированием
                    person_activity = people_df['person_id'].value_counts().head(8)
                    
                    # Создаем цветовую градацию для самых активных
                    colors = px.colors.sequential.Viridis[:len(person_activity)]
                    
                    fig = px.bar(
                        x=person_activity.values,
                        y=[f"Person {pid}" for pid in person_activity.index],
                        orientation='h',
                        title="👤 Most Active Maintenance Personnel",
                        color=person_activity.values,
                        color_continuous_scale='Viridis',
                        labels={'x': 'Number of Activities', 'y': 'Person ID'},
                        text=person_activity.values
                    )
                    
                    fig.update_layout(
                        paper_bgcolor=COLORS['background'],
                        plot_bgcolor=COLORS['background'],
                        font=dict(color=COLORS['text'], size=12),
                        xaxis=dict(
                            color=COLORS['text'], 
                            gridcolor=COLORS['border'],
                            title_font=dict(size=11)
                        ),
                        yaxis=dict(
                            color=COLORS['text'],
                            title_font=dict(size=11)
                        ),
                        height=450,
                        showlegend=False
                    )
                    fig.update_traces(
                        texttemplate='%{text}',
                        textposition='outside',
                        textfont=dict(color=COLORS['text'], size=10),
                        marker=dict(line=dict(color=COLORS['border'], width=1))
                    )
                    fig.update_coloraxes(showscale=False)
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Person activity data not available")
                st.markdown('</div>', unsafe_allow_html=True)

    # ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ
    if people_loaded or train_loaded:
        st.markdown("#### 📋 DETAILED OPERATIONAL DATA")
        
        if people_loaded:
            with st.expander("👥 MAINTENANCE CREW ACTIVITY LOG", expanded=False):
                # Показываем только ключевые колонки для лучшей читаемости
                display_cols = ['person_id', 'start_dt', 'end_dt', 'status', 'zone']
                available_cols = [col for col in display_cols if col in people_df.columns]
                st.dataframe(people_df[available_cols], use_container_width=True, height=300)
        
        if train_loaded:
            with st.expander("🚆 TRAIN OPERATIONS LOG", expanded=False):
                # Показываем только ключевые колонки
                display_cols = ['train_id', 'arrival_dt', 'stopped', 'train_number']
                available_cols = [col for col in display_cols if col in train_df.columns]
                st.dataframe(train_df[available_cols], use_container_width=True, height=300)

# Боковая панель
with st.sidebar:
    st.markdown(f"""
    <div style='text-align: center; padding: 20px 0;'>
        <h2 style='color: {COLORS['text']}; margin-bottom: 5px;'>🏭</h2>
        <h3 style='color: {COLORS['text']}; margin: 0;'>RAILWAY AI</h3>
        <p style='color: {COLORS['text']}; opacity: 0.7; font-size: 0.9em;'>Maintenance Monitoring</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("### 🚀 Quick Actions")
    if st.button("🔄 REFRESH DASHBOARD", use_container_width=True, type="primary"):
        st.rerun()
    
    if st.button("📊 SYSTEM HEALTH", use_container_width=True):
        from utils.db_client import test_connection
        success, message = test_connection()
        if success:
            st.success(message)
        else:
            st.error(message)
    
    st.markdown("---")
    st.markdown("### 📈 Current Statistics")
    
    if people_stats:
        st.metric("Activity Records", people_stats['total_people'])
        st.metric("Active Cameras", people_stats['unique_cameras'])
        st.metric("Monitored Zones", people_stats['unique_zones'])
    
    if train_stats:
        st.metric("Train Events", train_stats['total_trains'])
        st.metric("Train Cameras", train_stats['unique_cameras'])
        st.metric("Train Numbers", train_stats['unique_trains'])
    
    st.markdown("---")
    st.markdown("### 🔧 System Info")
    st.info("""
    **Railway Maintenance Analytics**
    
    Monitoring:
    - Crew activity tracking
    - Train operations  
    - Work zone safety
    - Maintenance efficiency
    - Video surveillance
    """)

# Футер
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: {COLORS['text']}; opacity: 0.7; padding: 30px;'>
    <div style='font-size: 0.9em;'>Railway Maintenance Dashboard © 2025</div>
    <div style='font-size: 0.8em; margin-top: 5px;'>Operational Intelligence • Safety Monitoring • Maintenance Analytics</div>
</div>
""", unsafe_allow_html=True)
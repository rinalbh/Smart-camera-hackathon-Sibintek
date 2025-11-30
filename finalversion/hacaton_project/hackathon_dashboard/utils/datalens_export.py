import pandas as pd
from .db_client import get_people_data, get_train_data

def export_to_csv_for_datalens():
    """Экспортирует данные в CSV для Datalens"""
    
    # Получаем данные из ClickHouse
    people_df = get_people_data()
    train_df = get_train_data()
    
    if people_df is not None and not people_df.empty:
        # Сохраняем в CSV
        people_df.to_csv('datalens_people.csv', index=False)
        print("✅ datalens_people.csv создан")
    else:
        print("⚠️ Нет данных people для экспорта")
    
    if train_df is not None and not train_df.empty:
        train_df.to_csv('datalens_train.csv', index=False)
        print("✅ datalens_train.csv создан")
    else:
        print("⚠️ Нет данных train для экспорта")

def create_aggregated_data():
    """Создает агрегированные данные для Datalens"""
    people_df = get_people_data()
    
    if people_df is not None and not people_df.empty:
        # Агрегация по дням/часам
        people_df['start_dt'] = pd.to_datetime(people_df['start_dt'])
        people_df['date'] = people_df['start_dt'].dt.date
        people_df['hour'] = people_df['start_dt'].dt.hour
        
        daily_stats = people_df.groupby('date').agg({
            'person_id': 'count',
            'video_id': 'nunique'
        }).reset_index()
        
        daily_stats.columns = ['date', 'people_count', 'unique_cameras']
        daily_stats.to_csv('datalens_daily_stats.csv', index=False)
        return daily_stats
    return None
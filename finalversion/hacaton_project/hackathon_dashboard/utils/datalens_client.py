import streamlit as st
import pandas as pd
from yandex_datalens import Datalens
import os

def get_datalens_client():
    """
    Инициализация клиента Datalens
    """
    try:
        # Настройки из environment variables или secrets
        token = st.secrets.get("DATALENS_TOKEN", os.getenv('DATALENS_TOKEN'))
        folder_id = st.secrets.get("DATALENS_FOLDER_ID", os.getenv('DATALENS_FOLDER_ID'))
        
        if not token:
            st.error("Datalens token not configured")
            return None
            
        client = Datalens(token=token, folder_id=folder_id)
        return client
    except Exception as e:
        st.error(f"Failed to initialize Datalens client: {e}")
        return None

def create_industrial_dashboard():
    """
    Создание или получение дашборда Industrial Analytics
    """
    client = get_datalens_client()
    if not client:
        return None
    
    try:
        # Поиск существующего дашборда
        dashboards = client.dashboards.list()
        industrial_dash = None
        
        for dash in dashboards:
            if 'industrial' in dash.name.lower() or 'analytics' in dash.name.lower():
                industrial_dash = dash
                break
        
        if not industrial_dash:
            # Создание нового дашборда
            industrial_dash = client.dashboards.create(
                name="Industrial Analytics Dashboard",
                description="Real-time monitoring of people and train operations"
            )
        
        return industrial_dash
        
    except Exception as e:
        st.error(f"Failed to create/get dashboard: {e}")
        return None

def get_datalens_embed_url(dashboard_id):
    """
    Генерация embed URL для дашборда
    """
    client = get_datalens_client()
    if not client:
        return None
    
    try:
        embed_url = client.dashboards.get_embed_url(dashboard_id)
        return embed_url
    except Exception as e:
        st.error(f"Failed to get embed URL: {e}")
        return None
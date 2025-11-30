import clickhouse_connect

CLICKHOUSE_CONFIG = {
    'host': 'iqydclkqtr.us-east1.gcp.clickhouse.cloud',
    'port': 8443,
    'username': 'default',
    'password': 'zrh0w4W_gzVFO',
    'secure': True
}

print("🔌 Тестируем подключение к ClickHouse...")
print(f"Хост: {CLICKHOUSE_CONFIG['host']}")
print(f"Порт: {CLICKHOUSE_CONFIG['port']}")

try:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_CONFIG['host'],
        port=CLICKHOUSE_CONFIG['port'],
        username=CLICKHOUSE_CONFIG['username'],
        password=CLICKHOUSE_CONFIG['password'],
        secure=CLICKHOUSE_CONFIG['secure']
    )
    
    print("✅ ClickHouse подключен!")
    
    # Проверяем существующие таблицы
    result = client.query("SHOW TABLES")
    tables = [row[0] for row in result.result_rows]
    print(f"📊 Таблицы в базе: {tables}")
    
    # Если есть таблицы - покажем структуру
    for table in tables:
        print(f"\n🔍 Структура таблицы {table}:")
        desc_result = client.query(f"DESCRIBE {table}")
        for column in desc_result.result_rows:
            print(f"   {column[0]} | {column[1]} | {column[2]}")
            
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")

input("\nНажми Enter для выхода...")
import pandas as pd
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '../data/app.db')

def run_analytics():
    if not os.path.exists(DB_PATH):
        print("База данных не найдена. Сначала запусти job2.")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("SELECT * FROM events", conn)
        if df.empty:
            print("Таблица events пуста.")
            return

        # Аналитика через Pandas
        summary = df.groupby('date').agg({
            'flux': ['mean', 'max', 'count']
        }).reset_index()
        summary.columns = ['date', 'avg_flux', 'max_flux', 'event_count']

        summary.to_sql('daily_summary', conn, if_exists='replace', index=False)
        print("Аналитика готова:")
        print(summary)
        
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_analytics()
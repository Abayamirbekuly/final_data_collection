import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '../data/app.db')

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    curr = conn.cursor()
    # Таблица для сырых/очищенных данных
    curr.execute('''CREATE TABLE IF NOT EXISTS events 
                 (time_tag TEXT, flux REAL, energy TEXT, date TEXT)''')
    # Таблица для аналитики
    curr.execute('''CREATE TABLE IF NOT EXISTS daily_summary 
                 (date TEXT, avg_flux REAL, max_flux REAL, count INTEGER)''')
    conn.commit()
    conn.close()
    print("Database initialized!")

if __name__ == "__main__":
    init_db()
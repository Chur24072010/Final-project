import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WeatherScraper:
    def __init__(self, url):
        self.url = url

    def fetch_data(self):
        try:
            logging.info("Отримання даних з сайту")
            response = requests.get(self.url)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            logging.error(f"Помилка підключення до сайту: {e}")
            return None

    def parse_weather(self):
        soup = self.fetch_data()
        if not soup:
            return []

        weather_data = []
        forecast = soup.find_all('div', class_='forecast-day')
        for day in forecast[:10]:
            date = day.find('span', class_='date-class').text.strip()
            temperature = day.find('span', class_='temp-class').text.strip()
            precipitation = 'Так' if 'дощ' in day.text.lower() else 'Ні'
            wind_speed = day.find('span', class_='wind-speed-class').text.strip()
            wind_direction = day.find('span', class_='wind-dir-class').text.strip()

            weather_data.append({
                'date': date,
                'temperature': temperature,
                'precipitation': precipitation,
                'wind_speed': wind_speed,
                'wind_direction': wind_direction
            })
        logging.info("Дані успішно отримані")
        return weather_data

import sqlite3

class WeatherDatabase:
    def __init__(self, db_name='weather_data.db'):
        self.db_name = db_name
        self.conn = None

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.create_table()
            logging.info("Підключення до бази даних успішне")
        except sqlite3.Error as e:
            logging.error(f"Помилка при підключенні до бази даних: {e}")

    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            temperature TEXT,
            precipitation TEXT,
            wind_speed TEXT,
            wind_direction TEXT
        );
        """
        self.conn.execute(query)
        self.conn.commit()

    def insert_data(self, data):
        query = """
        INSERT INTO weather (date, temperature, precipitation, wind_speed, wind_direction)
        VALUES (?, ?, ?, ?, ?);
        """
        self.conn.executemany(query, [
            (item['date'], item['temperature'], item['precipitation'], item['wind_speed'], item['wind_direction'])
            for item in data
        ])
        self.conn.commit()
        logging.info("Дані успішно збережені")

class WeatherQuery:
    def __init__(self, db_name='weather_data.db'):
        self.conn = sqlite3.connect(db_name)

    def get_by_date(self, date):
        query = "SELECT * FROM weather WHERE date = ?"
        return self.conn.execute(query, (date,)).fetchall()

    def get_min_temperature(self):
        query = "SELECT * FROM weather ORDER BY temperature ASC LIMIT 1"
        return self.conn.execute(query).fetchone()

    def get_max_temperature(self):
        query = "SELECT * FROM weather ORDER BY temperature DESC LIMIT 1"
        return self.conn.execute(query).fetchone()

if __name__ == "__main__":

    scraper = WeatherScraper("https://example-weather-site.com/forecast")
    weather_data = scraper.parse_weather()


    db = WeatherDatabase()
    db.connect()
    db.insert_data(weather_data)


    query = WeatherQuery()
    by_date = query.get_by_date('2024-12-01')
    min_temp = query.get_min_temperature()
    max_temp = query.get_max_temperature()


    for row in by_date:
        weather = DateWeather(*row[1:])
        print(weather)

    print("Найменша температура:", DateWeather(*min_temp[1:]))
    print("Найбільша температура:", DateWeather(*max_temp[1:]))

from kafka import KafkaProducer, KafkaConsumer
import json
import time
import pandas as pd
from datetime import datetime, timedelta
import random


# Kafka Topics
BUILDING_SENSORS_TOPIC = "building_sensors_olena"
TEMPERATURE_ALERTS_TOPIC = "temperature_alerts_olena"
HUMIDITY_ALERTS_TOPIC = "humidity_alerts_olena"
ALERTS_TOPIC = "alerts_olena"

# Load alert conditions from CSV
alerts_conditions_df = pd.read_csv("/home/obogol/kafka_2.13-3.9.0/alerts_conditions.csv")


class SensorDataProcessor:
    def __init__(self, input_topic, temp_alert_topic, hum_alert_topic):
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.temp_alert_topic = temp_alert_topic
        self.hum_alert_topic = hum_alert_topic
        self.window_size = timedelta(minutes=1)
        self.sliding_interval = timedelta(seconds=30)
        self.window_data = []

    def process_data(self):
        print("Kafka Consumer запущено... Очікуємо повідомлення")
        for message in self.consumer:
            data = message.value
            self.window_data.append(data)
            self.clean_old_data()

            print(f"Current window data: {self.window_data}")  # Додаємо для налагодження

            if len(self.window_data) > 1:
                t_avg = sum(d['temperature'] for d in self.window_data) / len(self.window_data)
                h_avg = sum(d['humidity'] for d in self.window_data) / len(self.window_data)
                window_start = (datetime.utcnow() - self.window_size).isoformat()
                window_end = datetime.utcnow().isoformat()

                print(f"Sliding Window | Avg Temp: {t_avg}, Avg Humidity: {h_avg}")  # Додаємо вивід

                alert = self.get_alert({
                    "window_start": window_start,
                    "window_end": window_end,
                    "t_avg": t_avg,
                    "h_avg": h_avg
                })

                if alert:
                    self.producer.send(ALERTS_TOPIC, alert)
                    print(f"Sent alert: {alert}")

    def get_alert(self, sensor_data):
        for _, row in alerts_conditions_df.iterrows():
            if ((row['humidity_min'] <= sensor_data['h_avg'] <= row['humidity_max']) or (
                    row['humidity_min'] == -999 and row['humidity_max'] == -999)) and \
                    ((row['temperature_min'] <= sensor_data['t_avg'] <= row['temperature_max']) or (
                            row['temperature_min'] == -999 and row['temperature_max'] == -999)):
                return {
                    "window": {
                        "start": sensor_data['window_start'],
                        "end": sensor_data['window_end']
                    },
                    "t_avg": sensor_data['t_avg'],
                    "h_avg": sensor_data['h_avg'],
                    "code": str(row['code']),
                    "message": row['message'],
                    "timestamp": datetime.utcnow().isoformat()
                }
        # Якщо жоден поріг не перевищено
        print(f"No alert triggered | T_avg={sensor_data['t_avg']}, H_avg={sensor_data['h_avg']}")
        return None

    def clean_old_data(self):
        now = datetime.utcnow()
        self.window_data = [d for d in self.window_data if
                            datetime.fromisoformat(d['timestamp']) >= now - self.window_size]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["produce", "process", "read", "list_topics"], help="Mode to run: produce, process, read, or list_topics")
    args = parser.parse_args()

    if args.mode == "produce":
        producer = SensorDataProducer(BUILDING_SENSORS_TOPIC)
        producer.send_data()

    elif args.mode == "process":
        processor = SensorDataProcessor(BUILDING_SENSORS_TOPIC, TEMPERATURE_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC)
        processor.process_data()

    elif args.mode == "read":
        reader = AlertReader([TEMPERATURE_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC])
        reader.read_alerts()

    elif args.mode == "list_topics":
        list_kafka_topics()

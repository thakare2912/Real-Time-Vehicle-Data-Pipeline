
import json
import logging
import os
import random
import time
from datetime import datetime

from confluent_kafka import Producer
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

# Kafka config
kafka_bootstrap_servers = "localhost:29092"
kafka_topic_vehicle = "vehicle-info"

CAR_NAMES = [
    'Maruti Swift', 'Hyundai i20', 'Tata Nexon',
    'Mahindra XUV300', 'Honda City', 'Toyota Fortuner',
    'Kia Seltos', 'Suzuki Baleno'
]

BIKE_NAMES = [
    'Royal Enfield Classic', 'Bajaj Pulsar', 'TVS Apache',
    'Hero Splendor', 'Honda Activa', 'Yamaha FZ',
    'Suzuki Gixxer', 'KTM Duke'
]

STATE_CODES = [
    'MH', 'DL', 'KA', 'TN', 'GJ', 'RJ',
    'UP', 'MP', 'WB', 'PB', 'HR', 'CG'
]


class VehicleDataProducer:
    def __init__(self, bootstrap_servers=kafka_bootstrap_servers, topic=kafka_topic_vehicle):
        self.logger = logger
        self.topic = topic

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "vehicle-data-producer-0",
        }

        try:
            self.producer = Producer(conf)
            self.logger.info(f"Kafka producer initialized. Sending to {bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka producer: {e}")
            raise

    def generate_number_plate(self):
        state_code = random.choice(STATE_CODES)
        district_code = random.choice(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '12', '14', '20'])
        series = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
        number = random.randint(1000, 9999)
        return f"{state_code} {district_code} {series} {number}"

    def generate_phone_number(self):
        prefix = random.choice(['7', '8', '9'])
        number = ''.join([str(random.randint(0, 9)) for _ in range(8)])
        return prefix + number

    def generate_vehicle_data(self):
        vehicle_type = random.choice(['car', 'bike'])
        vehicle_name = random.choice(CAR_NAMES) if vehicle_type == 'car' else random.choice(BIKE_NAMES)
        speed = round(random.uniform(20, 200), 2)
        fine = 1000 if speed > 150 else 0

        data = {
            'vehicle_id': f'V{random.randint(1000, 9999)}',
            'number_plate': self.generate_number_plate(),
            'vehicle_type': vehicle_type,
            'vehicle_name': vehicle_name,
            'speed': speed,
            'location_lat': round(random.uniform(18.50, 19.00), 6) ,
            'location_lon': round(random.uniform(73.70, 74.00), 6)
            ,
            'user_id': random.randint(1, 1000),
            'phone_number': self.generate_phone_number(),
            'car_life_years': random.randint(1, 15),
            'price_of_vechical': round(random.uniform(100000, 2000000), 2),
            'fine': fine,
            'timestamp': time.time()
        }

        return data

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed for message: {msg}: {err}")
        else:
            self.logger.info(f"Message delivered successfully to {msg.topic()} [{msg.partition()}]")

    def produce_vehicle_data(self):
        data = self.generate_vehicle_data()
        try:
            self.producer.produce(
                topic=self.topic,
                key=None,
                value=json.dumps(data),
                callback=self.delivery_report
            )
            self.producer.poll(0)
            self.logger.info(f"Sent: {data}")

            if data['fine'] > 0:
                self.logger.warning(
                    f" WARNING: Vehicle {data['vehicle_id']} overspeeding at {data['speed']} km/h — Fine ₹{data['fine']} sent to phone {data['phone_number']}"
                )

        except Exception as e:
            self.logger.error(f"Failed to produce vehicle data: {e}")

    def run(self):
        self.logger.info(f"Starting continuous vehicle data production to topic {self.topic}")
        while True:
            self.produce_vehicle_data()
            time.sleep(1)


def main():
    try:
        logger.info(f"Starting Vehicle Data Producer")
        producer = VehicleDataProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            topic=kafka_topic_vehicle
        )
        producer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")


if __name__ == "__main__":
    main()

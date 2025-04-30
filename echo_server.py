from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from psycopg2 import pool

import psycopg2
import logging
import socket
import pytz
import os

# === Setup database connection === #
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
connection_string = os.getenv('DATABASE_URL')
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, connection_string)

# === Constants for convertion === #
GALLONS_PER_LITTER = 0.264172
MOISTURE_TO_RH = 1.5
KWH_PER_WH = 0.001
PST = pytz.timezone("America/Los_Angeles")

# === Using simulated time because Dataniz usage expired, so we are using payload data from April 18th === #
USE_SIMULATED_TIME = True
SIMULATED_NOW = datetime.fromisoformat("2025-04-18T21:51:05+00:00")

def get_now():
    return SIMULATED_NOW if USE_SIMULATED_TIME else datetime.now()

def three_hours_ago():
    return get_now() - timedelta(hours=3)

def convert_to_pst(timestamp: datetime) -> datetime:
    return timestamp.astimezone(PST)

# === Caching using hashmaps for easy and fast look up === #
fridge_data = {}
dishwasher_data = {}

def load_sensor_data():
    global fridge_data, dishwasher_data
    conn = connection_pool.getconn()
    cur = conn.cursor()

    try:
        # Load fridge data and store in fridge map
        cur.execute("SELECT payload, time FROM fridge_data_virtual")
        fridge_data = {time: payload for payload, time in cur.fetchall()}

        # Load dishwasher data and store
        cur.execute("SELECT payload, time FROM dishwasher_data_virtual")
        dishwasher_data = {time: payload for payload, time in cur.fetchall()}

        logger.info("Sensor data loaded into memory")

    except Exception as e:
        logger.error(f"Error loading sensor data: {e}")

    finally:
        cur.close()
        connection_pool.putconn(conn)

# === Query Handlers ==== #
def process_fridge_moisture_query():
    total = 0
    count = 0
    cutoff = three_hours_ago()

    for time, payload in fridge_data.items():
        if time >= cutoff:
            moisture = payload.get("DHT11 - fridge_moisture_sensor")
            if moisture:
                total += float(moisture) * MOISTURE_TO_RH
                count += 1

    if count == 0:
        return "No recent fridge moisture data"
    return f"Average fridge moisture: {total / count:.1f}% RH"

def process_dishwasher_water_query():
    total_gallons = 0
    count = 0
    cutoff = three_hours_ago()

    for time, payload in dishwasher_data.items():
        if time >= cutoff:
            water = payload.get("dishwasher_water_flow")
            if water:
                total_gallons += float(water) * GALLONS_PER_LITTER
                count += 1

    if count == 0:
        return "No recent dishwasher water data"
    return f"Average dishwasher water: {total_gallons / count:.2f} gallons"

def process_electricity_comparison_query():
    power_consumption = defaultdict(float)
    cutoff = three_hours_ago()

    for time, payload in fridge_data.items():
        if time >= cutoff:
            power = payload.get("fridge_ammeter")
            if power:
                power_consumption["fridge"] += float(power)

    for time, payload in dishwasher_data.items():
        if time >= cutoff:
            power = payload.get("dishwasher_ammeter")
            if power:
                power_consumption["dishwasher"] += float(power)

    if not power_consumption:
        return "No electricity data available"

    top_device = max(power_consumption, key=power_consumption.get)
    kwh = power_consumption[top_device] * KWH_PER_WH
    return f"{top_device.capitalize()} used the most electricity: {kwh:.2f} kWh"


def process_query(query: str) -> str:
    try:
        if query == "1" or query == "What is the average moisture inside my kitchen fridge in the past three hours?":
            return process_fridge_moisture_query()
        elif query == "2" or query == "What is the average water consumption per cycle in my smart dishwasher?":
            return process_dishwasher_water_query()
        elif query == "1" or query == "Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?":
            return process_electricity_comparison_query()
        else:
            return "invalid query"
    except Exception as e:
        logger.error(f"error: {e}")
        return "error processing query"

# === Server Setup === #
ip_address = "0.0.0.0"
port_number = 1
load_sensor_data() # load sensor data caches so we don't have to keep requerying the db

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
    server_socket.bind((ip_address, port_number))
    server_socket.listen()
    logger.info(f"server listening on {ip_address}:{port_number}")

    while True:
        conn, addr = server_socket.accept()
        with conn:
            logger.info(f"connected to {addr}")
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                query = data.decode()
                response = process_query(query)
                conn.sendall(response.encode())

from psycopg2 import pool
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Any

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

# === Binary Tree & Metadata === #
@dataclass
class DeviceMetadata:
    id: int
    assetUid: str
    parentAssetUid: Optional[str]
    eventTypes: dict
    mediaType: str
    assetType: str
    latitude: float
    longitude: float
    status: str
    customAttributes: dict
    createdAt: datetime
    updatedAt: datetime

class TreeNode:
    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value
        self.left: None
        self.right: None

class BinaryTree:
    def __init__(self):
        self.root: None

    def insert(self, key: str, value: Any):
        if not self.root:
            self.root = TreeNode(key, value)
            return
        current = self.root

        while True:
            if key < current.key:
                if not current.left:
                    current.left = BinaryTree(key, value)
                    break
                current = current.left
            else:
                if not current.right:
                    current.right = BinaryTree(key, value)
                    break
                current = current.right

    def search(self, key: str):
        current = self.root
        while current:
            if key == current.key:
                return current.value
            if key < current.key:
                current = current.left
            else:
                current = current.right
        return None

device_metadata_tree = BinaryTree()

def load_device_metadata():
    conn = connection_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM fridge_data_metadata")
        results = cur.fetchall()
        
        for row in results:
            device_metadata_tree.insert(row[1], DeviceMetadata(*row)) # insert to row[1] with assertUid
        logger.info("Fridge metadata loaded")

    except Exception as e:
        logger.error(f"Fridge metadata load error: {e}")
    
    finally:
        cur.close()
        connection_pool.putconn(conn) # returns connection object to be reused, avoidf overhead

def convert_to_pst(timestamp: datetime) -> datetime:
    return timestamp.astimezone(PST)

# === Query Handlers ==== #
def process_fridge_moisture_query():
    conn = connection_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT payload, time FROM fridge_data_virtual""",)
        rows = cur.fetchall()

        total = 0
        count = 0
        
        for payload in rows:
            moisture = payload.get("DHT11 - fridge_moisture_sensor")
            if moisture:
                total += float(moisture) * MOISTURE_TO_RH
                count += 1
            
        if count == 0:
            return "No recent fridge moisture data"
        return f"Average fridge moisture: {total / count:.1f}% RH"

    except Exception as e:
        return f"Error getting fridge data: {e}"
    
    finally:
        cur.close()
        connection_pool.putconn(conn)

# def process_dishwasher_water_query():
#     conn = get_db_connection()
#     cur = conn.cursor()
#     try:
#         cur.execute("""
#             select sr.assetUid, sr.payload, sr.time
#             from sensor_readings_virtual sr
#             where sr.topic = 'dishwasher/water' and sr.time >= now() - interval '3 hours'
#         """)
        
#         readings = cur.fetchall()
#         if not readings:
#             return "no water data available"
        
#         total_gallons = 0
#         count = 0
#         for assetUid, payload, timestamp in readings:
#             water = payload.get("water", 0)
#             if water:
#                 total_gallons += water
#                 count += 1
        
#         if count > 0:
#             avg_gallons = total_gallons / count
#             return f"average water: {avg_gallons:.1f} gallons"
#         return "no valid water data"
#     except Exception as e:
#         logger.error(f"error: {e}")
#         return "error processing query"
#     finally:
#         cur.close()
#         release_db_connection(conn)

# def process_electricity_comparison_query():
#     conn = get_db_connection()
#     cur = conn.cursor()
#     try:
#         cur.execute("""
#             select sr.assetUid, sr.payload, sr.time, sr.topic
#             from sensor_readings_virtual sr
#             where sr.topic in ('refrigerator/power', 'dishwasher/power') 
#             and sr.time >= now() - interval '1 day'
#         """)
        
#         readings = cur.fetchall()
#         if not readings:
#             return "no power data available"
        
#         power_consumption = defaultdict(float)
#         for assetUid, payload, timestamp, topic in readings:
#             power = payload.get("power", 0)
#             if power:
#                 power_consumption[assetUid] += power
        
#         if power_consumption:
#             highest_consumer = max(power_consumption, key=power_consumption.get)
#             total_power = power_consumption[highest_consumer]
#             kwh = total_power * kwh_per_watt_hour
#             return f"device {highest_consumer} consumed the most power: {kwh:.1f} kwh"
#         return "no valid power data"
#     except Exception as e:
#         logger.error(f"error: {e}")
#         return "error processing query"
#     finally:
#         cur.close()
#         release_db_connection(conn)

def process_query(query: str) -> str:
    try:
        if query == "What is the average moisture inside my kitchen fridge in the past three hours?" or '1':
            return process_fridge_moisture_query()
        elif query == "What is the average water consumption per cycle in my smart dishwasher?" or '2':
            return "query 2 test"
            # return process_dishwasher_water_query()
        elif query == "Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?" or '3':
            return "query 3 test"
            # return process_electricity_comparison_query()
        else:
            return "invalid query"
    except Exception as e:
        logger.error(f"error: {e}")
        return "error processing query"

# load metadata before starting server
load_device_metadata()

# server setup
ip_address = "0.0.0.0"
port_number = 1

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

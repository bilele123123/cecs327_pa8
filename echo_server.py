import socket
import os
from psycopg2 import pool
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
from collections import defaultdict
import logging
from dataclasses import dataclass
from typing import Optional, Any

# setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# load env vars and setup db connection
load_dotenv()
connection_string = os.getenv('DATABASE_URL')
connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)

# unit conversion constants
MOISTURE_TO_RH_CONVERSION = 1.5
GALLONS_PER_LITER = 0.264172
KWH_PER_WATT_HOUR = 0.001

@dataclass
class DeviceMetadata:
    device_id: str
    device_type: str
    data_source: str
    timezone: str
    unit_of_measure: str
    conversion_factor: float

class BinaryTreeNode:
    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value
        self.left: Optional[BinaryTreeNode] = None
        self.right: Optional[BinaryTreeNode] = None

class BinaryTree:
    def __init__(self):
        self.root: Optional[BinaryTreeNode] = None

    def insert(self, key: str, value: Any):
        if not self.root:
            self.root = BinaryTreeNode(key, value)
            return
        
        current = self.root
        while True:
            if key < current.key:
                if not current.left:
                    current.left = BinaryTreeNode(key, value)
                    break
                current = current.left
            else:
                if not current.right:
                    current.right = BinaryTreeNode(key, value)
                    break
                current = current.right

    def search(self, key: str) -> Optional[Any]:
        current = self.root
        while current:
            if key == current.key:
                return current.value
            if key < current.key:
                current = current.left
            else:
                current = current.right
        return None

# init device metadata tree
device_metadata_tree = BinaryTree()

def load_device_metadata():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT device_id, device_type, data_source, timezone, unit_of_measure, conversion_factor
            FROM device_metadata
        """)
        for row in cur.fetchall():
            metadata = DeviceMetadata(*row)
            device_metadata_tree.insert(metadata.device_id, metadata)
        logger.info("device metadata loaded")
    except Exception as e:
        logger.error(f"error loading metadata: {e}")
    finally:
        cur.close()
        release_db_connection(conn)

def get_db_connection():
    return connection_pool.getconn()

def release_db_connection(conn):
    connection_pool.putconn(conn)

def convert_to_pst(timestamp: datetime) -> datetime:
    pst = pytz.timezone('America/Los_Angeles')
    return timestamp.astimezone(pst)

def process_fridge_moisture_query():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        three_hours_ago = datetime.utcnow() - timedelta(hours=3)
        cur.execute("""
            SELECT fd.device_id, fd.moisture_reading, fd.timestamp, dm.conversion_factor
            FROM fridge_data fd
            JOIN device_metadata dm ON fd.device_id = dm.device_id
            WHERE fd.device_type = 'refrigerator' 
            AND fd.timestamp >= %s
        """, (three_hours_ago,))
        
        readings = cur.fetchall()
        if not readings:
            return "no moisture data available"
        
        total_rh = 0
        count = 0
        for device_id, moisture, timestamp, conversion_factor in readings:
            metadata = device_metadata_tree.search(device_id)
            if metadata:
                rh_percentage = moisture * conversion_factor
                total_rh += rh_percentage
                count += 1
        
        if count > 0:
            avg_rh = total_rh / count
            return f"average moisture: {avg_rh:.1f}% rh"
        return "no valid moisture data"
    except Exception as e:
        logger.error(f"error: {e}")
        return "error processing query"
    finally:
        cur.close()
        release_db_connection(conn)

def process_dishwasher_water_query():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT dd.device_id, dd.water_consumption, dd.timestamp, dm.conversion_factor
            FROM dishwasher_data dd
            JOIN device_metadata dm ON dd.device_id = dm.device_id
            WHERE dd.cycle_completed = true
        """)
        
        readings = cur.fetchall()
        if not readings:
            return "no water data available"
        
        total_gallons = 0
        count = 0
        for device_id, water, timestamp, conversion_factor in readings:
            metadata = device_metadata_tree.search(device_id)
            if metadata:
                gallons = water * conversion_factor
                total_gallons += gallons
                count += 1
        
        if count > 0:
            avg_gallons = total_gallons / count
            return f"average water: {avg_gallons:.1f} gallons"
        return "no valid water data"
    except Exception as e:
        logger.error(f"error: {e}")
        return "error processing query"
    finally:
        cur.close()
        release_db_connection(conn)

def process_electricity_comparison_query():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT dpd.device_id, SUM(dpd.power_consumption) as total_power,
                   dm.device_type, dm.conversion_factor
            FROM device_power_data dpd
            JOIN device_metadata dm ON dpd.device_id = dm.device_id
            WHERE dm.device_type IN ('refrigerator', 'dishwasher')
            GROUP BY dpd.device_id, dm.device_type, dm.conversion_factor
            ORDER BY total_power DESC
        """)
        
        results = cur.fetchall()
        if not results:
            return "no power data available"
        
        highest_consumer = results[0]
        device_id, total_power, device_type, conversion_factor = highest_consumer
        kwh = total_power * conversion_factor
        
        return f"device {device_id} ({device_type}): {kwh:.1f} kwh"
    except Exception as e:
        logger.error(f"error: {e}")
        return "error processing query"
    finally:
        cur.close()
        release_db_connection(conn)

def process_query(query: str) -> str:
    try:
        if query == "What is the average moisture inside my kitchen fridge in the past three hours?":
            return process_fridge_moisture_query()
        elif query == "What is the average water consumption per cycle in my smart dishwasher?":
            return process_dishwasher_water_query()
        elif query == "Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?":
            return process_electricity_comparison_query()
        else:
            return "invalid query"
    except Exception as e:
        logger.error(f"error: {e}")
        return "error processing query"

# load metadata before starting server
load_device_metadata()

# server setup
ip_address = "0.0.0.0"
port_number = 60000

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
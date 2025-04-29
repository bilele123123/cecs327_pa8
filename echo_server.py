import socket
import psycopg2
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

# database connection string
connection_string = "DATABASE_URL"

# setup connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, connection_string)

# unit conversion constants
moisture_to_rh_conversion = 1.5
gallons_per_liter = 0.264172
kwh_per_watt_hour = 0.001

@dataclass
class device_metadata:
    device_id: str
    device_type: str
    data_source: str
    timezone: str
    unit_of_measure: str
    conversion_factor: float
    column_7: str
    column_8: str
    column_9: str
    column_10: str
    column_11: str
    column_12: str

class binary_tree_node:
    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value
        self.left: Optional[binary_tree_node] = None
        self.right: Optional[binary_tree_node] = None

class binary_tree:
    def __init__(self):
        self.root: Optional[binary_tree_node] = None

    def insert(self, key: str, value: Any):
        if not self.root:
            self.root = binary_tree_node(key, value)
            return
        
        current = self.root
        while True:
            if key < current.key:
                if not current.left:
                    current.left = binary_tree_node(key, value)
                    break
                current = current.left
            else:
                if not current.right:
                    current.right = binary_tree_node(key, value)
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
device_metadata_tree = binary_tree()

def get_db_connection():
    return connection_pool.getconn()

def release_db_connection(conn):
    connection_pool.putconn(conn)

def load_device_metadata():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("select * from sensor_readings_metadata")
        results = cur.fetchall()
        
        for row in results:
            metadata = device_metadata(*row)
            device_metadata_tree.insert(metadata.device_id, metadata)
        
        logger.info("device metadata loaded")
    except Exception as e:
        logger.error(f"error loading metadata: {e}")
    finally:
        cur.close()
        release_db_connection(conn)

def convert_to_pst(timestamp: datetime) -> datetime:
    pst = pytz.timezone('America/Los_Angeles')
    return timestamp.astimezone(pst)

def process_fridge_moisture_query():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        three_hours_ago = datetime.utcnow() - timedelta(hours=3)
        cur.execute("""
            select sr.assetUid, sr.payload, sr.time 
            from sensor_readings_virtual sr
            where sr.topic = 'fridge/moisture' and sr.time >= %s
        """, (three_hours_ago,))
        
        readings = cur.fetchall()
        if not readings:
            return "no moisture data available"
        
        total_rh = 0
        count = 0
        for assetUid, payload, timestamp in readings:
            moisture = payload.get("moisture", 0)
            if moisture:
                total_rh += moisture
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
            select sr.assetUid, sr.payload, sr.time
            from sensor_readings_virtual sr
            where sr.topic = 'dishwasher/water' and sr.time >= now() - interval '3 hours'
        """)
        
        readings = cur.fetchall()
        if not readings:
            return "no water data available"
        
        total_gallons = 0
        count = 0
        for assetUid, payload, timestamp in readings:
            water = payload.get("water", 0)
            if water:
                total_gallons += water
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
            select sr.assetUid, sr.payload, sr.time, sr.topic
            from sensor_readings_virtual sr
            where sr.topic in ('refrigerator/power', 'dishwasher/power') 
            and sr.time >= now() - interval '1 day'
        """)
        
        readings = cur.fetchall()
        if not readings:
            return "no power data available"
        
        power_consumption = defaultdict(float)
        for assetUid, payload, timestamp, topic in readings:
            power = payload.get("power", 0)
            if power:
                power_consumption[assetUid] += power
        
        if power_consumption:
            highest_consumer = max(power_consumption, key=power_consumption.get)
            total_power = power_consumption[highest_consumer]
            kwh = total_power * kwh_per_watt_hour
            return f"device {highest_consumer} consumed the most power: {kwh:.1f} kwh"
        return "no valid power data"
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

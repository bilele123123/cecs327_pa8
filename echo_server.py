import socket
import os
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

# directly define the connection string
connection_string = "postgresql://neondb_owner:npg_YZPcTaL9X1uQ@ep-rough-forest-a6lx105f-pooler.us-west-2.aws.neon.tech/neondb?sslmode=require"

# set up the connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, connection_string)

# get a connection from the pool
connection = connection_pool.getconn()

# do something with the connection
cursor = connection.cursor()
cursor.execute("SELECT * FROM sensor_readings_metadata")
results = cursor.fetchall()
print(results)

# return the connection back to the pool
connection_pool.putconn(connection)

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
    column_7: str  # Add these for each additional column in the query result
    column_8: str
    column_9: str
    column_10: str
    column_11: str
    column_12: str

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
            SELECT * FROM sensor_readings_metadata
        """)
        results = cur.fetchall()
        
        # Log the number of columns returned in the first result row
        logger.info(f"Query returned {len(results[0])} columns.")  # Log column count
        
        for row in results:
            logger.info(f"Row length: {len(row)}")  # Check if row length matches expected number of fields
            metadata = DeviceMetadata(*row)  # This is where the error occurs if row length doesn't match
            device_metadata_tree.insert(metadata.device_id, metadata)
        logger.info("Device metadata loaded")
    except Exception as e:
        logger.error(f"Error loading metadata: {e}")
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
        # Assume 'fridge' data is contained in `sensor_readings_virtual` with payload data
        three_hours_ago = datetime.utcnow() - timedelta(hours=3)
        cur.execute("""
            SELECT sr.assetUid, sr.payload, sr.time, sr.retain 
            FROM sensor_readings_virtual sr
            WHERE sr.topic = 'fridge/moisture' AND sr.time >= %s
        """, (three_hours_ago,))
        
        readings = cur.fetchall()
        if not readings:
            return "no moisture data available"
        
        total_rh = 0
        count = 0
        for assetUid, payload, timestamp, retain in readings:
            # Assuming 'payload' contains the moisture reading and possibly other relevant info
            moisture = payload.get("moisture", 0)  # Adjust this depending on payload structure
            if moisture:
                # You could apply a conversion factor here if needed (similar to your original logic)
                total_rh += moisture  # Example: directly adding moisture value
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
            SELECT sr.assetUid, sr.payload, sr.time
            FROM sensor_readings_virtual sr
            WHERE sr.topic = 'dishwasher/water' AND sr.time >= NOW() - INTERVAL '3 hours'
        """)
        
        readings = cur.fetchall()
        if not readings:
            return "no water data available"
        
        total_gallons = 0
        count = 0
        for assetUid, payload, timestamp in readings:
            # Assuming 'payload' contains water consumption info
            water = payload.get("water", 0)  # Adjust this depending on your actual data structure
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
            SELECT sr.assetUid, sr.payload, sr.time, sr.topic
            FROM sensor_readings_virtual sr
            WHERE sr.topic IN ('refrigerator/power', 'dishwasher/power') 
            AND sr.time >= NOW() - INTERVAL '1 day'  -- Adjust as needed for the timeframe
        """)
        
        readings = cur.fetchall()
        if not readings:
            return "no power data available"
        
        power_consumption = defaultdict(float)
        
        # Iterate over the readings and aggregate the power consumption for each device
        for assetUid, payload, timestamp, topic in readings:
            # Extract power consumption from the payload (assuming 'power' is a key)
            power = payload.get("power", 0)  # Adjust if the structure of payload is different
            
            if power:
                power_consumption[assetUid] += power
        
        # Now find the device with the highest power consumption
        if power_consumption:
            highest_consumer = max(power_consumption, key=power_consumption.get)
            total_power = power_consumption[highest_consumer]
            # Optionally convert power to kWh if needed, for example by multiplying by a conversion factor
            kwh = total_power * KWH_PER_WATT_HOUR  # If needed to convert to kWh (change constant if necessary)
            
            return f"device {highest_consumer} consumed the most power: {kwh:.1f} kWh"
        else:
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
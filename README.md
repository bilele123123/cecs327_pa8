# IoT Query System — Assignment 8

This project implements a **TCP server and client** that queries IoT data from a database connected to a simulated IoT environment (Dataniz). The server retrieves and caches data from three IoT devices:

- Smart Fridge
- Smart Fridge Clone
- Smart Dishwasher

It answers user queries about the devices' sensor data.

## Setup Instructions

1. **Clone the project & navigate to the directory**

    ```sh
    git clone <https://github.com/bilele123123/cecs327_pa8.git>
    cd <cecs327_pa8>
    ```

2. **Create a .env file**

    Create a .env file in the project root directory and add your database URL:

    ```sh 
    DATABASE_URL="your-database-connection-url"
    ```

3. **Create and activate a Python virtual environment**

    ```sh
    python3 -m venv venv
    source venv/bin/activate # on Mac/Linux
    venv\Scripts\activate    # on Windows
    ```

4. **Install dependencies**
    ```sh
    pip install -r requirements.txt
    ```

5. **Run the server**
    ```sh
    python3 echo_server.py
    ```
    This will start the TCP server and load IoT sensor data into memory. The server automatically refreshes the cache every 3 minutes.

6. **Run the client in a new terminal**
    ```sh
    python3 echo_client.py
    ```
    You will be prompted to enter the server IP adress and port.

    ```sh
    Enter IP address: 0.0.0.0
    Enter port: 60000
    ```

## Valid Queries

You can enter either the query number of the full query text:

| Option | Query                                                                                                   |
| :----- | :------------------------------------------------------------------------------------------------------ |
| 1      | What is the average moisture inside my kitchen fridge in the past three hours?                          |
| 2      | What is the average water consumption per cycle in my smart dishwasher?                                 |
| 3      | Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)? |
| 4      | Reload data                                                                                             |

## Dataniz Setup is Required

Before running this system, you need to configure your own Dataniz IoT simulation platform:

- Create three virtual devices:

    1. smart_fridge
    2. smart_fridge_clone
    3. smart_dishwasher

- Add sensors to each device (e.g., moisture sensor, ammeter, water flow sensor)

- Ensure Dataniz is connected to your database and pushing sensor data into respective tables

**IMPORTANT: The server expects the following table schemas with payload JSON columns:**

- smart_fridge_virtual
- smart_fridge_clone_virtual
- smart_dishwasher_virtual

## How It Works

- The server loads sensor data into in-memory hashmaps for fast query processing.
- Every 3 minutes, the server automatically reloads data from the database.
- The client sends a query → the server responds using cached data.
- Sending query "Reload data" (or option 4) forces a manual cache reload.

## Example Run

```sh
Enter IP address: 127.0.0.1
Enter port: 60000

Welcome to the IoT Query System!

Valid queries:
1. What is the average moisture inside my kitchen fridge in the past three hours?
2. What is the average water consumption per cycle in my smart dishwasher?
3. Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?
4. Reload data

Enter your query: 1

Server response: Average fridge moisture: 68.4% RH
```


## Notes
The system filters data to return results from the last 3 hours.

Output is returned in:

    Moisture → % RH

    Water → gallons

    Electricity → 
    
Server timestamps are handled in PST

## Troubleshooting

If you get a connection error, check:

- Server is running and listening on port `60000`
- Database URL in `.env` is correct
- Dataniz is properly pushing data to the database
- Database tables contain recent data

## Credits

Built for **CECS 327 - Assignment 8**

Contributors: *Thai Le and Mariann Grace*

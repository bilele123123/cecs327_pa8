import socket

# Define valid queries
VALID_QUERIES = [
    "What is the average moisture inside my kitchen fridge in the past three hours?",
    "What is the average water consumption per cycle in my smart dishwasher?",
    "Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?"
]

def display_valid_queries():
    print("\nValid queries:")
    for i, query in enumerate(VALID_QUERIES, 1):
        print(f"{i}. {query}")

def is_valid_query(message):
    return message in VALID_QUERIES

ip_address = input("Enter IP address: ") # prompt ip 
port = int(input("Enter port: ")) # prompt port

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    try:
        client_socket.connect((ip_address, port))
        print("\nWelcome to the IoT Query System!")
        display_valid_queries()
        
        while True: # sending multiple messages
            message_to_send = input("\nEnter your query: ") # prompt message
            
            if not is_valid_query(message_to_send):
                print("\nSorry, this query cannot be processed. Please try one of the following:")
                display_valid_queries()
                continue
                
            client_socket.sendall(message_to_send.encode()) 
            response = client_socket.recv(1024).decode()
            print(f"\nServer response: {response}") # display server response
            
    except Exception as e:
        print(f"Error: {e}") # display error
import socket

ip_address = input("Enter IP address: ") # prompt ip 
port = int(input("Enter port: ")) # prompt port

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    try:
        client_socket.connect((ip_address, port))
        while True: # sending multiple messages
            message_to_send = input("Enter message to send: ") # prompt message
            client_socket.sendall(message_to_send.encode()) 
            response = client_socket.recv(1024).decode()
            print(f"Server response: {response}") # display server response
    except Exception as e:
        print(f"Error {e}") # display error
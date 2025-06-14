import socket
import threading
import random
import os
import base64


class UDPServer:
    def __init__(self, port):  #Initialize the server
        self.server_port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind(('', port))
        print(f"[Server] Started successfully, listening on port {port}")

    def start(self):
        while True:
            try:
                data, client_addr = self.server_socket.recvfrom(1024)
                message = data.decode().strip()
                print(f"[Server] Received message from {client_addr}: {message}")

                if message.startswith("DOWNLOAD"):
                    threading.Thread(  #Process download requests
                        target=self.handle_download_request,
                        args=(message, client_addr)
                    ).start()

            except Exception as e:
                print(f"[Server] Error: {e}")

    def handle_download_request(self, message, client_addr):
        try: #Processing download request
            filename = message.split()[1]#Get file name
            if os.path.exists(filename):
                file_size = os.path.getsize(filename) #Get file size
                worker_port = random.randint(50000, 51000)
                response = f"OK {filename} SIZE {file_size} PORT {worker_port}"
                self.server_socket.sendto(response.encode(), client_addr)
                print(f"[Server] Sent OK response to {client_addr}, port {worker_port}")
                transfer_thread = threading.Thread(
                    target=self.handle_file_transfer,
                    args=(filename, worker_port, client_addr[0]),
                    daemon=True
                )
                transfer_thread.start() #Start the file transfer thread
            else:
                response = f"ERR {filename} NOT_FOUND"
                self.server_socket.sendto(response.encode(), client_addr)
                print(f"[Server] File {filename} not found")
        except IndexError:
            print(f"[Server] Invalid DOWNLOAD message format: {message}")
            response = "ERR INVALID_FORMAT"
            self.server_socket.sendto(response.encode(), client_addr)
        except Exception as e:
            print(f"[Server] Download request error: {e}")

    def handle_file_transfer(self, filename, port, client_host):
        # Handle file transfer
        transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        transfer_socket.bind(('', port))  # Bind transfer port
        print(f"[File Transfer] Starting file {filename} transfer on port {port}")
        try:
            with open(filename, 'rb') as file:
                while True:
                    data, addr = transfer_socket.recvfrom(2048)
                    message = data.decode()
                    if message.startswith("FILE") and "GET" in message:
                        # Handle file chunk request
                        parts = message.split()
                        start = int(parts[parts.index("START") + 1])
                        end = int(parts[parts.index("END") + 1])
                        file.seek(start)
                        chunk = file.read(end - start + 1)
                        if not chunk:  # Check for empty data chunk
                            print(f"[File Transfer] Warning: Empty data chunk {start}-{end}")
                            continue
                        print(f"[File Transfer] Actually read {len(chunk)} bytes of data")
                        encoded_data = base64.b64encode(chunk).decode('utf-8')  # Base64 encoding
                        # Send large data in chunks (avoid oversized UDP packets)
                        max_chunk_size = 1024
                        for i in range(0, len(encoded_data), max_chunk_size):
                            chunk_part = encoded_data[i:i + max_chunk_size]
                            response = (
                                f"FILE {filename} OK START {start} END {end} "
                                f"DATA {chunk_part}"
                            )
                            transfer_socket.sendto(response.encode(), (client_host, addr[1]))
                        print(f"[File Transfer] Sent block {start}-{end} (original data {len(chunk)} bytes)")
                    elif message.startswith("FILE") and "CLOSE" in message:
                        # Handle close request
                        response = f"FILE {filename} CLOSE_OK"
                        transfer_socket.sendto(response.encode(), (client_host, addr[1]))
                        print(f"[File Transfer] File {filename} transfer completed")
                        break
        except Exception as e:
            print(f"[File Transfer] Error: {e}")
        finally:
            transfer_socket.close()  # Close transfer socket


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python3 UDPserver.py <port>")
        sys.exit(1)
    port = int(sys.argv[1])
    server = UDPServer(port)
    server.start()

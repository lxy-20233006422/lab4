import socket
import threading
import random
import os
import base64


class ParallelUDPServer:#Initializing TCP server
    def __init__(self, port, max_workers=10):
        self.server_port = port
        self.max_workers = max_workers
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind(('', port))
        self.active_transfers = 0
        self.lock = threading.Lock()
        print(f"[Parallel Server] Started successfully, listening on port {port}")

    def start(self): #Start the server
        while True:
            try:
                data, client_addr = self.server_socket.recvfrom(1024)
                message = data.decode().strip()
                print(f"[Parallel Server] Received message from {client_addr}: {message}")
                if message.startswith("DOWNLOAD"):
                    with self.lock:
                        if self.active_transfers >= self.max_workers:
                            response = "ERR SERVER_BUSY"
                            self.server_socket.sendto(response.encode(), client_addr)
                            print(f"[Parallel Server] Request denied: reached max worker threads {self.max_workers}")
                            continue
                        self.active_transfers += 1
                    threading.Thread(
                        target=self.handle_download_request,
                        args=(message, client_addr)
                    ).start()
            except Exception as e:
                print(f"[Parallel Server] Error: {e}")

    def handle_download_request(self, message, client_addr):
        try:#The specific logic for handling download requests
            filename = message.split()[1]
            if os.path.exists(filename):
                file_size = os.path.getsize(filename)
                worker_port = random.randint(50000, 51000)
                response = f"OK {filename} SIZE {file_size} PORT {worker_port}"
                self.server_socket.sendto(response.encode(), client_addr)
                print(f"[Parallel Server] Sent OK response to {client_addr}, port {worker_port}")
                transfer_thread = threading.Thread(
                    target=self.handle_file_transfer,
                    args=(filename, worker_port, client_addr[0]),
                    daemon=True
                )
                transfer_thread.start()
                transfer_thread.join()
            else:
                response = f"ERR {filename} NOT_FOUND"
                self.server_socket.sendto(response.encode(), client_addr)
                print(f"[Parallel Server] File {filename} not found")
        except IndexError:
            print(f"[Parallel Server] Invalid DOWNLOAD message format: {message}")
            response = "ERR INVALID_FORMAT"
            self.server_socket.sendto(response.encode(), client_addr)
        except Exception as e:
            print(f"[Parallel Server] Download request error: {e}")
        finally:
            with self.lock:
                self.active_transfers -= 1
    def handle_file_transfer(self, filename, port, client_host):#The specific logic for handling file transfers
        transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        transfer_socket.bind(('', port))
        print(f"[Parallel Transfer] Starting file transfer for {filename} on port {port}")
        try:#Receive client request
            with open(filename, 'rb') as file:
                while True:
                    data, addr = transfer_socket.recvfrom(2048)
                    message = data.decode()
                    if message.startswith("FILE") and "GET" in message:
                        parts = message.split()#Analyze the starting and ending positions in the request.
                        start = int(parts[parts.index("START") + 1])
                        end = int(parts[parts.index("END") + 1])
                        file.seek(start)#Read file block and send
                        chunk = file.read(end - start + 1)
                        if not chunk:
                            print(f"[Parallel Transfer] Warning: Empty data block {start}-{end}")
                            continue
                        encoded_data = base64.b64encode(chunk).decode('utf-8')#Use Base64 encoded data block
                        response = f"FILE {filename} OK START {start} END {end} DATA {encoded_data}"
                        transfer_socket.sendto(response.encode(), (client_host, addr[1]))
        except Exception as e:
            print(f"[Parallel Transfer] Error: {e}")
        finally:
            transfer_socket.close()

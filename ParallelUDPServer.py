import socket
import threading
import random
import os
import base64


class ParallelUDPServer: #Initializing TCP server
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

import base64
import os
import socket
import threading
from queue import Queue

class ParallelUDPClient: #Initialize UDP client
    def __init__(self, host, port, file_list, max_threads=4):
        self.server_host = host
        self.server_port = port
        self.file_list = file_list
        self.max_threads = max_threads
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(5.0)
        self.max_retries = 5
        self.file_queue = Queue()
        self.lock = threading.Lock()

    def run(self): #Start the main logic of the client.
        try:
            with open(self.file_list, 'r', encoding='utf-8') as f:
                filenames = [line.strip() for line in f if line.strip()] #Read file list
            for filename in filenames:
                self.file_queue.put(filename) #Add the filename to the queue.
            threads = []
            for _ in range(min(self.max_threads, len(filenames))):
                t = threading.Thread(target=self.worker)
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
        except FileNotFoundError:
            print(f"[Parallel Client] Error: File list {self.file_list} not found")
        except Exception as e:
            print(f"[Parallel Client] Error: {e}")
        finally:
            self.socket.close() #Close the socket

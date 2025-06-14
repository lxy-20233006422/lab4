import base64
import os
import socket
import threading
from queue import Queue

class ParallelUDPClient:
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

    def run(self):
        try:
            with open(self.file_list, 'r', encoding='utf-8') as f:
                filenames = [line.strip() for line in f if line.strip()]
            for filename in filenames:
                self.file_queue.put(filename)
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
            self.socket.close()

    def worker(self):#Worker thread function that retrieves files from the queue and downloads them.
        while not self.file_queue.empty():
            filename = self.file_queue.get()
            try:
                self.download_file(filename)
            finally:
                self.file_queue.task_done()
    def download_file(self, filename):
        with self.lock:
            print(f"[Parallel Client] Starting download: {filename}")
        response = self.send_with_retry(#Send a download request and retry.
            f"DOWNLOAD {filename}",
            (self.server_host, self.server_port),
            lambda r: r.startswith(("OK", "ERR")))
        if not response:
            with self.lock:
                print(f"[Parallel Client] Download failed for {filename}: Cannot connect to server")
            return
        if response.startswith("ERR"):
            with self.lock:
                print(f"[Parallel Client] Download failed for {filename}: {response}")
            return
        parts = response.split() #Parse server response
        file_size = int(parts[parts.index("SIZE") + 1])
        port = int(parts[parts.index("PORT") + 1])
        with self.lock:
            print(f"[Parallel Client] Receiving file {filename} (size: {file_size} bytes)")
        if not self.receive_file(filename, file_size, port):#Receive file
            with self.lock:
                print(f"[Parallel Client] File {filename} incomplete")
            return
        with self.lock:
            print(f"[Parallel Client] File {filename} downloaded successfully")

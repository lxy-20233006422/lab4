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

    def receive_file(self, filename, file_size, port):
        # Create UDP socket for file transfer
        transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Set socket timeout to 10 seconds
        transfer_socket.settimeout(10.0)
        # Temporary filename
        temp_filename = filename + ".download"
        try:
            # Open temporary file in binary write mode
            with open(temp_filename, 'wb') as file:
                total_received = 0
                # Loop to receive file data until complete
                while total_received < file_size:
                    start = total_received
                    end = min(start + 999, file_size - 1)
                    # Maximum 5 attempts per data chunk
                    for attempt in range(5):
                        try:
                            # Send file chunk request
                            request = f"FILE {filename} GET START {start} END {end}"
                            transfer_socket.sendto(request.encode(), (self.server_host, port))
                            # Receive server response
                            data, _ = transfer_socket.recvfrom(4096)
                            response = data.decode()
                            # Process response containing data
                            if "DATA" in response:
                                data_part = response.split("DATA ")[1]
                                chunk = base64.b64decode(data_part.encode('utf-8'))
                                # Verify received data length
                                if len(chunk) == (end - start + 1):
                                    # Write to file
                                    file.write(chunk)
                                    total_received += len(chunk)
                                    # Print progress (using \r for single-line update)
                                    with self.lock:
                                        print(
                                            f"\r[Progress {filename}] {total_received}/{file_size} ({total_received / file_size:.1%})",
                                            end='', flush=True)
                                    break  # Break retry loop after successful reception
                        except socket.timeout:
                            # Timeout handling: print retry message
                            with self.lock:
                                print(f"\n[Retry {filename}] Chunk {start}-{end} timeout ({attempt + 1}/5)")
                            continue  # Continue retry
            # Check if file is fully received
            if total_received == file_size:
                # Replace if target exists, otherwise rename
                if os.path.exists(filename):
                    os.replace(temp_filename, filename)
                else:
                    os.rename(temp_filename, filename)
                return True  # Return success
            return False  # Return failure if incomplete
        except Exception as e:
            # Exception handling: print error message
            with self.lock:
                print(f"\n[Critical Error {filename}] {str(e)}")
            return False
        finally:
            # Cleanup operations that always execute
            transfer_socket.close()  # Close socket
            # Remove temp file if exists (for failure cleanup)
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

    def send_with_retry(self, message, address, response_validator):
        retry_count = 0
        while retry_count < self.max_retries:

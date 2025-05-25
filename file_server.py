import socket
import json
import base64
import logging
import os
import time
import concurrent.futures
import statistics
import csv
import argparse
from typing import List, Dict


SERVER_IP = "172.16.16.101"
SERVER_PORT = 6677  # Changed port number

class FileServer:
    def __init__(self, worker_type: str = 'thread', workers: int = 1):
        self._setup_directories()
        self.logger = self._configure_logging()
        self.worker_type = worker_type
        self.workers = workers
        self.success_count = 0
        self.fail_count = 0

    def _setup_directories(self) -> None:
        os.makedirs('server_files', exist_ok=True)

    def _configure_logging(self) -> logging.Logger:
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.StreamHandler()  # Removed FileHandler for server.log
            ]
        )
        return logging.getLogger(__name__)

    def _handle_connection(self, client_socket: socket.socket) -> None:
        try:
            data_received = ""
            while True:
                data = client_socket.recv(1024*1024)
                if data:
                    data_received += data.decode()
                    if "\r\n\r\n" in data_received:
                        break
                else:
                    break

            command = data_received.split("\r\n\r\n")[0]
            response = self._process_command(command)
            json_response = json.dumps(response) + "\r\n\r\n"
            client_socket.sendall(json_response.encode())
            self.success_count += 1
        except Exception as e:
            self.logger.error(f"Connection handling error: {str(e)}")
            self.fail_count += 1
        finally:
            client_socket.close()

    def _process_command(self, command: str) -> Dict:
        try:
            parts = command.split()
            if not parts:
                return {'status': 'ERROR', 'data': 'Empty command'}

            cmd = parts[0].upper()
            if cmd == 'LIST':
                return self._list_files()
            elif cmd == 'UPLOAD' and len(parts) >= 3:
                return self._upload_file(parts[1], ' '.join(parts[2:]))
            elif cmd == 'GET' and len(parts) >= 2:
                return self._download_file(parts[1])
            else:
                return {'status': 'ERROR', 'data': 'Invalid command'}
        except Exception as e:
            return {'status': 'ERROR', 'data': str(e)}

    def _list_files(self) -> Dict:
        try:
            files = os.listdir('server_files')
            return {'status': 'OK', 'data': files}
        except Exception as e:
            return {'status': 'ERROR', 'data': str(e)}

    def _upload_file(self, filename: str, content_b64: str) -> Dict:
        try:
            filepath = os.path.join('server_files', filename)
            with open(filepath, 'wb') as f:
                f.write(base64.b64decode(content_b64))
            return {'status': 'OK', 'data': f'File {filename} uploaded successfully'}
        except Exception as e:
            return {'status': 'ERROR', 'data': str(e)}

    def _download_file(self, filename: str) -> Dict:
        try:
            filepath = os.path.join('server_files', filename)
            if not os.path.exists(filepath):
                return {'status': 'ERROR', 'data': 'File not found'}

            with open(filepath, 'rb') as f:
                content = base64.b64encode(f.read()).decode()
            return {'status': 'OK', 'data_file': content, 'data': f'File {filename} downloaded successfully'}
        except Exception as e:
            return {'status': 'ERROR', 'data': str(e)}

    def run(self) -> None:
        self.logger.info(f"Initializing server with {self.workers} {self.worker_type} workers")
        
        executor_class = concurrent.futures.ThreadPoolExecutor if self.worker_type == 'thread' else concurrent.futures.ProcessPoolExecutor
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((SERVER_IP, SERVER_PORT))
            server_socket.listen(50)
            self.logger.info(f"Server active on {SERVER_IP}:{SERVER_PORT}")

            with executor_class(max_workers=self.workers) as executor:
                try:
                    while True:
                        client_socket, addr = server_socket.accept()
                        self.logger.info(f"New connection from {addr}")
                        executor.submit(self._handle_connection, client_socket)
                except KeyboardInterrupt:
                    self.logger.info("Shutting down server...")
                    self.logger.info(f"Final stats - Successful operations: {self.success_count}, Failed: {self.fail_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='File Server with configurable workers')
    parser.add_argument('--worker-type', choices=['thread', 'process'], default='thread',
                       help='Worker type (thread or process)')
    parser.add_argument('--workers', type=int, default=1,
                       help='Number of worker threads/processes')
    
    args = parser.parse_args()
    
    server = FileServer(worker_type=args.worker_type, workers=args.workers)
    server.run()
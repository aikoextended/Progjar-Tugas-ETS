import socket
import os
import base64
import logging
import json
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import time
import statistics
import csv
from datetime import datetime

class FileClient:
    def __init__(self, server_host='172.16.16.101', server_port=6677):
        self.server_host = server_host
        self.server_port = server_port
        self.operation_stats = {}
        self.csv_filename = 'stress_test_results.csv'
        self._reset_stats()
        self._init_csv()

        self.logger = logging.getLogger('FileClient')
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        os.makedirs('downloaded_files', exist_ok=True)
        os.makedirs('upload_files', exist_ok=True)
        
    def _init_csv(self):
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Timestamp',
                    'Operation',
                    'File Size (MB)',
                    'Client Workers',
                    'Server Workers',
                    'Executor Type',
                    'Success Count',
                    'Fail Count',
                    'Total Time (s)',
                    'Avg Throughput (bytes/s)'
                ])

    def _reset_stats(self):
        self.operation_stats = {
            'operation': '',
            'file_size_mb': 0,
            'client_pool_size': 0,
            'server_pool_size': 0,
            'executor_type': '',
            'success_count': 0,
            'fail_count': 0,
            'durations': [],
            'throughputs': [],
            'results': []
        }

    def _save_to_csv(self):
        stats = self.operation_stats
        with open(self.csv_filename, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                stats['operation'],
                # stats['file_size_mb'],
                int(stats['file_size_mb']) if isinstance(stats['file_size_mb'], float) and stats['file_size_mb'].is_integer() else stats['file_size_mb'],
                stats['client_pool_size'],
                stats['server_pool_size'],
                stats['executor_type'],
                stats['success_count'],
                stats['fail_count'],
                sum(stats['durations']) if stats['durations'] else 0,
                statistics.mean(stats['throughputs']) if stats['throughputs'] else 0
            ])

    def _display_results(self):
        stats = self.operation_stats
        print("\n=== HASIL STRESS TEST ===")
        print(f"1. Operasi: {stats['operation']}")
        
        if stats['operation'] == 'download' and stats['success_count'] > 0:
            # Calculate average file size for downloads in MB
            file_sizes = [r['file_size']/(1024*1024) for r in stats['results'] if r.get('status') == 'OK' and 'file_size' in r]
            if file_sizes:
                avg_file_size = statistics.mean(file_sizes)
                print(f"2. Ukuran File Rata-rata: {avg_file_size:.2f} MB")
                stats['file_size_mb'] = avg_file_size  # Update for CSV
            else:
                print("2. Ukuran File: Tidak tersedia")
        else:
            print(f"2. Ukuran File: {stats['file_size_mb']:.2f} MB")
            
        print(f"3. Jumlah client worker pool: {stats['client_pool_size']}")
        print(f"4. Jumlah server worker pool: {stats['server_pool_size']}")
        
        if stats['durations']:
            total_time = sum(stats['durations'])
            avg_throughput = statistics.mean(stats['throughputs'])
            print(f"5. Waktu total per client: {total_time:.4f} detik")
            print(f"6. Throughput per client: {avg_throughput:.2f} bytes/detik")
        else:
            print("5. Waktu total per client: N/A")
            print("6. Throughput per client: N/A")
            
        print(f"7. Client workers - Sukses: {stats['success_count']}, Gagal: {stats['fail_count']}")
        print(f"8. Server workers - Sukses: {stats['success_count']}, Gagal: {stats['fail_count']}")
        print("="*30)

    def send_command(self, command_str=""):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(300)

        try:
            sock.connect((self.server_host, self.server_port))
            sock.sendall(command_str.encode('utf-8') + b"\r\n\r\n")

            data_received = bytearray()
            while True:
                try:
                    chunk = sock.recv(1024*1024)
                    if chunk:
                        data_received.extend(chunk)
                        if b"\r\n\r\n" in data_received:
                            break
                    else:
                        break
                except socket.timeout:
                    return {'status': 'ERROR', 'data': 'Socket timeout'}
                
            response = data_received.partition(b"\r\n\r\n")[0].decode('utf-8')
            return json.loads(response) if response else {'status': 'ERROR', 'data': 'Empty response'}

        except Exception as e:
            return {'status': 'ERROR', 'data': str(e)}
        finally:
            sock.close()

    def list_files(self):
        response = self.send_command("LIST")
        if response and response.get('status') == 'OK':
            files = response.get('data', [])
            if files:
                print("\nDaftar File:")
                for i, filename in enumerate(files, 1):
                    print(f"{i}. {filename}")
            else:
                print("Tidak ada file di server.")
            return files
        else:
            print("Gagal mendapatkan daftar file.")
            return []

    def download_file(self, filename, worker_id=None):
        start = time.time()
        response = self.send_command(f"GET {filename}")

        if response and response.get('status') == 'OK':
            try:
                file_data = base64.b64decode(response.get('data_file', ''))
                duration = time.time() - start
                file_size = len(file_data)
                
                # Save the downloaded file with worker ID if provided
                save_filename = f"{worker_id}_{filename}" if worker_id is not None else filename
                save_path = os.path.join('downloaded_files', save_filename)
                with open(save_path, 'wb') as f:
                    f.write(file_data)
                
                return {
                    'status': 'OK', 
                    'duration': duration,
                    'throughput': file_size / duration if duration > 0 else 0,
                    'file_size': file_size
                }
            except Exception as e:
                return {'status': 'ERROR', 'error': str(e)}
        return {'status': 'ERROR'}

    def upload_file(self, filepath, worker_id=None):
        start = time.time()
        try:
            with open(filepath, 'rb') as f:
                content = base64.b64encode(f.read()).decode('ascii')
            
            response = self.send_command(f"UPLOAD {os.path.basename(filepath)} {content}")
            duration = time.time() - start
            size = os.path.getsize(filepath)
            
            if response and response.get('status') == 'OK':
                return {
                    'status': 'OK',
                    'duration': duration,
                    'throughput': size / duration if duration > 0 else 0,
                    'file_size': size
                }
            return {'status': 'ERROR'}
        except Exception as e:
            return {'status': 'ERROR', 'error': str(e)}

    def generate_dummy_file(self, size_mb):
        filename = f"dummy_{size_mb}MB.bin"
        filepath = os.path.join('upload_files', filename)
        size_bytes = size_mb * 1024 * 1024

        if os.path.exists(filepath) and os.path.getsize(filepath) == size_bytes:
            return filepath

        try:
            with open(filepath, 'wb') as f:
                f.write(os.urandom(size_bytes))
            return filepath
        except Exception as e:
            print(f"Gagal membuat file dummy: {str(e)}")
            return None

    def _worker_task(self, operation, item, worker_id=None):
        if operation == 'download':
            return self.download_file(item, worker_id)
        elif operation == 'upload':
            return self.upload_file(item, worker_id)
        return {'status': 'ERROR'}

    def perform_operation(self, operation, worker_type='thread', workers=1, server_pool_size=0):
        self._reset_stats()
        self.operation_stats.update({
            'operation': operation,
            'client_pool_size': workers,
            'server_pool_size': server_pool_size,
            'executor_type': worker_type
        })

        if operation == 'list':
            self.list_files()
            return

        items = []
        if operation == 'download':
            files = self.list_files()
            if not files:
                return

            choice = input("Pilih file (nomor atau 'all' untuk semua): ").strip().lower()
            if choice == 'all':
                selected = files
            else:
                try:
                    indices = [int(c.strip())-1 for c in choice.split(',')]
                    selected = [files[i] for i in indices if 0 <= i < len(files)]
                except ValueError:
                    selected = []
            
            if not selected:
                print("Pilihan tidak valid.")
                return
            
            items = selected * workers
        elif operation == 'upload':
            size_mb = int(input("Masukkan ukuran file (MB): "))
            filepath = self.generate_dummy_file(size_mb)
            if filepath:
                items = [filepath] * workers
                self.operation_stats['file_size_mb'] = size_mb
            else:
                print("Gagal membuat file dummy.")
                return

        if worker_type == 'thread':
            executor = ThreadPoolExecutor(max_workers=workers)
        else:
            executor = ProcessPoolExecutor(max_workers=workers)

        with executor:
            futures = [executor.submit(self._worker_task, operation, item, i) 
                      for i, item in enumerate(items)]
            
            for future in as_completed(futures):
                result = future.result()
                self.operation_stats['results'].append(result)
                if result['status'] == 'OK':
                    self.operation_stats['success_count'] += 1
                    self.operation_stats['durations'].append(result['duration'])
                    self.operation_stats['throughputs'].append(result['throughput'])
                else:
                    self.operation_stats['fail_count'] += 1

        self._display_results()
        self._save_to_csv()

def show_menu():
    print("\n=== MENU ===")
    print("1. List")
    print("2. Download")
    print("3. Upload")
    choice = input("Pilih operasi (1-3): ").strip()
    
    if choice not in ['1', '2', '3']:
        print("Pilihan tidak valid.")
        return None
    
    operation = ['list', 'download', 'upload'][int(choice)-1]
    
    server_workers = int(input("\nServer Workers:\nMasukkan jumlah worker server (default 0): ") or 0)
    client_workers = int(input("\nClient Workers:\nMasukkan jumlah worker client (default 0): ") or 0)
    
    print("\nExecutor Type:")
    print("1. Threads")
    print("2. Processes")
    executor = input("Pilih executor (1-2): ").strip()
    worker_type = 'thread' if executor == '1' else 'process'
    
    return {
        'operation': operation,
        'server_pool_size': server_workers,
        'client_pool_size': client_workers,
        'worker_type': worker_type
    }

if __name__ == '__main__':
    client = FileClient()
    
    while True:
        params = show_menu()
        if not params:
            continue
            
        client.perform_operation(
            operation=params['operation'],
            worker_type=params['worker_type'],
            workers=params['client_pool_size'],
            server_pool_size=params['server_pool_size']
        )
        
        if input("\nLanjutkan testing? (y/n): ").lower() != 'y':
            print(f"\nSemua hasil tes telah disimpan di: {client.csv_filename}")
            break
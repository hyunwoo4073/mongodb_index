import socket
from pymongo import MongoClient
import json
from datetime import datetime
import time

# MongoDB 클라이언트 생성
client = MongoClient('mongodb://192.168.0.31:27017/')
db = client['mydatabase_test']

# 메모리에 데이터를 저장할 리스트
data_list = []

# 데이터 리스트 최대 크기
DATA_LIST_MAX_SIZE = 10000

# 총 데이터 개수
TOTAL_DATA_COUNT = 1000000

# 처리된 데이터의 총량
processed_data_count = 0

# 데이터 삽입 시작 시간
insertion_start_time = None

# Timeseries 컬렉션의 현재 인덱스
current_index = 305

collection_name = "timeseries"

def create_timeseries_collection(collection_name):
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name, timeseries={'timeField': 'timestamp', 'metaField': 'type', 'granularity': 'seconds'})
    return collection_name

def insert_data(collection_name):
    global data_list, processed_data_count, insertion_start_time, current_index

    if not insertion_start_time:
        insertion_start_time = time.time()  # 첫 데이터 처리 시작 시간 측정

    # 데이터 삽입 로직
    collection = db[collection_name]
    collection.insert_many(data_list)  # 실제 MongoDB에 데이터를 삽입합니다.
    data_list.clear()  # 삽입 후 데이터 리스트 초기화

    processed_data_count += DATA_LIST_MAX_SIZE

    # 일정량의 데이터(100만개)가 처리되었는지 확인
    if processed_data_count >= TOTAL_DATA_COUNT:
        # 데이터 삽입 종료 시간 측정 및 총 시간 계산
        insertion_end_time = time.time()
        total_insertion_time = insertion_end_time - insertion_start_time
        print(f"Total insertion time: {total_insertion_time:.2f} seconds for {processed_data_count} documents.")
        exit()  # 서버 종료

def process_data(json_str, collection_name):
    global data_list
    data_dict = json.loads(json_str)
    data_dict['timestamp'] = datetime.fromisoformat(data_dict['timestamp'])
    data_list.append(data_dict)

    if len(data_list) >= DATA_LIST_MAX_SIZE:
        insert_data(collection_name)

def start_server(host, port):
    print(f"Server listening on {host}:{port}")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()

        while True:
            conn, addr = s.accept()
            with conn:
                print(f"Connected by {addr}")
                while True:
                    chunk = conn.recv(10000).decode('utf-8')
                    if not chunk:
                        break  # 연결이 종료되면 반복 종료
                    data_buffer = chunk
                    while '\n' in data_buffer:
                        json_str, _, data_buffer = data_buffer.partition('\n')
                        process_data(json_str, 'timeseries_' + str(current_index))
                    conn.sendall(b"Data processed")

if __name__ == '__main__':
    start_server('192.168.0.31', 65432)

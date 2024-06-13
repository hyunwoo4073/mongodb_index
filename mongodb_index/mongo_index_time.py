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

# Timeseries 컬렉션의 현재 인덱스
current_index = 304

# 데이터 리스트 최대 크기 및 timeseries 컬렉션 문서 한계
DATA_LIST_MAX_SIZE = 1
TIMESERIES_DOC_LIMIT = 1000000

# 처리된 데이터의 총량
processed_data_count = 0

# 데이터 삽입 시작 시간
insertion_start_time = None
total_insertion_time = 0
def create_timeseries_collection(index):
    collection_name = f"timeseries_{index}"
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name, timeseries={'timeField': 'timestamp', 'metaField': 'type', 'granularity': 'seconds'})
    return collection_name

def save_data_and_metadata():
    global current_index, data_list, processed_data_count, insertion_start_time, total_insertion_time

    if not data_list:
        return

    if processed_data_count == 0:  # 데이터 삽입 시작 시간 측정
        insertion_start_time = time.time()

    collection_name = create_timeseries_collection(current_index)
    collection = db[collection_name]
    collection.insert_many(data_list)
    processed_data_count += len(data_list)

    # 메타데이터 업데이트
    timestamps = [data['timestamp'] for data in data_list]
    min_timestamp, max_timestamp = min(timestamps), max(timestamps)
    metadata_collection = db['metadata']
    metadata_doc = metadata_collection.find_one({'collection_name': collection_name})
    if metadata_doc:
        metadata_collection.update_one(
            {'collection_name': collection_name},
            {'$set': {
                'min_timestamp': min(min_timestamp, metadata_doc.get('min_timestamp', min_timestamp)),
                'max_timestamp': max(max_timestamp, metadata_doc.get('max_timestamp', max_timestamp)),
                'count': metadata_doc.get('count', 0) + len(data_list)
            }}
        )
    else:
        metadata_collection.insert_one({
            'collection_name': collection_name,
            'min_timestamp': min_timestamp,
            'max_timestamp': max_timestamp,
            'count': len(data_list)
        })

    data_list = []  # 삽입 후 데이터 리스트 초기화

    # 일정량의 데이터가 처리되었는지 확인
    if processed_data_count >= TIMESERIES_DOC_LIMIT:
        # 데이터 삽입 종료 시간 측정 및 총 시간 계산
        insertion_end_time = time.time()
        total_insertion_time = insertion_end_time - insertion_start_time
        print(f"Total insertion time: {total_insertion_time:.2f} seconds for {processed_data_count} documents.")
        exit()  # 서버 종료

def process_data(json_str):
    global data_list
    data_dict = json.loads(json_str)
    data_dict['timestamp'] = datetime.fromisoformat(data_dict['timestamp'])
    data_list.append(data_dict)
    if len(data_list) >= DATA_LIST_MAX_SIZE:
        save_data_and_metadata()

def start_server(host, port):
    print(f"Server listening on {host}:{port}")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()

        while True:
            conn, addr = s.accept()
            with conn:
                print(f"Connected by {addr}")
                data_buffer = ""
                while True:
                    chunk = conn.recv(10000).decode('utf-8')
                    if not chunk:
                        # 데이터 수신이 끝나면 총 소요 시간 출력
                        # print(f"Total data insertion took {total_insertion_time:.2f} seconds.")
                        break
                    data_buffer += chunk
                    while '\n' in data_buffer:
                        json_str, _, data_buffer = data_buffer.partition('\n')
                        process_data(json_str)
                    conn.sendall(b"Data processed")

if __name__ == '__main__':
    start_server('192.168.0.31', 65432)

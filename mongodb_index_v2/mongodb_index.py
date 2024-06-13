import socket
from pymongo import MongoClient
import json
from datetime import datetime
import time

# MongoDB 클라이언트 생성
client = MongoClient('mongodb://192.168.0.31:27017/')
db = client['query_test']

# 메모리에 데이터를 저장할 리스트
data_list = []

# Timeseries 컬렉션의 현재 인덱스
current_index = 0

# 데이터 리스트 최대 크기 및 timeseries 컬렉션 문서 한계
DATA_LIST_MAX_SIZE = 1
TIMESERIES_DOC_LIMIT = 1000

def create_timeseries_collection(index):
    collection_name = f"timeseries_{index}"
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name, timeseries={'timeField': 'timestamp', 'metaField': 'type', 'granularity': 'seconds'})
    return collection_name

def save_data_and_metadata():
    global current_index
    global data_list

    if data_list:
        collection_name = create_timeseries_collection(current_index)
        collection = db[collection_name]
        collection.insert_many(data_list)

        timestamps = [data['timestamp'] for data in data_list]
        min_timestamp, max_timestamp = min(timestamps), max(timestamps)
        
        content_values = [data['content'] for data in data_list]
        type_values = [data['type'] for data in data_list]

        min_content, max_content = min(content_values), max(content_values)
        min_type, max_type = min(type_values), max(type_values)

        sum_content, avg_content = sum(content_values), sum(content_values) / len(content_values)
        sum_type, avg_type = sum(type_values), sum(type_values) / len(type_values)

        metadata_collection = db['metadata']
        metadata_doc = metadata_collection.find_one({'collection_name': collection_name})

        if metadata_doc:
            new_min_timestamp = min(min_timestamp, metadata_doc['min_timestamp'])
            new_max_timestamp = max(max_timestamp, metadata_doc['max_timestamp'])
            new_count = metadata_doc['count'] + len(data_list)

            new_min_content = min(min_content, metadata_doc['min_content'])
            new_max_content = max(max_content, metadata_doc['max_content'])
            new_sum_content = metadata_doc['sum_content'] + sum_content
            new_avg_content = new_sum_content / new_count

            new_min_type = min(min_type, metadata_doc['min_type'])
            new_max_type = max(max_type, metadata_doc['max_type'])
            new_sum_type = metadata_doc['sum_type'] + sum_type
            new_avg_type = new_sum_type / new_count

            metadata_collection.update_one(
                {'collection_name': collection_name},
                {'$set': {
                    'min_timestamp': new_min_timestamp,
                    'max_timestamp': new_max_timestamp,
                    'count': new_count,
                    'min_content': new_min_content,
                    'max_content': new_max_content,
                    'sum_content': new_sum_content,
                    'avg_content': new_avg_content,
                    'min_type': new_min_type,
                    'max_type': new_max_type,
                    'sum_type': new_sum_type,
                    'avg_type': new_avg_type
                }}
            )
        else:
            metadata_collection.insert_one({
                'collection_name': collection_name,
                'min_timestamp': min_timestamp,
                'max_timestamp': max_timestamp,
                'count': len(data_list),
                'min_content': min_content,
                'max_content': max_content,
                'sum_content': sum_content,
                'avg_content': avg_content,
                'min_type': min_type,
                'max_type': max_type,
                'sum_type': sum_type,
                'avg_type': avg_type
            })
        
        data_list = []
        if collection.count_documents({}) >= TIMESERIES_DOC_LIMIT:
            current_index += 1

def process_data(json_str):
    global data_list
    try:
        data_dict = json.loads(json_str)
        # ISO 8601 형식으로 변환
        data_dict['timestamp'] = datetime.fromisoformat(data_dict['timestamp'])
        data_list.append(data_dict)
        if len(data_list) >= DATA_LIST_MAX_SIZE:
            save_data_and_metadata()
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")

def start_server(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        print(f"Server listening on {host}:{port}")

        while True:
            conn, addr = s.accept()
            with conn:
                print(f"Connected by {addr}")
                data_buffer = ""
                while True:
                    chunk = conn.recv(10000).decode('utf-8')
                    if not chunk:
                        break
                    data_buffer += chunk
                    # JSON 문자열 분리 및 처리
                    while '\n' in data_buffer:
                        json_str, _, data_buffer = data_buffer.partition('\n')
                        process_data(json_str)
                    conn.sendall(b"Data processed")

if __name__ == '__main__':
    start_server('192.168.0.31', 65432)

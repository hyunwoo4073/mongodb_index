import socket
from pymongo import MongoClient
import json
from datetime import datetime

# MongoDB 클라이언트 생성
client = MongoClient('mongodb://192.168.0.31:27017/')
db = client['indexing_test']

# 메모리에 데이터를 저장할 리스트
data_list = []

# Timeseries 컬렉션의 현재 인덱스
# current_index = 1
current_index = 0

# 데이터 리스트 최대 크기 및 timeseries 컬렉션 문서 한계
DATA_LIST_MAX_SIZE = 10000
TIMESERIES_DOC_LIMIT = 1000000

def create_timeseries_collection(index):
    collection_name = f"timeseries_{index}"
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name, timeseries={'timeField': 'timestamp', 'metaField': 'type', 'granularity': 'seconds'})
    return collection_name

def save_data_and_metadata():
    global current_index
    global data_list

    if data_list:
        # 데이터를 timeseries 컬렉션에 저장
        collection_name = create_timeseries_collection(current_index)
        collection = db[collection_name]
        collection.insert_many(data_list)

        # Metadata 계산 및 저장 또는 업데이트
        timestamps = [data['timestamp'] for data in data_list]
        min_timestamp, max_timestamp = min(timestamps), max(timestamps)
        metadata_collection = db['metadata']
        metadata_doc = metadata_collection.find_one({'collection_name': collection_name})
        
        if metadata_doc:
            # 기존 메타데이터 업데이트
            new_min_timestamp = min(min_timestamp, metadata_doc['min_timestamp'])
            new_max_timestamp = max(max_timestamp, metadata_doc['max_timestamp'])
            new_count = metadata_doc['count'] + len(data_list)
            metadata_collection.update_one({'collection_name': collection_name},
                                           {'$set': {'min_timestamp': new_min_timestamp,
                                                     'max_timestamp': new_max_timestamp,
                                                     'count': new_count}})
        else:
            # 새로운 메타데이터 저장
            metadata_collection.insert_one({'collection_name': collection_name,
                                            'min_timestamp': min_timestamp,
                                            'max_timestamp': max_timestamp,
                                            'count': len(data_list)})

        # 준비된 데이터 리스트 초기화
        data_list = []

        # timeseries 컬렉션 문서 개수 확인 및 필요시 다음 컬렉션으로 이동
        if collection.count_documents({}) >= TIMESERIES_DOC_LIMIT:
            current_index += 1

# def process_data(data):
#     global data_list

#     data_dict = json.loads(data)
#     data_dict['timestamp'] = datetime.strptime(data_dict['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')

#     data_list.append(data_dict)

#     # 데이터 리스트가 DATA_LIST_MAX_SIZE에 도달하면 저장 및 메타데이터 업데이트
#     if len(data_list) >= DATA_LIST_MAX_SIZE:
#         save_data_and_metadata()

def process_data(data):
    global data_list

    data_dict = json.loads(data)

    # 밀리초 처리: 밀리초가 없는 경우 '.000'을 추가
    timestamp_str = data_dict['timestamp']
    if '.' not in timestamp_str:
        timestamp_str += '.000'

    data_dict['timestamp'] = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%f')

    data_list.append(data_dict)

    # 데이터 리스트가 DATA_LIST_MAX_SIZE에 도달하면 저장 및 메타데이터 업데이트
    if len(data_list) >= DATA_LIST_MAX_SIZE:
        save_data_and_metadata()


def start_server(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        print(f"Server listening on {host}:{port}")
        
        i = 0
        while True:
            conn, addr = s.accept()
            with conn:
                i += 1   
                # print(f"Connected by {addr}")
                print(f"data inserted {i}")
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    process_data(data.decode('utf-8'))
                    conn.sendall(b"Data processed")

if __name__ == '__main__':
    start_server('192.168.0.31', 65432)

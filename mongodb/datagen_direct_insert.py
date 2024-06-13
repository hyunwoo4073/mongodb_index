# from pymongo import MongoClient
# from datetime import datetime, timedelta
# import random
# from concurrent.futures import ProcessPoolExecutor, as_completed
# import os

# def create_mongo_client():
#     # MongoDB 클라이언트 생성 함수
#     return MongoClient("mongodb://192.168.0.31:27017")

# def insert_data_into_mongodb(documents):
#     # MongoDB에 데이터 삽입 함수
#     client = create_mongo_client()  # MongoDB 클라이언트 생성
#     db = client['timeseries_db']  # 데이터베이스 선택
#     collection = db['timeseries_data2']  # 컬렉션 선택
#     result = collection.insert_many(documents)  # 여러 문서 삽입
#     client.close()  # 클라이언트 연결 종료
#     return result

# def generate_and_insert_batch(start_index, batch_size):
#     # 각 배치 데이터 생성 및 MongoDB에 삽입
#     documents = []
#     for index in range(start_index, start_index + batch_size):
#         content = random.randint(0, 2**32-1)
#         type_ = random.randint(0, 2**32-1)
#         # timestamp를 datetime 객체로 변환
#         timestamp = datetime.now() - timedelta(seconds=index)
#         document = {'content': content, 'type': type_, 'timestamp': timestamp}
#         documents.append(document)
#     insert_data_into_mongodb(documents)

# def generate_and_save_data_in_batches(num_rows, batch_size=10000, num_processes=4):
#     num_batches = (num_rows + batch_size - 1) // batch_size
    
#     with ProcessPoolExecutor(max_workers=num_processes) as executor:
#         futures = {executor.submit(generate_and_insert_batch, i * batch_size, batch_size): i for i in range(num_batches)}
#         for future in as_completed(futures):
#             batch_index = futures[future]
#             print(f"Batch {batch_index + 1}/{num_batches} completed. Progress: {((batch_index + 1)/num_batches)*100:.2f}%")

# # 예시 실행
# generate_and_save_data_in_batches(20000, batch_size=5000, num_processes=4)




# 시간 측정
# from pymongo import MongoClient
# from datetime import datetime, timedelta
# import random
# from concurrent.futures import ProcessPoolExecutor, as_completed
# import os
# import time

# def create_mongo_client():
#     return MongoClient("mongodb://192.168.0.31:27017")

# def insert_data_into_mongodb(documents):
#     client = create_mongo_client()
#     # db = client['timeseries_db']
#     # db = client['indexing_db']
#     db = client['mydatabase']
#     # collection = db['timeseries_data4_minute']
#     # collection = db['indexing_db_1']
#     collection = db['timeseries_303']
#     collection.insert_many(documents)
#     client.close()

# def generate_and_insert_batch(start_index, batch_size):
#     documents = []
#     for index in range(start_index, start_index + batch_size):
#         content = random.randint(0, 2**32-1)
#         type_ = random.randint(0, 2**32-1)
#         timestamp = datetime.now() - timedelta(seconds=index)
#         document = {'content': content, 'type': type_, 'timestamp': timestamp}
#         documents.append(document)
#     insert_data_into_mongodb(documents)

# def generate_and_save_data_in_batches(num_rows, batch_size=1000, num_processes=4):
#     num_batches = (num_rows + batch_size - 1) // batch_size
#     start_time = time.time()  # 전체 작업 시작 시간 기록

#     with ProcessPoolExecutor(max_workers=num_processes) as executor:
#         futures = [executor.submit(generate_and_insert_batch, i * batch_size, batch_size) for i in range(num_batches)]
#         for i, future in enumerate(as_completed(futures)):
#             progress = ((i + 1) / num_batches) * 100
#             print(f"Batch {i + 1}/{num_batches} completed. Progress: {progress:.2f}%.")

#     end_time = time.time()  # 전체 작업 종료 시간 기록
#     total_time = end_time - start_time  # 총 소요 시간 계산
#     docs_per_second = num_rows / total_time  # 초당 삽입된 문서 수 계산
#     print(f"Total time taken to insert {num_rows} documents: {total_time:.2f} seconds. Insert rate: {docs_per_second:.2f} documents per second.")

# # 예시 실행
# generate_and_save_data_in_batches(280000, batch_size=1000, num_processes=10)




# 짤린 부분 데이터 생성
from pymongo import MongoClient
from datetime import datetime, timedelta
import random
import pytz

def create_mongo_client():
    return MongoClient("mongodb://192.168.0.31:27017")

def insert_data_into_mongodb(documents):
    client = create_mongo_client()
    db = client['mydatabase']
    collection = db['timeseries_303']
    collection.insert_many(documents)
    client.close()

def generate_and_insert_batch(start_time, start_index, batch_size):
    documents = []
    for index in range(batch_size):
        content = random.randint(0, 2**32-1)
        type_ = random.randint(0, 2**32-1)
        timestamp = start_time + timedelta(seconds=index + start_index)
        document = {'content': content, 'type': type_, 'timestamp': timestamp}
        documents.append(document)
    insert_data_into_mongodb(documents)

def generate_and_save_data_in_batches(num_rows, start_time_str, batch_size=1000):
    start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    start_time = start_time.replace(tzinfo=pytz.UTC)  # Assume UTC timezone for consistency
    total_batches = (num_rows + batch_size - 1) // batch_size

    for batch_index in range(total_batches):
        start_index = batch_index * batch_size
        generate_and_insert_batch(start_time, start_index, batch_size)
        print(f"Batch {batch_index + 1}/{total_batches} completed.")

if __name__ == "__main__":
    num_rows = 280000  # Total number of documents to insert
    batch_size = 1000  # Number of documents per batch
    start_time_str = '2033-11-08T20:53:34.793Z'  # Start time string in UTC
    generate_and_save_data_in_batches(num_rows, start_time_str, batch_size=batch_size)

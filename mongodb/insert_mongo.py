# 데이터를 하나씩 삽입

# import csv
# from pymongo import MongoClient
# from datetime import datetime, timedelta
# import time

# # MongoDB 클라이언트 연결
# client = MongoClient('mongodb://localhost:27017/')
# db = client['timeseries_db']  # 사용할 데이터베이스 선택

# # 타임시리즈 컬렉션 생성 (이미 존재하지 않는 경우)
# collection_name = 'timeseries_data'
# if collection_name not in db.list_collection_names():
#     db.create_collection(
#         collection_name,
#         timeseries={
#             'timeField': 'timestamp',  # 타임스탬프 필드 지정
#             'metaField': 'type',  # 메타 데이터 필드 (옵션)
#             'granularity': 'seconds'  # 데이터의 시간 간격 설정 (옵션)
#         }
#     )

# # 데이터 삽입 시작 시간
# start_time = time.time()

# # CSV 파일 읽기 및 MongoDB에 데이터 삽입
# with open('generated_data.csv', mode='r') as csvfile:
#     csvreader = csv.DictReader(csvfile)
#     row_count = 0  # 삽입된 데이터 수
#     for row in csvreader:
#         timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
#         content = int(row['content'])
#         type_ = int(row['type'])
        
#         document = {
#             'content': content,
#             'type': type_,
#             'timestamp': timestamp
#         }
        
#         db[collection_name].insert_one(document)
#         row_count += 1

# # 데이터 삽입 종료 시간
# end_time = time.time()

# # 전체 걸린 시간 계산
# total_time = end_time - start_time

# # 초당 삽입된 데이터 수 계산
# data_per_second = row_count / total_time

# print(f"Total time for inserting data: {total_time:.2f} seconds")
# print(f"Data inserted per second: {data_per_second:.2f}")



#데이터를 배치로 삽입
# import csv
# from pymongo import MongoClient
# from datetime import datetime
# import time

# # MongoDB 클라이언트 연결
# client = MongoClient('mongodb://localhost:27017/')
# db = client['timeseries_db']  # 사용할 데이터베이스 선택

# # 타임시리즈 컬렉션 생성 (이미 존재하지 않는 경우)
# collection_name = 'timeseries_data'
# if collection_name not in db.list_collection_names():
#     db.create_collection(
#         collection_name,
#         timeseries={
#             'timeField': 'timestamp',  # 타임스탬프 필드 지정
#             'metaField': 'type',  # 메타 데이터 필드 (옵션)
#             'granularity': 'seconds'  # 데이터의 시간 간격 설정 (옵션)
#         }
#     )

# # 데이터 삽입 시작 시간
# start_time = time.time()

# # CSV 파일 읽기 및 MongoDB에 데이터 삽입
# with open('generated_data.csv', mode='r') as csvfile:
#     csvreader = csv.DictReader(csvfile)
#     documents = []  # 문서를 저장할 리스트
#     batch_size = 100000  # 한 번에 삽입할 문서의 수
#     for row in csvreader:
#         timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
#         content = int(row['content'])
#         type_ = int(row['type'])
        
#         document = {
#             'content': content,
#             'type': type_,
#             'timestamp': timestamp
#         }
        
#         documents.append(document)
        
#         # 배치 크기에 도달하면 MongoDB에 삽입
#         if len(documents) == batch_size:
#             db[collection_name].insert_many(documents)
#             documents = []  # 리스트를 비워 다음 배치를 위해 준비
    
#     # 남은 문서가 있으면 삽입
#     if documents:
#         db[collection_name].insert_many(documents)

# # 데이터 삽입 종료 시간
# end_time = time.time()

# # 전체 걸린 시간 계산
# total_time = end_time - start_time
# # 삽입된 총 문서 수
# total_documents = db[collection_name].count_documents({})

# # 초당 삽입된 데이터 수 계산
# data_per_second = total_documents / total_time

# print(f"Total time for inserting data: {total_time:.2f} seconds")
# print(f"Total documents inserted: {total_documents}")
# print(f"Data inserted per second: {data_per_second:.2f}")





# 메모리에 데이터를 다 올리고 배치처리
# import csv
# from pymongo import MongoClient
# from datetime import datetime
# import time

# # MongoDB 클라이언트 연결
# client = MongoClient('mongodb://localhost:27017/')
# db = client['timeseries_db']

# # 타임시리즈 컬렉션 선택
# collection_name = 'timeseries_data'

# # 배치 크기 설정
# batch_size = 150000  # 한 번에 처리할 문서의 수

# # CSV 파일 읽기
# def read_data(csv_file_path):
#     documents = []  # 전체 문서를 담을 리스트
#     with open(csv_file_path, mode='r') as csvfile:
#         csvreader = csv.DictReader(csvfile)
#         for row in csvreader:
#             timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
#             content = int(row['content'])
#             type_ = int(row['type'])
            
#             document = {
#                 'content': content,
#                 'type': type_,
#                 'timestamp': timestamp
#             }
#             documents.append(document)
#     return documents

# # MongoDB에 데이터 배치 삽입
# def insert_data_in_batches(documents, batch_size):
#     total_inserted = 0  # 총 삽입된 문서 수

#     # 데이터 삽입 시작 시간 측정
#     start_time = time.time()

#     for i in range(0, len(documents), batch_size):
#         batch = documents[i:i+batch_size]
#         db[collection_name].insert_many(batch)
#         total_inserted += len(batch)

#     # 데이터 삽입 종료 시간 측정
#     end_time = time.time()
#     documents = read_data('generated_data.csv')
#     # 삽입에 걸린 시간 계산
#     total_time = end_time - start_time
#     print(f"데이터 삽입에 걸린 총 시간: {total_time:.2f} 초")
#     print(f"초당 삽입된 데이터 수: {total_inserted / total_time:.2f}")
#     print(f"Total documents inserted: {total_documents}")

# # 실행
# total_documents = db[collection_name].count_documents({})


# insert_data_in_batches(documents, batch_size)





# 병렬처리
# 스레드
# import csv
# from pymongo import MongoClient
# from datetime import datetime
# import time
# from concurrent.futures import ThreadPoolExecutor

# # MongoDB 클라이언트 연결
# # client = MongoClient("mongodb://my-user:root@example-mongodb-0.example-mongodb-svc.mongodb.svc.cluster.local:27017/admin?replicaSet=example-mongodb&ssl=false")
# client = MongoClient("mongodb://192.168.0.31:27017")
# db = client['timeseries_db']

# # 타임시리즈 컬렉션 선택
# collection_name = 'timeseries_data'

# # CSV 파일 읽기
# def read_data(csv_file_path):
#     documents = []
#     with open(csv_file_path, mode='r') as csvfile:
#         csvreader = csv.DictReader(csvfile)
#         for row in csvreader:
#             timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
#             content = int(row['content'])
#             type_ = int(row['type'])
#             document = {'content': content, 'type': type_, 'timestamp': timestamp}
#             documents.append(document)
#     return documents

# # MongoDB에 단일 배치 데이터 삽입
# def insert_batch(batch):
#     db[collection_name].insert_many(batch)

# # 메인 함수
# def main(documents, batch_size=40000):
#     # 배치로 분할
#     batches = [documents[i:i+batch_size] for i in range(0, len(documents), batch_size)]
    
#     # 시작 시간 측정
#     start_time = time.time()
    
#     # 병렬 처리
#     with ThreadPoolExecutor() as executor:
#         executor.map(insert_batch, batches)
    
#     # 종료 시간 측정 및 출력
#     end_time = time.time()
#     total_time = end_time - start_time
#     print(f"데이터 삽입에 걸린 총 시간: {total_time:.2f} 초")
#     print(f"초당 삽입된 데이터 수: {len(documents) / total_time:.2f}")

# if __name__ == "__main__":
#     documents = read_data('generated_data_2000000.csv')
#     main(documents)



# 병렬처리
# 프로세스

import csv
from pymongo import MongoClient
from datetime import datetime
import time
from concurrent.futures import ProcessPoolExecutor

# MongoDB 클라이언트 연결을 위한 함수
def create_mongo_client():
    return MongoClient("mongodb://192.168.0.31:27017")

# CSV 파일 읽기 함수
def read_data(csv_file_path):
    documents = []
    with open(csv_file_path, mode='r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            # timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
            timestamp = datetime.fromtimestamp(int(row['timestamp']))
            # content = int(row['content'])
            # type_ = int(row['type'])
            # document = {'content': content, 'type': type_, 'timestamp': timestamp}
            document = {'timestamp': timestamp}
            documents.append(document)
    return documents

# MongoDB에 단일 배치 데이터 삽입을 위한 함수
def insert_batch(batch):
    client = create_mongo_client()  # 각 프로세스에서 MongoDB 클라이언트 생성
    db = client['timeseries_db']
    # collection_name = 'timeseries_data'
    collection_name = 'timeseries_data_timestamp3'
    db[collection_name].insert_many(batch)
    client.close()  # 데이터 삽입 후 클라이언트 연결 종료

# 메인 함수
def main(documents, batch_size, num_threads):
    # 배치로 분할
    batches = [documents[i:i+batch_size] for i in range(0, len(documents), batch_size)]
    
    # 시작 시간 측정
    start_time = time.time()
    
    # 병렬 처리
    with ProcessPoolExecutor(max_workers=num_threads) as executor:
        executor.map(insert_batch, batches)
    
    # 종료 시간 측정 및 출력
    end_time = time.time()
    total_time = end_time - start_time
    print(f"데이터 삽입에 걸린 총 시간: {total_time:.2f} 초")
    print(f"초당 삽입된 데이터 수: {len(documents) / total_time:.2f}")

if __name__ == "__main__":
    # documents = read_data('generated_data_100000000.csv')
    documents = read_data('/home/dblab/data/mongodb/generated_data_20000_type.csv')
    main(documents, batch_size=40000, num_threads=4)
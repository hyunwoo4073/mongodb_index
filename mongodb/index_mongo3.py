from pymongo import MongoClient
from datetime import datetime, timedelta
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

def create_mongo_client():
    return MongoClient("mongodb://192.168.0.31:27017")

def get_collection_name(base_name, index):
    return f"{base_name}_{index}"

def create_collection_if_not_exists(client, collection_name):
    db = client['indexing_db']
    if collection_name not in db.list_collection_names():
        db.create_collection(
            collection_name,
            timeseries={
                'timeField': 'timestamp',
                'metaField': 'type',
                'granularity': 'seconds',
            }
        )
        print(f"Created new collection: {collection_name}")

def insert_data_into_mongodb(collection_name, documents):
    client = create_mongo_client()
    db = client['indexing_db']
    collection = db[collection_name]
    collection.insert_many(documents)
    client.close()

def generate_documents(start_index, batch_size):
    documents = []
    for index in range(start_index, start_index + batch_size):
        content = random.randint(0, 2**32 - 1)
        type_ = random.randint(0, 2**32 - 1)
        timestamp = datetime.now() - timedelta(seconds=index)
        document = {'content': content, 'type': type_, 'timestamp': timestamp}
        documents.append(document)
    return documents

def generate_and_insert_batch(collection_name, start_index, batch_size):
    documents = generate_documents(start_index, batch_size)
    insert_data_into_mongodb(collection_name, documents)

def get_next_collection_index(client, base_name, start_index=1):
    db = client['indexing_db']
    index = start_index
    while True:
        collection_name = get_collection_name(base_name, index)
        if collection_name not in db.list_collection_names():
            return index
        stats = db.command('collStats', collection_name)
        bucketCount = stats.get('timeseries', {}).get('bucketCount', 0)
        if bucketCount >= 1000:
            index += 1
        else:
            # Save metadata for the current collection before moving to the next one
            save_metadata_for_collection(client, collection_name)
            return index

def save_metadata_for_collection(client, collection_name):
    db = client['indexing_db']
    collection = db[collection_name]
    try:
        min_timestamp_doc = collection.find().sort("timestamp", 1).limit(1).next()
        max_timestamp_doc = collection.find().sort("timestamp", -1).limit(1).next()
        # 메타데이터 콜렉션에 정보 저장
        metadata_collection = db['metadata_collection']
        metadata = {
            "collection_name": collection_name,
            "earliest_timestamp": min_timestamp_doc['timestamp'],
            "latest_timestamp": max_timestamp_doc['timestamp']
        }
        metadata_collection.insert_one(metadata)
        print(f"Metadata saved for collection: {collection_name}")
    except StopIteration:
        print(f"No data in collection {collection_name} to save metadata.")

def continuous_data_insertion(base_name, num_rows, batch_size=10000, num_processes=4):
    client = create_mongo_client()
    # Calculate how many batches are needed
    num_batches = (num_rows + batch_size - 1) // batch_size
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        for batch_index in range(num_batches):
            start_index = batch_index * batch_size
            index = get_next_collection_index(client, base_name)
            collection_name = get_collection_name(base_name, index)
            create_collection_if_not_exists(client, collection_name)

            # 데이터 삽입 작업 실행
            generate_and_insert_batch(collection_name, start_index, batch_size)
        
        # 모든 데이터 삽입 작업이 완료된 후, 현재의 콜렉션에 대한 메타데이터 저장
        save_metadata_for_collection(client, collection_name)

if __name__ == "__main__":
    # 이 부분에서 continuous_data_insertion 함수를 호출합니다.
    # 예시: 10,000개의 문서를 삽입하고, 각 배치의 크기는 100, 프로세스 수는 10으로 설정
    num_rows = 10000
    batch_size = 100
    num_processes = 10
    continuous_data_insertion('indexing_db', num_rows, batch_size, num_processes)

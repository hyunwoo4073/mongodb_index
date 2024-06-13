from pymongo import MongoClient
from datetime import datetime, timedelta
import random
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

def check_and_update_collection_if_needed(client, base_name, current_index):
    collection_name = get_collection_name(base_name, current_index)
    db = client['indexing_db']
    collection = db[collection_name]
    count = collection.count_documents({})
    if count >= 10000000:  # 예: 콜렉션 당 최대 10,000개 문서로 설정
        # 현재 콜렉션에 대한 메타데이터 저장
        save_metadata_for_collection(client, collection_name)
        # 다음 콜렉션으로 이동
        return current_index + 1
    return current_index

def save_metadata_for_collection(client, collection_name):
    db = client['indexing_db']
    collection = db[collection_name]
    try:
        min_timestamp_doc = collection.find().sort("timestamp", 1).limit(1).next()
        max_timestamp_doc = collection.find().sort("timestamp", -1).limit(1).next()
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

def generate_documents(start_index, batch_size):
    documents = []
    for i in range(batch_size):
        timestamp = datetime.now() + timedelta(seconds=start_index + i)
        document = {
            "content": random.randint(0, 2**32 - 1),
            "type": random.randint(0, 2**32 - 1),
            "timestamp": timestamp
        }
        documents.append(document)
    return documents

def insert_data_into_mongodb(collection_name, documents):
    client = create_mongo_client()
    db = client['indexing_db']
    collection = db[collection_name]
    collection.insert_many(documents)

def continuous_data_insertion(base_name, num_rows, batch_size=100):
    client = create_mongo_client()
    index = 1  # 시작 콜렉션 인덱스
    total_inserted = 0

    while total_inserted < num_rows:
        index = check_and_update_collection_if_needed(client, base_name, index)
        collection_name = get_collection_name(base_name, index)
        create_collection_if_not_exists(client, collection_name)

        # 데이터 삽입
        documents = generate_documents(total_inserted, min(batch_size, num_rows - total_inserted))
        insert_data_into_mongodb(collection_name, documents)

        total_inserted += len(documents)

if __name__ == "__main__":
    num_rows = 100000001
    batch_size = 10000
    continuous_data_insertion('indexing_db', num_rows, batch_size)
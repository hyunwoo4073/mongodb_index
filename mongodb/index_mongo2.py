from pymongo import MongoClient
from datetime import datetime, timedelta
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
            return index

def save_metadata_for_collection(client, collection_name):
    db = client['indexing_db']
    collection = db[collection_name]
    
    try:
        min_timestamp_doc = collection.find().sort("timestamp", 1).limit(1).next()
        earliest_timestamp = min_timestamp_doc['timestamp']
    except StopIteration:
        earliest_timestamp = None  # 또는 적절한 기본값 설정
    
    try:
        max_timestamp_doc = collection.find().sort("timestamp", -1).limit(1).next()
        latest_timestamp = max_timestamp_doc['timestamp']
    except StopIteration:
        latest_timestamp = None  # 또는 적절한 기본값 설정
    
    if earliest_timestamp is not None and latest_timestamp is not None:
        metadata_collection = db['metadata_collection']
        metadata = {
            "collection_name": collection_name,
            "earliest_timestamp": earliest_timestamp,
            "latest_timestamp": latest_timestamp
        }
        metadata_collection.insert_one(metadata)
        print(f"Metadata saved for collection: {collection_name}")
    else:
        print(f"No documents in collection: {collection_name}, no metadata saved.")

def monitor_and_update_metadata(base_name):
    client = create_mongo_client()
    while True:
        index = get_next_collection_index(client, base_name)
        collection_name = get_collection_name(base_name, index)
        # 콜렉션 생성은 데이터 삽입 스크립트에서 처리되므로 여기서는 생략
        save_metadata_for_collection(client, collection_name)
        time.sleep(5)  # 60초 대기 후 다시 확인

if __name__ == "__main__":
    monitor_and_update_metadata('indexing_db')

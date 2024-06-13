from pymongo import MongoClient
from datetime import datetime, timedelta
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import time
from pymongo.errors import CollectionInvalid

# def create_mongo_client():
#     return MongoClient("mongodb://192.168.0.31:27017")

# def Create_Collection():
#     client = create_mongo_client()
#     db = client['timeseries_db']
#     collection = db.create_collection('indexing_db', timeseries={
#         'timeField': "timestamp",
#         'metaField': "type",
#         'granularity': "seconds"
#         }
#     )


# Create_Collection()

# client = MongoClient("mongodb://192.168.0.31:27017")
# db = client['indexing_db']

# def create_timeseries_collection(indexing_db, index):
#     collection_name = f"{indexing_db}_{index}"
#     try:
#         db.create_collection(
#             collection_name,
#             timeseries={
#                 'timeField': 'timestamp',
#                 'metaField': 'type',
#                 'granularity': 'seconds',
#             }
#         )
#         print(f"Created timeseries collection: {collection_name}")
#     except CollectionInvalid:
#         print(f"Collection {collection_name} already exists. Exiting.")
#         raise SystemExit  # 프로그램 종료
#     return collection_name

# def create_metadata_collection(db, name='metadata_collection'):
#     try:
#         if name not in db.list_collection_names():
#             db.create_collection(name)
#             print(f"Created metadata collection: {name}")
#         else:
#             print(f"Metadata collection {name} already exists.")
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         raise SystemExit  # 프로그램 종료
#     return db[name]

# def count_bucket(collection_name):
#     stats = db.command('collstats', collection_name)
#     bucketCount = stats['bucketCount']
#     return bucketCount

# collection_name = create_timeseries_collection('indexing_db', 1)

# bucket_count = count_bucket(collection_name)

# print(f"bucket count: {count_bucket()}")


# def insert_documents(data, base_collection_name='timeseries', max_docs_per_collection=1000000000):
#     metadata_collection = create_metadata_collection()
#     collection_index = metadata_collection.count_documents({})
#     collection_name = create_timeseries_collection(base_collection_name, collection_index)
#     current_collection = db[collection_name]
    
#     for doc in data:
#         if current_collection.count_documents({}) >= max_docs_per_collection:
#             # 현재 컬렉션의 마지막 timestamp 업데이트
#             last_timestamp = current_collection.find().sort('timestamp', -1).limit(1)[0]['timestamp']
#             metadata_collection.update_one({'name': collection_name}, {'$set': {'end_timestamp': last_timestamp}}, upsert=True)
            
#             # 새로운 컬렉션 생성
#             collection_index += 1
#             collection_name = create_timeseries_collection(base_collection_name, collection_index)
#             current_collection = db[collection_name]
        
#         current_collection.insert_one(doc)
        
#         # 처음 문서를 삽입할 때 시작 timestamp 저장
#         if current_collection.count_documents({}) == 1:
#             metadata_collection.insert_one({'name': collection_name, 'start_timestamp': doc['timestamp']})


# def query_data(start_timestamp, end_timestamp):
#     metadata_collection = create_metadata_collection()
#     relevant_collections = metadata_collection.find({
#         '$or': [
#             {'start_timestamp': {'$lte': end_timestamp}, 'end_timestamp': {'$gte': start_timestamp}},
#             {'start_timestamp': {'$lte': start_timestamp}, 'end_timestamp': {'$exists': False}}
#         ]
#     })
    
#     results = []
#     for meta in relevant_collections:
#         collection = db[meta['name']]
#         results.extend(collection.find({'timestamp': {'$gte': start_timestamp, '$lte': end_timestamp}}))
    
#     return results




client = MongoClient("mongodb://192.168.0.31:27017")
db = client['indexing_db']

def create_timeseries_collection(db, indexing_db, index):
    collection_name = f"{indexing_db}_{index}"
    if collection_name not in db.list_collection_names():
        try:
            db.create_collection(
                collection_name,
                timeseries={
                    'timeField': 'timestamp',
                    'metaField': 'type',
                    'granularity': 'seconds',
                }
            )
            print(f"Created timeseries collection: {collection_name}")
        except Exception as e:
            print(f"An error occurred while creating the collection: {e}")
            return None
    else:
        print(f"Collection {collection_name} already exists. Proceeding to count buckets.")
    return collection_name

def create_metadata_collection(db, name='metadata_collection'):
    if name not in db.list_collection_names():
        db.create_collection(name)
        print(f"Created metadata collection: {name}")
    else:
        print(f"Metadata collection {name} already exists.")
    return db[name]

def count_bucket(db, collection_name):
    stats = db.command('collstats', collection_name)
    bucketCount = stats.get('timeseries', {}).get('bucketCount', 'Stats not available')
    return bucketCount

index = 1
collection_name = create_timeseries_collection(db, 'indexing_db', index)

if collection_name:
    bucket_count = count_bucket(db, collection_name)
    print(f"Bucket count: {bucket_count}")
else:
    print("Exiting without counting buckets due to an issue with collection creation.")
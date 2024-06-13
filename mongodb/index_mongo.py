# from pymongo import MongoClient
# import time

# client = MongoClient("mongodb://192.168.0.31:27017")
# db = client['indexing_db']

# def get_collection_name(base_name, index):
#     return f"{base_name}_{index}"

# def create_collection_if_not_exists(collection_name):
#     if collection_name not in db.list_collection_names():
#         db.create_collection(
#             collection_name,
#             timeseries={
#                 'timeField': 'timestamp',
#                 'metaField': 'type',
#                 'granularity': 'seconds',
#             }
#         )
#         print(f"Created new collection: {collection_name}")
#     else:
#         print(f"Collection {collection_name} already exists.")

# def get_next_collection_index(base_name, start_index=1):
#     index = start_index
#     while True:
#         collection_name = get_collection_name(base_name, index)
#         stats = db.command('collStats', collection_name)
#         bucketCount = stats.get('timeseries', {}).get('bucketCount', 0)
        
#         if bucketCount >= 100000:  # If bucket count is 1 billion or more, move to the next collection
#             index += 1
#         else:
#             break  # Use the current collection
#     return index

# def continuous_data_insertion(base_name):
#     index = 1
#     while True:
#         # Get the current or next collection name based on the bucket count
#         index = get_next_collection_index(base_name, index)
#         collection_name = get_collection_name(base_name, index)
        
#         # Ensure the collection exists
#         create_collection_if_not_exists(collection_name)
        
#         # Simulate data insertion (Replace this part with actual data insertion logic)
#         print(f"Inserting data into {collection_name}...")
#         # Example: db[collection_name].insert_one({"data": "your_data_here"})
        
#         # Wait for a bit before inserting next data (for demonstration purposes)
#         time.sleep(1)  # Adjust or remove according to your actual use case

# # Start the continuous data insertion process
# continuous_data_insertion('indexing_db')






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
            return index

# def continuous_data_insertion(base_name, num_rows, batch_size=10000, num_processes=4):
#     client = create_mongo_client()
#     num_batches = (num_rows + batch_size - 1) // batch_size
    
#     with ProcessPoolExecutor(max_workers=num_processes) as executor:
#         for batch_index in range(num_batches):
#             start_index = batch_index * batch_size
#             index = get_next_collection_index(client, base_name)
#             collection_name = get_collection_name(base_name, index)
#             create_collection_if_not_exists(client, collection_name)
#             executor.submit(generate_and_insert_batch, collection_name, start_index, batch_size)

def continuous_data_insertion(base_name, num_rows, batch_size=10000, num_processes=4):
    client = create_mongo_client()
    num_batches = (num_rows + batch_size - 1) // batch_size
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = []
        for batch_index in range(num_batches):
            start_index = batch_index * batch_size
            index = get_next_collection_index(client, base_name)
            collection_name = get_collection_name(base_name, index)
            create_collection_if_not_exists(client, collection_name)
            
            # 데이터 삽입 작업 실행
            future = executor.submit(generate_and_insert_batch, collection_name, start_index, batch_size)
            futures.append((future, collection_name))
        
        # 모든 데이터 삽입 작업 완료 대기
        for future, collection_name in futures:
            result = future.result()
            
            # 메타데이터 저장
            save_metadata_for_collection(client, collection_name)

def save_metadata_for_collection(client, collection_name):
    db = client['indexing_db']
    collection = db[collection_name]
    
    # 최소 및 최대 timestamp 조회
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

# Start the continuous data insertion process with parameters
num_rows = 10000
batch_size = 100
num_processes = 10
continuous_data_insertion('indexing_db', num_rows, batch_size, num_processes)

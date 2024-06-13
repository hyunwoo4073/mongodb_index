from pymongo import MongoClient
from datetime import datetime
import time

def find_collections_and_counts_in_time_range(start_time, end_time):
    client = MongoClient('mongodb://192.168.0.31:27017/')
    db = client['indexing_test']
    metadata_collection = db['metadata']

    query = {
        'min_timestamp': {'$lte': end_time},
        'max_timestamp': {'$gte': start_time}
    }
    collections = metadata_collection.find(query, {'collection_name': 1, 'count': 1, '_id': 0})
    return list(collections)

def aggregate_data_from_collections(collection_data, start_time, end_time):
    client = MongoClient('mongodb://192.168.0.31:27017/')
    db = client['indexing_test']
    total_count = 0

    if not collection_data:
        print("No collections found within the specified time range.")
        return 0, 0  # 반환 값은 카운트 0과 쿼리 시간 0

    first_collection = collection_data[0]['collection_name']
    last_collection = collection_data[-1]['collection_name']

    # 전체 쿼리 시간 측정 시작
    total_query_start_time = time.time()

    # 첫번째와 마지막 컬렉션 실제 count 집계
    for collection_name in [first_collection, last_collection]:
        count = db[collection_name].count_documents({'timestamp': {'$gte': start_time, '$lte': end_time}})
        total_count += count
        print(f"Actual count from {collection_name}: {count}")

    # 나머지 컬렉션은 metadata에서 count 정보 사용
    for col in collection_data[1:-1]:  # 첫 번째와 마지막 제외
        total_count += col['count']
        print(f"Metadata count from {col['collection_name']}: {col['count']}")

    # 전체 쿼리 시간 측정 종료
    total_query_end_time = time.time()
    total_query_time = total_query_end_time - total_query_start_time

    return total_count, total_query_time

if __name__ == "__main__":
    start_time = datetime(2034, 7, 4, 0, 0)
    end_time = datetime(2036, 10, 4, 0, 0)

    collection_data = find_collections_and_counts_in_time_range(start_time, end_time)
    if collection_data:
        total_count, total_query_time = aggregate_data_from_collections(collection_data, start_time, end_time)
        print(f"Total count of documents: {total_count}")
        print(f"Total query time: {total_query_time:.2f} seconds")
    else:
        print("No data found in the specified range.")

# from pymongo import MongoClient
# from datetime import datetime
# import time  # 시간 측정을 위해 time 모듈 추가

# def find_collections_in_time_range(start_time, end_time):
#     client = MongoClient('mongodb://192.168.0.31:27017/')
#     db = client['indexing_test']
#     metadata_collection = db['metadata']

#     query = {
#         'min_timestamp': {'$lte': end_time},
#         'max_timestamp': {'$gte': start_time}
#     }
#     collections = metadata_collection.find(query)
#     collection_names = [col['collection_name'] for col in collections]
#     return collection_names

# def aggregate_data_in_collections(collection_names, start_time, end_time, aggregation_type, field=None):
#     client = MongoClient('mongodb://192.168.0.31:27017/')
#     db = client['indexing_test']
#     metadata_collection = db['metadata']
#     individual_results = []
#     total_aggregate_result = None

#     # 전체 쿼리 시간 측정 시작
#     total_query_start_time = time.time()

#     for name in collection_names:
#         metadata_doc = metadata_collection.find_one({'collection_name': name})
        
#         if metadata_doc:
#             if aggregation_type == 'count':
#                 result_value = metadata_doc['count']
#             else:
#                 if aggregation_type == 'sum':
#                     result_value = metadata_doc[f'sum_{field}']
#                 elif aggregation_type == 'min':
#                     result_value = metadata_doc[f'min_{field}']
#                 elif aggregation_type == 'max':
#                     result_value = metadata_doc[f'max_{field}']
#                 elif aggregation_type == 'avg':
#                     result_value = metadata_doc[f'avg_{field}']
#                 else:
#                     raise ValueError(f"Unsupported aggregation type: {aggregation_type}")

#             individual_results.append({name: result_value})

#             if total_aggregate_result is None:
#                 total_aggregate_result = result_value
#             else:
#                 if aggregation_type == 'sum':
#                     total_aggregate_result += result_value
#                 elif aggregation_type == 'min':
#                     total_aggregate_result = min(total_aggregate_result, result_value)
#                 elif aggregation_type == 'max':
#                     total_aggregate_result = max(total_aggregate_result, result_value)
#                 elif aggregation_type == 'avg':
#                     # 평균의 경우 가중 평균을 계산해야 함
#                     count = metadata_doc['count']
#                     total_count = sum([metadata_collection.find_one({'collection_name': name})['count'] for name in collection_names])
#                     total_aggregate_result = ((total_aggregate_result * (total_count - count)) + (result_value * count)) / total_count

#     # 전체 쿼리 시간 측정 종료
#     total_query_end_time = time.time()
#     total_query_time = total_query_end_time - total_query_start_time

#     return individual_results, total_aggregate_result, total_query_time

# if __name__ == "__main__":
#     start_time = datetime(2034, 1, 4, 0, 0)
#     end_time = datetime(2034, 4, 4, 0, 0)
#     aggregation_type = 'max'  # Or 'sum', 'min', 'max', 'average'
#     field = 'type'  # Specify the field for sum, min, max, and average aggregations

#     collection_names = find_collections_in_time_range(start_time, end_time)
#     print(f"Found collections: {collection_names}")

#     individual_results, total_aggregate_result, total_query_time = aggregate_data_in_collections(collection_names, start_time, end_time, aggregation_type, field)
#     print(f"Aggregation Type: {aggregation_type}")
#     print(f"Field Name: {field}")
#     print(f"Individual Aggregation Results: {individual_results}")
#     print(f"Total Aggregation: {total_aggregate_result}")
#     print(f"Total query time: {total_query_time:.2f} seconds")


from pymongo import MongoClient
from datetime import datetime
import time  # 시간 측정을 위해 time 모듈 추가

def find_collections_in_time_range(start_time, end_time):
    client = MongoClient('mongodb://192.168.0.31:27017/')
    db = client['indexing_test']
    metadata_collection = db['metadata']

    query = {
        'min_timestamp': {'$lte': end_time},
        'max_timestamp': {'$gte': start_time}
    }
    collections = metadata_collection.find(query)
    collection_names = [col['collection_name'] for col in collections]
    return collection_names

def aggregate_data_in_collections(collection_names, start_time, end_time, aggregation_type, field=None):
    client = MongoClient('mongodb://192.168.0.31:27017/')
    db = client['indexing_test']
    metadata_collection = db['metadata']
    individual_results = []
    total_aggregate_result = None

    # 전체 쿼리 시간 측정 시작
    total_query_start_time = time.time()

    for name in collection_names:
        metadata_doc = metadata_collection.find_one({'collection_name': name})
        
        if metadata_doc:
            if aggregation_type == 'count':
                result_value = metadata_doc['count']
            else:
                if aggregation_type == 'sum':
                    result_value = metadata_doc[f'sum_{field}']
                elif aggregation_type == 'min':
                    result_value = metadata_doc[f'min_{field}']
                elif aggregation_type == 'max':
                    result_value = metadata_doc[f'max_{field}']
                elif aggregation_type == 'avg':
                    result_value = metadata_doc[f'avg_{field}']
                else:
                    raise ValueError(f"Unsupported aggregation type: {aggregation_type}")

            individual_results.append({name: result_value})

            if total_aggregate_result is None:
                total_aggregate_result = result_value
            else:
                if aggregation_type == 'sum':
                    total_aggregate_result += result_value
                elif aggregation_type == 'min':
                    total_aggregate_result = min(total_aggregate_result, result_value)
                elif aggregation_type == 'max':
                    total_aggregate_result = max(total_aggregate_result, result_value)
                elif aggregation_type == 'avg':
                    # 평균의 경우 가중 평균을 계산해야 함
                    count = metadata_doc['count']
                    total_count = sum([metadata_collection.find_one({'collection_name': name})['count'] for name in collection_names])
                    total_aggregate_result = ((total_aggregate_result * (total_count - count)) + (result_value * count)) / total_count

    # 전체 쿼리 시간 측정 종료
    total_query_end_time = time.time()
    total_query_time = total_query_end_time - total_query_start_time

    return individual_results, total_aggregate_result, total_query_time

def query_data_in_collections(collection_names, start_time, end_time):
    client = MongoClient('mongodb://192.168.0.31:27017/')
    db = client['indexing_test']
    results = []

    # 전체 쿼리 시간 측정 시작
    total_query_start_time = time.time()

    for name in collection_names:
        collection = db[name]
        query = {
            'timestamp': {
                '$gte': start_time,
                '$lt': end_time
            }
        }
        collection_results = list(collection.find(query))
        results.extend(collection_results)

    # 전체 쿼리 시간 측정 종료
    total_query_end_time = time.time()
    total_query_time = total_query_end_time - total_query_start_time

    return results, total_query_time

if __name__ == "__main__":
    start_time = datetime(2034, 3, 4, 0, 0)
    end_time = datetime(2035, 4, 4, 0, 0)
    
    # 사용자로부터 작업 선택
    action = input("Choose action (aggregate/query): ").strip().lower()
    
    if action == 'aggregate':
        aggregation_type = input("Choose aggregation type (count/sum/min/max/avg): ").strip().lower()
        field = None
        if aggregation_type in ['sum', 'min', 'max', 'avg']:
            field = input("Specify the field for aggregation: ").strip()
        
        collection_names = find_collections_in_time_range(start_time, end_time)
        print(f"Found collections: {collection_names}")

        individual_results, total_aggregate_result, total_query_time = aggregate_data_in_collections(collection_names, start_time, end_time, aggregation_type, field)
        print(f"Aggregation Type: {aggregation_type}")
        if field:
            print(f"Field Name: {field}")
        print(f"Individual Aggregation Results: {individual_results}")
        print(f"Total Aggregation: {total_aggregate_result}")
        print(f"Total aggregation query time: {total_query_time:.2f} seconds")

    elif action == 'query':
        collection_names = find_collections_in_time_range(start_time, end_time)
        print(f"Found collections: {collection_names}")

        query_results, query_time = query_data_in_collections(collection_names, start_time, end_time)
        print(f"Total documents found: {len(query_results)}")
        print(f"Total data retrieval query time: {query_time:.2f} seconds")
        # for result in query_results:
        #     print(result)
    
    else:
        print("Invalid action selected.")

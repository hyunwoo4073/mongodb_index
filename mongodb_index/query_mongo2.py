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
    individual_results = []
    total_aggregate_result = {}

    # 전체 쿼리 시간 측정 시작
    total_query_start_time = time.time()

    for name in collection_names:
        collection = db[name]
        match_query = {'$match': {'timestamp': {'$gte': start_time, '$lte': end_time}}}
        aggregate_query = []

        if aggregation_type == 'count':
            aggregate_query = [match_query, {'$count': 'total_count'}]
        else:
            group_query = {
                '$group': {
                    '_id': None,
                    'result': {
                        '${}'.format(aggregation_type): "${}".format(field)
                    }
                }
            }
            aggregate_query = [match_query, group_query]
        
        aggregation_result = list(collection.aggregate(aggregate_query))
        if aggregation_result:
            result_key = 'total_count' if aggregation_type == 'count' else 'result'
            result_value = aggregation_result[0][result_key]
            individual_results.append({name: result_value})

            if aggregation_type == 'count':
                total_aggregate_result[aggregation_type] = total_aggregate_result.get(aggregation_type, 0) + result_value
            else:
                if aggregation_type in ['sum', 'min', 'max']:
                    if aggregation_type not in total_aggregate_result:
                        total_aggregate_result[aggregation_type] = result_value
                    else:
                        if aggregation_type == 'sum':
                            total_aggregate_result[aggregation_type] += result_value
                        elif aggregation_type == 'min':
                            total_aggregate_result[aggregation_type] = min(total_aggregate_result[aggregation_type], result_value)
                        elif aggregation_type == 'max':
                            total_aggregate_result[aggregation_type] = max(total_aggregate_result[aggregation_type], result_value)

    # 전체 쿼리 시간 측정 종료
    total_query_end_time = time.time()
    total_query_time = total_query_end_time - total_query_start_time

    return individual_results, total_aggregate_result, total_query_time

if __name__ == "__main__":
    start_time = datetime(2034, 1, 1, 0, 0)
    end_time = datetime(2036, 1, 1, 0, 0)
    aggregation_type = 'count'  # Or 'sum', 'min', 'max', 'average'
    field = 'type'  # Specify the field for sum, min, max, and average aggregations

    collection_names = find_collections_in_time_range(start_time, end_time)
    print(f"Found collections: {collection_names}")

    individual_results, total_aggregate_result, total_query_time = aggregate_data_in_collections(collection_names, start_time, end_time, aggregation_type, field)
    print(f"Aggregation Type: {aggregation_type}")
    print(f"Field Name: {field}")
    print(f"Individual Aggregation Results: {individual_results}")
    print(f"Total Aggregation: {total_aggregate_result}")
    print(f"Total query time: {total_query_time:.2f} seconds")

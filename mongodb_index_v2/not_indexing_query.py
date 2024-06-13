# from pymongo import MongoClient
# from datetime import datetime
# import time

# def get_aggregation_pipeline(start_time, end_time, aggregation_type, field):
#     match_stage = {
#         '$match': {
#             'timestamp': {
#                 '$gte': start_time,
#                 '$lt': end_time
#             }
#         }
#     }
#     if aggregation_type == 'count':
#         return [match_stage, {'$count': 'total_documents'}]
#     elif aggregation_type == 'min':
#         return [match_stage, {'$group': {'_id': None, 'min_value': {'$min': f'${field}'}}}]
#     elif aggregation_type == 'max':
#         return [match_stage, {'$group': {'_id': None, 'max_value': {'$max': f'${field}'}}}]
#     elif aggregation_type == 'sum':
#         return [match_stage, {'$group': {'_id': None, 'sum_value': {'$sum': f'${field}'}}}]
#     elif aggregation_type == 'avg':
#         return [match_stage, {'$group': {'_id': None, 'avg_value': {'$avg': f'${field}'}}}]
#     else:
#         raise ValueError(f"Unsupported aggregation type: {aggregation_type}")

# # MongoDB 클라이언트 연결
# client = MongoClient("mongodb://192.168.0.31:27017")
# db = client['timeseries_db']
# collection_name = 'timeseries_data'

# # 특정 시간 범위 설정
# start_time = datetime(2017, 7, 4, 0, 0, 0)  # 예시 시작 시간
# end_time = datetime(2027, 10, 4, 0, 0, 0)    # 예시 종료 시간

# # 사용자로부터 집계 유형과 필드 입력 받기
# aggregation_type = 'max'  # 'count', 'min', 'max', 'sum', 'avg' 중 하나
# field = 'type'  # 집계할 필드 이름

# # 집계 파이프라인 생성
# aggregation_pipeline = get_aggregation_pipeline(start_time, end_time, aggregation_type, field)

# # 쿼리 실행 및 시간 측정
# query_start_time = time.time()
# results = db[collection_name].aggregate(aggregation_pipeline)
# query_end_time = time.time()
# total_time = query_end_time - query_start_time

# # explain 결과 출력
# explain_result = db.command(
#     'explain', {
#         'aggregate': collection_name,
#         'pipeline': aggregation_pipeline,
#         'cursor': {}
#     },
#     verbosity='executionStats'
# )
# print(explain_result)

# # 결과 출력
# for result in results:
#     print(result)
# print(f"쿼리 시간: {total_time:.2f} 초")


from pymongo import MongoClient
from datetime import datetime
import time

def get_aggregation_pipeline(start_time, end_time, aggregation_type, field):
    match_stage = {
        '$match': {
            'timestamp': {
                '$gte': start_time,
                '$lt': end_time
            }
        }
    }
    if aggregation_type == 'count':
        return [match_stage, {'$count': 'total_documents'}]
    elif aggregation_type == 'min':
        return [match_stage, {'$group': {'_id': None, 'min_value': {'$min': f'${field}'}}}]
    elif aggregation_type == 'max':
        return [match_stage, {'$group': {'_id': None, 'max_value': {'$max': f'${field}'}}}]
    elif aggregation_type == 'sum':
        return [match_stage, {'$group': {'_id': None, 'sum_value': {'$sum': f'${field}'}}}]
    elif aggregation_type == 'avg':
        return [match_stage, {'$group': {'_id': None, 'avg_value': {'$avg': f'${field}'}}}]
    else:
        raise ValueError(f"Unsupported aggregation type: {aggregation_type}")

def query_data_in_time_range(collection, start_time, end_time):
    query = {
        'timestamp': {
            '$gte': start_time,
            '$lt': end_time
        }
    }
    return list(collection.find(query))

# MongoDB 클라이언트 연결
client = MongoClient("mongodb://192.168.0.31:27017")
db = client['timeseries_db']
collection_name = 'timeseries_data'

# 특정 시간 범위 설정
start_time = datetime(2021, 7, 4, 0, 0, 0)  # 예시 시작 시간
end_time = datetime(2022, 8, 4, 0, 0, 0)  # 예시 종료 시간

# 사용자로부터 작업 유형 입력 받기
action = input("Choose action (aggregate/query): ").strip().lower()

if action == 'aggregate':
    # 사용자로부터 집계 유형과 필드 입력 받기
    aggregation_type = input("Choose aggregation type (count/sum/min/max/avg): ").strip().lower()
    field = None
    if aggregation_type in ['sum', 'min', 'max', 'avg']:
        field = input("Specify the field for aggregation: ").strip()

    # 집계 파이프라인 생성
    aggregation_pipeline = get_aggregation_pipeline(start_time, end_time, aggregation_type, field)

    # 쿼리 실행 및 시간 측정
    query_start_time = time.time()
    results = db[collection_name].aggregate(aggregation_pipeline)
    query_end_time = time.time()
    total_time = query_end_time - query_start_time

    # explain 결과 출력
    explain_result = db.command(
        'explain', {
            'aggregate': collection_name,
            'pipeline': aggregation_pipeline,
            'cursor': {}
        },
        verbosity='executionStats'
    )
    print(explain_result)

    # 결과 출력
    for result in results:
        print(result)
    print(f"Aggregation query time: {total_time:.2f} seconds")

elif action == 'query':
    # 데이터 조회 쿼리 실행 및 시간 측정
    collection = db[collection_name]
    query_start_time = time.time()
    results = query_data_in_time_range(collection, start_time, end_time)
    query_end_time = time.time()
    total_time = query_end_time - query_start_time

    # explain 결과 출력
    explain_result = db.command(
        'explain', {
            'find': collection_name,
            'filter': {
                'timestamp': {
                    '$gte': start_time,
                    '$lt': end_time
                }
            }
        },
        verbosity='executionStats'
    )
    print(explain_result)

    # 결과 출력 (여기서는 출력 생략 가능)
    print(f"Total documents found: {len(results)}")
    print(f"Data query time: {total_time:.2f} seconds")

else:
    print("Invalid action selected.")

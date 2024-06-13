from pymongo import MongoClient
from datetime import datetime
import time

# MongoDB 클라이언트 연결
client = MongoClient("mongodb://192.168.0.31:27017")
db = client['timeseries_db']
collection_name = 'timeseries_data'

# 특정 시간 범위 설정
start_time = datetime(2021, 1, 4, 0, 0, 0)  # 예시 시작 시간
end_time = datetime(2023, 4, 4, 0, 0, 0)    # 예시 종료 시간

# 시간 범위 내 문서 개수 계산 쿼리
aggregation_pipeline = [
    {
        '$match': {
            'timestamp': {
                '$gte': start_time,
                '$lt': end_time
            }
        }
    },
    {
        '$count': 'total_documents'
    }
    #  {
    #     '$group': {
    #         '_id': None,  # 모든 문서를 하나의 그룹으로 처리
    #         'total_sum': {
    #             '$sum': '$type'  # 'type' 필드의 합계
    #         }
    #     }
    # }
    # {
    #     '$group': {
    #         '_id': None,  # 모든 문서를 하나의 그룹으로 처리
    #         'min_type_value': {
    #             '$min': '$type'  # 'type' 필드의 최소값 계산
    #         }
    #     }
    # }
]
query_start_time = time.time()
results = db[collection_name].aggregate(aggregation_pipeline)

query_end_time = time.time()

total_time = query_end_time - query_start_time

# query_plan = db.command('aggregate', collection_name, pipeline=aggregation_pipeline, explain=True)
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
print(f"쿼리 시간: {total_time:.2f} 초")
# print(query_plan)
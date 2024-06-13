from pymongo import MongoClient
from datetime import datetime
import time

# MongoDB 클라이언트 연결
client = MongoClient("mongodb://192.168.0.31:27017")
db = client['indexing_db']
collection_name = 'indexing_db_1'

# 특정 시간 범위 설정
start_time = datetime(1991, 7, 4, 0, 0, 0)  # 예시 시작 시간
end_time = datetime(2000, 7, 4, 0, 30, 0)    # 예시 종료 시간

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
        '$min': 'total_documents'
    },
    {
        '$count': 'total_documents'
    },
    {
        '$count': 'total_documents'
    },
    {
        '$count': 'total_documents'
    }
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
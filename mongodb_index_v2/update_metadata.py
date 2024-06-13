from pymongo import MongoClient

# MongoDB 클라이언트 생성
client = MongoClient('mongodb://192.168.0.31:27017/')
db = client['indexing_test']
metadata_collection = db['metadata']

# Timeseries 컬렉션을 가져오는 함수
def get_timeseries_collections():
    collections = db.list_collection_names()
    return [col for col in collections if col.startswith('timeseries_')]

# 메타데이터 업데이트 함수
def update_metadata():
    timeseries_collections = get_timeseries_collections()
    
    for collection_name in timeseries_collections:
        collection = db[collection_name]

        # type 및 content 값의 통계 계산
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'min_type': {'$min': '$type'},
                    'max_type': {'$max': '$type'},
                    'sum_type': {'$sum': '$type'},
                    'avg_type': {'$avg': '$type'},
                    'min_content': {'$min': '$content'},
                    'max_content': {'$max': '$content'},
                    'sum_content': {'$sum': '$content'},
                    'avg_content': {'$avg': '$content'}
                }
            }
        ]
        
        stats = list(collection.aggregate(pipeline))[0]
        
        # 메타데이터 문서 업데이트
        metadata_collection.update_one(
            {'collection_name': collection_name},
            {'$set': {
                'min_type': stats['min_type'],
                'max_type': stats['max_type'],
                'sum_type': stats['sum_type'],
                'avg_type': stats['avg_type'],
                'min_content': stats['min_content'],
                'max_content': stats['max_content'],
                'sum_content': stats['sum_content'],
                'avg_content': stats['avg_content']
            }}
        )

if __name__ == '__main__':
    update_metadata()

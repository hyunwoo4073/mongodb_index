# import csv
# import random
# import time
# from datetime import datetime

# # 데이터 생성 및 CSV 파일 저장 함수 정의
# def generate_data(filename, num_rows):
#     with open(filename, 'w', newline='') as csvfile:
#         fieldnames = ['content', 'type', 'timestamp']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
#         writer.writeheader()
#         for _ in range(num_rows):
#             content = random.randint(0, 2**32-1)  # 4바이트 정수
#             type_ = random.randint(0, 2**32-1)  # 4바이트 정수
#             timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 현재 시간 기반의 타임스탬프
            
#             writer.writerow({'content': content, 'type': type_, 'timestamp': timestamp})
#             time.sleep(0.001)  # 타임스탬프가 다르도록 간격을 줌

# # 함수 사용 예
# # 파일 이름과 생성할 행의 수를 지정
# generate_data('generated_data.csv', 1000000)



# 멀티 프로세싱 이용한 데이터 생성
# import csv
# import random
# from datetime import datetime, timedelta
# from concurrent.futures import ProcessPoolExecutor

# # 데이터 생성 함수 (각 데이터 행에 대해 고유한 타임스탬프 생성)
# def generate_data_row(index):
#     content = random.randint(0, 2**32-1)  # 4바이트 정수
#     type_ = random.randint(0, 2**32-1)  # 4바이트 정수
#     # 고유한 타임스탬프 생성: 기준 시간으로부터 index 초 더함
#     timestamp = (datetime.now() - timedelta(seconds=index)).strftime("%Y-%m-%d %H:%M:%S")
#     return {'content': content, 'type': type_, 'timestamp': timestamp}

# # 멀티프로세싱을 사용하여 데이터 생성 및 CSV 파일 저장
# def generate_and_save_data(filename, num_rows, num_processes=4):
#     with ProcessPoolExecutor(max_workers=num_processes) as executor:
#         # 데이터 생성: 인덱스를 매개변수로 전달하여 고유한 타임스탬프 생성
#         rows = list(executor.map(generate_data_row, range(num_rows)))
        
#     # 생성된 데이터를 CSV 파일에 저장
#     with open(filename, 'w', newline='') as csvfile:
#         fieldnames = ['content', 'type', 'timestamp']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
#         writer.writeheader()
#         for row in rows:
#             writer.writerow(row)

# # 예제 사용
# generate_and_save_data('generated_data_5000000.csv', 5000000, num_processes=10)



# 배치크기 만큼 만들어서 쓰는 코드
# import csv
# import random
# from datetime import datetime, timedelta
# from concurrent.futures import ProcessPoolExecutor, as_completed
# import os

# def generate_and_write_batch(start_index, batch_size, filename):
#     # 각 배치 데이터 생성 및 파일 쓰기
#     with open(filename, 'a', newline='') as csvfile:
#         fieldnames = ['content', 'type', 'timestamp']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         for index in range(start_index, start_index + batch_size):
#             content = random.randint(0, 2**32-1)
#             type_ = random.randint(0, 2**32-1)
#             timestamp = (datetime.now() - timedelta(seconds=index)).strftime("%Y-%m-%d %H:%M:%S")
#             writer.writerow({'content': content, 'type': type_, 'timestamp': timestamp})

# def generate_and_save_data_in_batches(filename, num_rows, save_path, batch_size=10000, num_processes=4):
#     filepath = os.path.join(save_path, filename)
#     num_batches = (num_rows + batch_size - 1) // batch_size
#     with open(filename, 'w', newline='') as csvfile:
#         fieldnames = ['content', 'type', 'timestamp']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         writer.writeheader()
    
#     with ProcessPoolExecutor(max_workers=num_processes) as executor:
#         futures = {executor.submit(generate_and_write_batch, i * batch_size, batch_size, filename): i for i in range(num_batches)}
#         for future in as_completed(futures):
#             batch_index = futures[future]
#             print(f"Batch {batch_index + 1}/{num_batches} completed. Progress: {((batch_index + 1)/num_batches)*100:.2f}%")

# generate_and_save_data_in_batches('generated_data_100000000.csv', 100000000, '/home/dblab/data/mongodb', batch_size=20000, num_processes=4)



import csv
import random
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

def generate_and_write_batch(start_index, batch_size, filepath):
    # 각 배치 데이터 생성 및 파일 쓰기
    with open(filepath, 'a', newline='') as csvfile:
        # fieldnames = ['content', 'type', 'timestamp']
        fieldnames = ['timestamp']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        for index in range(start_index, start_index + batch_size):
            # content = random.randint(0, 2**32-1)
            # type_ = random.randint(0, 2**32-1)
            # timestamp = (datetime.now() - timedelta(seconds=index)).strftime("%Y-%m-%d %H:%M:%S")
            timestamp = int((datetime.now() - timedelta(seconds=index)).timestamp())
            # writer.writerow({'content': content, 'type': type_, 'timestamp': timestamp})
            writer.writerow({'timestamp': timestamp})
def generate_and_save_data_in_batches(filename, num_rows, save_path, batch_size=10000, num_processes=4):
    filepath = os.path.join(save_path, filename)  # 파일 경로 조합
    num_batches = (num_rows + batch_size - 1) // batch_size
    
    # 파일 헤더 작성
    with open(filepath, 'w', newline='') as csvfile:
        # fieldnames = ['content', 'type', 'timestamp']
        fieldnames = ['timestamp']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = {executor.submit(generate_and_write_batch, i * batch_size, batch_size, filepath): i for i in range(num_batches)}
        for future in as_completed(futures):
            batch_index = futures[future]
            print(f"Batch {batch_index + 1}/{num_batches} completed. Progress: {((batch_index + 1)/num_batches)*100:.2f}%")

# 예시 실행, 절대 경로를 지정해야 합니다.
generate_and_save_data_in_batches('generated_data_2000000000_type.csv', 2000000000, '/home/dblab/data/mongodb', batch_size=10000, num_processes=4)
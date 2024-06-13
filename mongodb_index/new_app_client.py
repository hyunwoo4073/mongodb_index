import socket
import json
from datetime import datetime, timedelta
import random

def send_data(host, port, data_batch):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        for data in data_batch:
            # JSON 객체를 문자열로 변환 후 줄바꿈 문자 추가
            message = json.dumps(data).encode('utf-8') + b'\n'
            s.sendall(message)
        # 마지막에 서버로부터 응답 메시지 수신 및 출력
            response = s.recv(1024)
            print('Received:', repr(response))

def main(host, port, n, batch_size, start_timestamp):
    start_time = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
    batch = []
    for i in range(n):
        timestamp = start_time + timedelta(seconds=i)
        data = {
            'content': random.randint(0, 2**32 - 1),
            'type': random.randint(0, 2**32 - 1),
            'timestamp': timestamp.isoformat()
        }
        batch.append(data)
        if len(batch) == batch_size:
            send_data(host, port, batch)
            batch = []
    if batch:
        send_data(host, port, batch)

if __name__ == '__main__':
    host = '192.168.0.31'
    port = 65432
    n =   10000# 전송할 데이터의 총 개수
    batch_size = 1  # 배치 크기
    start_timestamp = '2033-11-12 02:40:14'  # 시작 타임스탬프
    main(host, port, n, batch_size, start_timestamp)


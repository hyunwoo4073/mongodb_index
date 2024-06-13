import socket
import json
from datetime import datetime, timedelta
import random

def send_data(host, port, data):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        # JSON 형태로 데이터 전송
        s.sendall(json.dumps(data).encode('utf-8'))
        # 서버로부터의 응답 수신
        response = s.recv(1024)
        print('Received:', repr(response))

if __name__ == '__main__':
    host = 'localhost'  # 서버의 호스트 주소
    port = 65432  # 서버의 포트 번호

    # n = 1000000000  # 전송할 데이터 개수
    n = 
    for i in range(n):
        data = {
            'content': random.randint(0, 2**32 - 1),
            'type': random.randint(0, 2**32 - 1),
            'timestamp': (datetime.now() + timedelta(seconds=i)).isoformat()
        }
        send_data(host, port, data)

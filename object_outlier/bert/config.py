from sqlalchemy import create_engine
import os

# 실행 정보
host = '0.0.0.0'
port = 5050

# 데이터베이스 정보
db_info = {
    'database' : 'postgresql',
    'host': '192.168.2.122',
    'port': '5432',
    'db': 'pretooldb',
    'user': 'pretoolusr',
    'password': '1234'
}

db_url = '{}://{}:{}@{}:{}/{}'.format(db_info['database'], db_info['user'], db_info['password'], db_info['host'], db_info['port'], db_info['db'])

engine = create_engine(db_url)

# 모델 경로
bert_model = os.path.join('/home', 'datanuri', 'bert', 'service', 'pth', 'bert.pth')
train_data_path = os.path.join('/bert_model', 'data')
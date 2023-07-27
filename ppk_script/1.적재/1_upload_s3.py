import pandas as pd
import numpy as np
import os
import boto3
import sys
import re
from logging.config import dictConfig
import logging
import datetime
import shutil

def log(msg):
    logging.info(msg)

filePath, fileName = os.path.split(__file__)
logFolder = os.path.join(filePath , 'logs')
os.makedirs(logFolder, exist_ok = True)
os.makedirs(os.path.join(filePath, 'Processed_data'), exist_ok = True)
logfilepath = os.path.join(logFolder, fileName.split('.')[0] + '_' +re.sub('-', '', datetime.datetime.now().strftime('%Y%m%d')) + '.log')

dictConfig({
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)s --- %(message)s',
        }
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': logfilepath,
            'formatter': 'default',
        },
    },
    'root': {
        'level': 'INFO',
        'handlers': ['file']
    }
})

file_name = 'PPK-TC_ELCTY_ATMBL_EHGR_DR_OP_RECD-20230101-20230331-N-P.csv'


def upload_s3(s3, bucket_name, file_name):
    # 파일 이름 가공
    file_name2 = re.sub('-P.','-M.', file_name)
    # if len(file_name.split('-')[-1]) <= 6:
    #     file_name2 = 'PPK-TC_{}-{}-{}-N-M.csv'.format(file_name.split('_N')[0][4:].upper(), re.sub('-', '', re.split('[_|.]', file_name.split('_Y_')[1])[0]), re.sub('-', '', re.split('[_|.]', file_name.split('_Y_')[1])[1]))
    # else:
    #     file_name2 = 'PPK-TC_{}-{}-{}-N-M_{}.csv'.format(file_name.split('_N')[0][4:].upper(), re.sub('-', '', re.split('[_|.]', file_name.split('_Y_')[1])[0]), re.sub('-', '', re.split('[_|.]', file_name.split('_Y_')[1])[1]), re.split('[_|.]', file_name)[-2])
        
    log('#### Upload to S3 File \"{}\" as \"{}\"'.format(file_name, file_name2))

    # file_name3 = re.split('_', file_name2, maxsplit=1)[1]

    # 불러올 파일 경로
    file_path = '/nas/PPK_masking/' + file_name
    file_path2 = '/nas/PPK_s3/' + file_name2

    # 파일 업로드
    s3.put_object(Bucket = bucket_name, Key = file_name2)

    s3.upload_file(file_path, bucket_name, file_name2)
    
    # s3 폴더 이동(백업)
    shutil.move(file_path, file_path2)

# key 세팅
service_name = 's3'
endpoint_url = 'https://kr.object.ncloudstorage.com'
region_name = 'Asia Pacific'
access_key = 'C38F7E3A65624D369C1D'
secret_key = '2FA18747C2D56E98963AB4D5D6B0D9A035518D98'

# s3 클라이언트 생성
s3 = boto3.client(service_name,
                    endpoint_url = endpoint_url,
                    aws_access_key_id = access_key,
                    aws_secret_access_key = secret_key)

# 업로드 버킷 위치
bucket_name = 'ppk'

if len(sys.argv) == 1:
    file_name_list = os.listdir('/nas/PPK_masking/')
    
    for file_name in file_name_list:
        upload_s3(s3, bucket_name, file_name)
elif len(sys.argv) > 1:
    # 인자값
    file_name = sys.argv[1]
    # file_name = 'PPK_ev_run_recd_Y_2022-01-01_2022-01-31_19.csv'
    # file_name = 'PPK_ev_run_recd_Y_2022-01-01_2022-01-31.csv'

    upload_s3(s3, bucket_name, file_name)
else:
    log('Error')

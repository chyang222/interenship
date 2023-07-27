import os
import pandas as pd
import re
from logging.config import dictConfig
import logging
import datetime
import traceback
import numpy as np

filePath, fileName = '/home/datanuri/3_ppk_2023/2_데이터_가공', 'ych_proc_date_dele.py'
logFolder = os.path.join(filePath , 'logs')
os.makedirs(logFolder, exist_ok = True)
logfilepath = os.path.join(logFolder, fileName.split('.')[0] + '_' +re.sub('-', '', datetime.datetime.now().strftime('%Y%m%d')) + '.log')

process_fileName = "PPK-TC_OPN_CHRSTN_RDNG_AND_TAXI_HIST_DATA-20230401-20230630-N-P.csv"
date_fileName = "PPK-TC_OPN_CHRSTN_RDNG_AND_TAXI_HIST_DATA2023-04-01,2023-06-30-error_date.csv"


dictConfig({
    'version': 1,
    'formatters': {
        'default': {
            'format' : '[%(asctime)s] %(levelname)7s --- %(lineno)6d : %(message)s',
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



def main():
    # try:
    logging.info("START")

    date_df = pd.read_csv(os.path.join('/nas/PPK_date_error', date_fileName))
    indices_to_delete = date_df["index"].tolist()
    
    chunks = pd.read_csv(os.path.join('/nas/PPK_process', process_fileName), encoding = 'utf-8-sig', chunksize = 1000000) # 청크사이즈 100만으로 조정 후 읽기

    for divided_file, sample_df in enumerate(chunks):
        processing_df = sample_df[~sample_df.index.isin(indices_to_delete)]
        processing_df = processing_df.reset_index(drop=True)

        logging.info(f'date 정제전데이터수|{len(sample_df)} | date 정제후데이터수 | {len(processing_df)}')

    
        # 삭제 후 다시 저장 
        if divided_file == 0:
            processing_df.to_csv(os.path.join('/nas/PPK_process', "del_date_" + process_fileName), encoding = 'utf-8-sig', index = False) # 첫번째 청크일때는 csv 생성
        else:
            processing_df.to_csv(os.path.join('/nas/PPK_process', "del_date_" + process_fileName), encoding = 'utf-8-sig', index = False, header = False, mode = 'a') # 이후 뒤에 더하는 방식

    
    logging.info(f"######################################## {process_fileName} Complete")


if __name__=="__main__":
    main()

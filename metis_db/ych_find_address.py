#!/usr/bin/python
# -*- coding : utf-8 -*-
'''
 @author : ych
'''
''' install '''

''' import '''
import pandas as pd
import numpy as np
import logging
import datetime
from logging.config import dictConfig
import re
import os
import warnings
import traceback
import csv

warnings.filterwarnings('ignore')
csv.field_size_limit(100000000)

filePath, fileName = '/home/datanuri/yjjo/python_scripts', 'ych_find_address.py'
logFolder = os.path.join(filePath , 'logs')
os.makedirs(logFolder, exist_ok = True)
logfilepath = os.path.join(logFolder, fileName.split('.')[0] + '_' +re.sub('-', '', datetime.datetime.now().strftime('%Y%m%d')) + '.log')


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

try:
    os.makedirs('/home/datanuri/yjjo/python_scripts/Report')
except:
    pass


#주소 체크 기준
check_add = ['강원특별자치도', '경기도', '경상남도', '경상북도', '광주광역시', '대구광역시', '대전광역시', '부산광역시', '서울특별시', '세종특별자치시', '울산광역시', '인천광역시', '전라남도', '전라북도', '제주특별자치도', '충청남도', '충정북도']

        
def main():
    try:
        logging.info("START")

        #파일 폴더를 불러와서 .csv가 끝인 확장자만 폴더에 저장
        data_path = '/data2/metis_dstrb/'
        filelist =  os.listdir(data_path)
        file_list_csv = [file for file in filelist if file.endswith('.csv')]
        address_report_format = pd.DataFrame(columns=('No','uuid','컬럼명','주소 여부'))
        index=0
        
        for idx, path in enumerate(file_list_csv):
            print(idx)
            logging.info(f"File Count : {idx+1}/{len(file_list_csv)+1}")
            logging.info('#### Read Path \"{}\"'.format(path))
            try:
                df = pd.read_csv(data_path + path, encoding = 'utf-8-sig', nrows = 100, engine='python') 
            except:
                try:
                    df = pd.read_csv(data_path + path, encoding = 'cp949', nrows = 100, engine='python') 
                except:
                    address_report_format.loc[index] = [idx+1, path, traceback.format_exc(), '-']
                    index += 1
            
            non_comp = df.select_dtypes(exclude='object')
            for cl in non_comp.columns:
                report_df_ex = [idx+1, path, cl, 'X']
                address_report_format.loc[index] = report_df_ex
                index+=1
            
            compare = df.select_dtypes(include='object')     
            
            for col in compare.columns:
                report_df_in = [idx+1, path, col]
                add_series = compare[col].apply(lambda x: check_address(x))
                report_df_in.append('O' if any(add_series == 'O') else 'X')
                address_report_format.loc[index] = report_df_in
                index+=1
            
        address_report_format.to_csv("/home/datanuri/yjjo/python_scripts/Report/데이터누리_주소판별여부.csv", encoding = 'utf-8-sig')

    except RuntimeWarning as w:
      logging.warning(w)
    except:
        logging.error('############ Read Path \"{}\" Error'.format(path))
        logging.error(traceback.format_exc())
        
        
def check_address(x:str):
    if pd.isnull(x):
        return 'X'
    else:
        x = x.strip().split()
        for i in x:
            if any(i in addr for addr in check_add):
                return 'O'
            else:
                return 'X'
        
        
if __name__=="__main__":
    # Calculate Run Time
    start_time = datetime.datetime.now()
    
    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    main()
    
    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    
    logging.info('==========================================================================================')
    logging.info('#### Run Time {}'.format(str(datetime.datetime.now() - start_time)))
    logging.info('==========================================================================================')
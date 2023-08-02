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
import psycopg2
from sqlalchemy import create_engine
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
        
def find_add():
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
                df = pd.read_csv(data_path + path, encoding = 'utf-8-sig', nrows = 10, engine='python') 
            except:
                try:
                    df = pd.read_csv(data_path + path, encoding = 'cp949', nrows = 10, ineterminator='\n', engine='python') 
                except:
                    try:
                        df = pd.read_csv(data_path + path, encoding = 'euc_kr', nrows = 10, engine='python') 
                    except:
                        address_report_format.loc[index] = [idx+1, path,'-', 'X']
                        index += 1    
                
            compare = df.select_dtypes(include='object')     
            
            check_add_count = 0
            
            for col in compare.columns:
                add_series = compare[col].apply(lambda x: check_address(x))
                if any(add_series == 'O'):
                    address_report_format.loc[index] = [idx+1, path, col, 'O']
                    check_add_count += 1
                    index+=1
            
            if check_add_count == 0:
                address_report_format.loc[index] = [idx+1, path, '-', 'X']
                index+=1
        
        address_report_format.to_csv("/home/datanuri/yjjo/python_scripts/Report/데이터누리_주소판별여부.csv", encoding = 'utf-8-sig')

        return address_report_format
    
    except RuntimeWarning as w:
      logging.warning(w)
    except:
        logging.error('############ Read Path \"{}\" Error'.format(path))
        logging.error(traceback.format_exc())
           
def check_address(x:str):
    try:
        if pd.isnull(x):
            return 'X'
        else:
            x = x.strip().split()
            for i in x:
                if any(i in addr for addr in check_add):
                    return 'O'
                else:
                    return 'X'
    except:
        pass

def connet_db(df):        
    host = "192.168.2.102"
    port = "5432"
    user = "metis"
    password = "password"
    database = "metisdb" 

    try:
        # PostgreSQL에 접속
        connection = psycopg2.connect(
            host=host, port=port, user=user, password=password, database=database
        )
        cursor = connection.cursor()

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        
        logging.info('PostgreSQL 접속.')

        #------------------------------------------------------------------------------------------------------------
        
        query_distribution = """
        SELECT id, resource_id, title, byte_size, file_name
        FROM distribution;
        """
        df_distribution = pd.read_sql_query(query_distribution, engine)
        merged_df = df.merge(df_distribution, left_on='uuid', right_on='id', how='left')
        merged_df.drop('id', axis=1, inplace=True)
        
        #------------------------------------------------------------------------------------------------------------
            
        query_resuorce = """
        SELECT id, title AS resource_title
        FROM resource
        """
        
        df_resource = pd.read_sql_query(query_resuorce, engine)
        merged_df_2 = merged_df.merge(df_resource, left_on='resource_id', right_on='id', how='left')
        merged_df_2.drop('id', axis=1, inplace=True)
        
        #------------------------------------------------------------------------------------------------------------
        
        query_resuorce_categoty = """
        SELECT resource_id, catalog_id, node_id
        FROM resource_category_map
        """
        
        df_resuorce_categoty = pd.read_sql_query(query_resuorce_categoty, engine)
        merged_df_3 = merged_df_2.merge(df_resuorce_categoty, left_on='resource_id', right_on='resource_id', how='left')
        
        #------------------------------------------------------------------------------------------------------------
        
        query_catalog = """
        SELECT catalog_id, catalog_name
        FROM catalog
        """
        
        df_catalog = pd.read_sql_query(query_catalog, engine)
        merged_df_4 = merged_df_3.merge(df_catalog, left_on='catalog_id', right_on='catalog_id', how='left')
        #------------------------------------------------------------------------------------------------------------

        query_taxonomy = """
        SELECT id, title AS taxonomy_title
        FROM taxonomy
        """
        df_taxonomy = pd.read_sql_query(query_taxonomy, engine)
        df_final = merged_df_4.merge(df_taxonomy, left_on='node_id', right_on='id', how='left')
        df_final.drop('id', axis=1, inplace=True)
        
        #------------------------------------------------------------------------------------------------------------
        df_final = df_final.dropna(subset=['taxonomy_title'])        
        #df_final.to_csv("/home/datanuri/yjjo/python_scripts/Report/mapping/데이터누리_address_mapping.csv", encoding = 'utf-8-sig', index=False)    
        logging.info('#### DB와 데이터프레임 merge 완료 ####')
        return df_final

    except (Exception, psycopg2.Error) as error:
        logging.info('#### PostgreSQL에 접속 중 오류가 발생했습니다: {}'.format(error))

def summart_report(df):
    logging.info('#### 레포트 요약 추출 준비 ####')
    summ_fomat = pd.DataFrame(columns=('카탈로그','분류 체계','전체 건', '주소 포함 건수', '주소 포함 비율'))

    node = df.groupby(['node_id','catalog_id'])

    
    node_idx = node['주소 여부'].count().sort_values().index
    node_count = node['주소 여부'].count().sort_values().to_list()
    include_df = node.agg({'주소 여부':get_add_count})['주소 여부'].droplevel(axis=0,level=1)


    idx = 0
    for id in node_idx:
        title = df[df.node_id == id[0]].taxonomy_title.iloc[0]
        addr = include_df[include_df.index == id[0]].iloc[0]
        catalog = df[df.node_id == id[0]].catalog_name.iloc[0]
        in_df = [catalog, title, node_count[idx], addr, round((addr/node_count[idx]) * 100, 2)] 
        summ_fomat.loc[idx] = in_df
        idx += 1
        

    writer = pd.ExcelWriter('/home/datanuri/yjjo/python_scripts/Report/mapping/데이터누리_address_final.xlsx', engine = 'xlsxwriter')
    summ_fomat.to_excel(writer, index = False, sheet_name = '요약')
    df.to_excel(writer , index = False, sheet_name = '세부내역')
    writer.save()
    logging.info('#### 레포트 요약 추출 완료 ####')
    

def get_add_count(series):
    res = len([ x for x in series if x[0]=='O'])
    return res

if __name__=="__main__":
    # Calculate Run Time
    start_time = datetime.datetime.now()
    
    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''    
    #주소 체크 기준
    check_add = ['강원특별자치도', '경기도', '경상남도', '경상북도', '광주광역시', '대구광역시', '대전광역시', '부산광역시', '서울특별시', '세종특별자치시', '울산광역시', '인천광역시', '전라남도', '전라북도', '제주특별자치도', '충청남도', '충정북도']
    df = find_add()
    
    df.drop('Unnamed: 0', axis=1, inplace=True)
    df['uuid'] = df['uuid'].str.replace('.csv', '')
    df = connet_db(df)
  
    summart_report(df)
    
    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    
    logging.info('==========================================================================================')
    logging.info('#### Run Time {}'.format(str(datetime.datetime.now() - start_time)))
    logging.info('==========================================================================================')
#!/usr/bin/python
# -*- coding : utf-8 -*-
'''
 @author : ych
'''
''' import '''
import logging
from logging.config import dictConfig
import datetime
import traceback
import os
import pandas as pd
from sqlalchemy import create_engine

''' log '''
script_abs_path, script_name = os.path.split(__file__)
script_abs_path, script_name = os.path.split('/home/datanuri/yjjo/python_scripts/check_status.py')

dictConfig({
    'version' : 1,
    'formatters' : {
        'default' : {
            'format' : '[%(asctime)s] %(levelname)7s --- %(lineno)6d : %(message)s',
        },
    },
    'handlers' : {
        'file' : {
            'class' : 'logging.FileHandler',
            'level' : 'DEBUG',
            'formatter' : 'default',
            'filename' : os.path.join(script_abs_path, 'logs', '{}_{}.log'.format(script_name.rsplit('.', maxsplit = 1)[0], datetime.datetime.now().strftime('%Y%m%d'))),
        },
    },
    'root' : {
        'level' : 'DEBUG',
        'handlers' : ['file']
    }
})

def log(msg):
    logging.info(msg)

def log_err(msg):
    logging.error(msg)

def log_warn(msg):
    logging.warning(msg)

''' main function'''
def main():
    try:
        database = 'postgresql'
        db_user = 'metis'
        db_password = 'password'
        host = 'localhost'
        port = '5432'
        db_name = 'metisdb'
        
        db_info = '{}://{}:{}@{}:{}/{}'.format(database, db_user, db_password, host, port, db_name)
        
        engine = create_db_engine(db_info)
        
        resource_df = select_table(engine, 'resource')
        distribution_df = select_table(engine, 'distribution')
        resource_category_map_df = select_table(engine, 'resource_category_map')
        taxonomy_df = select_table(engine, 'taxonomy')
        catalog_df = select_table(engine, 'catalog')
        
        resource_df.columns
        distribution_df.columns
        
        rsrc_dstrb_df = resource_df[['id', 'title']].merge(distribution_df[['resource_id', 'id', 'format', 'byte_size']], 'left', left_on = 'id', right_on = 'resource_id').drop(['id_x'], axis = 1)
        
        rsrc_dstrb_txnmy_df = rsrc_dstrb_df.merge(resource_category_map_df[['resource_id', 'catalog_id']], 'left', 'resource_id')
        
        rsrc_dstrb_txnmy_ctlog_df = rsrc_dstrb_txnmy_df.merge(catalog_df[['catalog_id', 'catalog_name']], 'left', 'catalog_id').drop(['catalog_id'], axis = 1)
        
        rsrc_dstrb_txnmy_ctlog_df_sum = rsrc_dstrb_txnmy_ctlog_df.groupby(['catalog_name', 'title', 'format']).sum().reset_index()[['catalog_name', 'title', 'format', 'byte_size']]
        rsrc_dstrb_txnmy_ctlog_df_count = rsrc_dstrb_txnmy_ctlog_df.groupby(['catalog_name', 'title', 'format']).count().reset_index()[['catalog_name', 'title', 'format', 'id_y']]
        
        final_df = rsrc_dstrb_txnmy_ctlog_df_count.merge(rsrc_dstrb_txnmy_ctlog_df_sum, 'left', ['catalog_name', 'title', 'format'])
        
        final_df['data_type'] = final_df.apply(lambda x : '정형' if x['format'] == 'csv' else '비정형', axis = 1)
        
        final_df.loc[final_df['data_type'] != '정형', 'data_type'] = final_df.apply(lambda x : '음성' if '음성' in x['title'] else('텍스트' if '텍스트' in x['title'] else '학습용데이터'), axis = 1)
        
        final_df[final_df['data_type'] != '정형']
        final_df = final_df[final_df['byte_size'] != 0].reset_index(drop = True)
        
        final_df_rename = final_df.rename({
            'catalog_name' : '데이터 분류',
            'title' : '데이터명',
            'format' : '데이터 포맷',
            'id_y' : '데이터셋 건수',
            'byte_size' : '데이터셋 크기',
            'data_type' : '데이터 타입',
        }, axis = 1)
        
        summary_df = final_df_rename.groupby(['데이터 분류', '데이터 타입']).sum().reset_index()
        
        summary_df['수집 방법'] = summary_df.apply(lambda x : '크롤링' if x['데이터 분류'] in ['공공데이터', '지식재산권', '영화', '한국전력공사'] else ('사업' if x['데이터 분류'] in ['데이터바우처', '플랫폼데이터', '학습용데이터'] else 'Open API'), axis = 1)
        
        writer = pd.ExcelWriter('{}/데이터누리 수집 데이터 현황 정리_{}.xlsx'.format(script_abs_path, datetime.datetime.now().strftime('%Y%m%d')), engine = 'xlsxwriter')
        summary_df.to_excel(writer, '수집 현황 요약', encoding = 'UTF-8-SIG')
        final_df_rename.to_excel(writer, sheet_name = '상세', encoding = 'UTF-8-SIG')
        writer.close()
    except:
        log_err('############ Main Funtion Error')
        log_err(traceback.format_exc())
    
''' functions '''
# file list reader input path return list
def read_file_list(path):
    try:
        log('#### Read Path \"{}\"'.format(path))
        file_list = list([])
        
        for dir_path, _, file_name in os.walk(path):
            for f in file_name:
                try:
                    file_list.append(os.path.abspath(os.path.join(dir_path, f)))
                except:
                    log('######## Read File \"{}\" Error'.format(file_name))
                    log(traceback.format_exc())

        log('############ Path \"{}\" File Count : {}'.format(path, len(file_list)))
        return file_list
    except RuntimeWarning as w:
        log_warn(w)
    except:
        log_err('############ Read Path \"{}\" Error'.format(path))
        log_err(traceback.format_exc())

# database engine creater
def create_db_engine(db_info):
    try:
        log('#### Create Database Engine \"{}\"'.format(db_info))
        return create_engine(db_info)
    except RuntimeWarning as w:
        log_warn(w)
    except:
        log_err('############ Create Database Engine Error')
        log_err(traceback.format_exc())

# table selector from database
def select_table(engine, table_name):
    try:
        log('#### Select Table : \"{}\"'.format(table_name))
        df = pd.read_sql(
            sql = 'select * from {}'.format(table_name),
            con = engine,
        )

        return df
    except RuntimeWarning as w:
        log_warn(w)
    except:
        log_err('############ Read Table \"{}\" Error'.format(table_name))
        log_err(traceback.format_exc())

''' main '''
if __name__ == '__main__':
    # Calculate Run Time
    start_time = datetime.datetime.now()

    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

    main()

    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    
    log('==========================================================================================')
    log('#### Run Time {}'.format(str(datetime.datetime.now() - start_time)))
    log('==========================================================================================')
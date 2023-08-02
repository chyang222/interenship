#!/usr/bin/python
# -*- coding : utf-8 -*-

''' import '''
import logging
from logging.config import dictConfig
import datetime
import traceback
import os
import pandas as pd
import zipfile
import re

''' log '''
script_abs_path, script_name = os.path.split(__file__)
script_abs_path, script_name = os.path.split('/home/datanuri/yjjo/python_scripts/zip_dstrb_file.py')

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
        dstrb_file_list = '/home/datanuri/metis_dstrb_file_list.txt'
        
        df = pd.read_csv(dstrb_file_list)
        
        dstrb_file_list2 = '/home/datanuri/metis_dstrb_file_list2.txt'
        
        df2 = pd.read_csv(dstrb_file_list2)
        
        df2.head(10)
        
        df = df.rename({'total 921289100' : 'full'}, axis = 1)
        
        df['size'] = df['full'].apply(lambda x : re.split(' +', x)[4]).astype(int)
        df['name'] = df['full'].apply(lambda x : re.split(' +', x)[-1])
        
        file_list2 = list(df.loc[df['size'] < 5000000, 'name'])

        path = '/data2/metis_dstrb'
        
        zip_file = zipfile.ZipFile('/data2/gov_dstrb2.zip', 'w')
        
        for idx, file_name in enumerate(file_list2):
            try:
                zip_file.write(os.path.join(path, file_name), compress_type = zipfile.ZIP_DEFLATED)
            except:
                print('{} : {}'.format(idx, file_name))
        
        zip_file.close()
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
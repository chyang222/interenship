
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
# import modin.pandas as pd
from scipy.spatial.distance import cdist
from sklearn.cluster import DBSCAN
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import Lasso, LinearRegression, LogisticRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from .util.custom_exception import CustomUserError

import uuid

import re

class HandlingDataset:

    def __init__(self, app, dsDAO, jhDAO, fileDAO, ahDAO, dataset):
        self.app = app
        self.dsDAO = dsDAO
        self.jhDAO = jhDAO
        self.fileDAO = fileDAO
        self.ahDAO = ahDAO
        self.dataset = dataset

    #########################################################################
    #########################################################################
    # 0. 공통 소스
    #########################################################################
    
    def insert_api_history(self, api_history):
        self.ahDAO.insert_api_history(api_history = api_history)
    
    def update_api_history(self, api_history):
        self.ahDAO.update_api_history(api_history = api_history)
    
    def insert_file_info_into_database(self, file):
        self.fileDAO.insert_file_info(file = file)
        
    # 0-1. Job_History 저장
    def insert_job_history_into_database(self, ds):
        target_id = 'user' + str(random.randint(100, 500))

        job_history = {
            'id': uuid.uuid1(),
            'group_id': ds.group_id,
            'department_id': ds.department_id,
            'user_id': target_id,
            'project_id': ds.project_id,
            'file_id': ds.file_id,
            'version': ds.version,
            'job_id': ds.job_id,
            'content': ds.job_content_to_json()
        }

        self.jhDAO.insert_job_history(job_history = job_history)
    
    def undo_redo(self, ds):
        return self.jhDAO.update_job_history_active(ds.project_id, ds.file_id, ds.version, ds.min_max, ds.job_active)
    
    def init_job_history(self, ds):
        return self.jhDAO.init_job_history(ds.project_id, ds.file_id, ds.version)
        # return self.jhDAO.delete_job_history(ds.id)
    
    def delete_inactive_job_history(self, ds):
        return self.jhDAO.delete_inactive_job_history(ds.project_id, ds.file_id, ds.version)
        # return self.jhDAO.delete_job_history(ds.id)

    # file list 불러오기
    def select_file_list(self, project_id, params):
        return self.fileDAO.select_file_list(project_id, params)
        # return self.fileDAO.get_file_info(ds.id)
        
    #########################################################################

    # 0-2. file 정보 불러오기
    def get_file_info_from_db(self, ds):
        return self.fileDAO.get_file_info(ds.project_id, ds.file_id, ds.version)
        # return self.fileDAO.get_file_info(ds.id)

    # 0-3. data option update
    def set_data_options(self, payload):
        try:
            # print(payload)
            file = {
                'id': uuid.uuid1(),
                'file_id': payload['file_id'],
                'project_id': payload['project_id'],
                'format': payload['format'],
                'encoding_info': payload['encoding_info'],
                'extra_options': json.dumps(payload['extra_options'])
            }
            self.fileDAO.update_file_info(file = file)
            return {'status' : 'success'}
        except:
            return {'status' : 'Fail'}

    # 0-3. redirect_preprocess
    def redirect_preprocess(self, ds):
        job_id = ds.job_id
        self.app.logger.info('%s' % ds.job_id)
        if job_id == 'drop_column':
            ds = self.drop_column(ds)
        elif job_id == 'missing_value':
            ds = self.missing_value(ds)
        elif job_id == 'set_col_prop':
            ds = self.set_col_prop(ds)
        elif job_id == 'set_col_prop_to_datetime':
            ds = self.set_col_prop_to_datetime(ds)
        elif job_id == 'split_datetime':
            ds = self.split_datetime(ds)
        elif job_id == 'dt_to_str_format':
            ds = self.dt_to_str_format(ds)
        elif job_id == 'diff_datetime':
            ds = self.diff_datetime(ds)
        elif job_id == 'change_column_order':
            ds = self.change_column_order(ds)
        elif job_id == 'case_sensitive':
            ds = self.case_sensitive(ds)
        elif job_id == 'replace_by_input_value':
            ds = self.replace_by_input_value(ds)
        elif job_id == 'remove_space_front_and_rear':
            ds = self.remove_space_front_and_rear(ds)
        elif job_id == 'drop_duplicate_row':
            ds = self.drop_duplicate_row(ds)
        elif job_id == 'calculating_column':
            ds = self.calculating_column(ds)
        elif job_id == 'drop_row':
            ds = self.drop_row(ds)
        elif job_id == 'rename_col':
            ds = self.rename_col(ds)
        elif job_id == 'split_col':
            ds = self.split_col(ds)
        elif job_id == 'missing_value_model':
            ds = self.missing_value_model(ds)
        elif job_id == 'unit_conversion':
            ds = self.unit_conversion(ds)
        elif job_id == 'concat':
            ds = self.concat(ds)
        elif job_id == 'merge':
            ds = self.merge(ds)
        elif job_id == 'conditioned_row':
            ds = self.conditioned_row(ds)
        elif job_id == 'missing_value_calc':
            ds = self.missing_value_calc(ds)
        elif job_id == 'reverse_boolean':
            ds = self.reverse_boolean(ds)
        elif job_id == 'set_col_to_index':
            ds = self.set_col_to_index(ds)
        elif job_id == 'cleansing_negative_value':
            ds = self.cleansing_negative_value(ds)
        elif job_id == 'round_value':
            ds = self.round_value(ds)
        elif job_id == 'apply_work_step':
            ds = self.apply_work_step(ds)
        elif job_id == 'sort_col':
            ds = self.sort_col(ds)
        elif job_id == 'group_by':
            ds = self.group_by(ds)
        elif job_id == 'transpose_df':
            ds = self.transpose_df(ds)
        elif job_id == 'show_filter':
            ds = self.show_filter(ds)
        elif job_id == 'remove_by_input_value':
            ds = self.remove_by_input_value(ds)
        elif job_id == 'concat_col':
            ds = self.concat_col(ds)
        elif job_id == 'outlier_iqr':
            ds = self.outlier_iqr(ds)
        elif job_id == 'outlier_inpt':
            ds = self.outlier_inpt(ds)
        elif job_id == 'outlier_value_counts':
            ds = self.outlier_value_counts(ds)
        elif job_id == 'outlier_clustering':
            ds = self.outlier_clustering(ds)
        elif job_id == 'missing_value_df':
            ds = self.missing_value_df(ds)
        elif job_id == 'duplicated_df':
            ds = self.duplicated_df(ds)
        elif job_id == 'create_new_column':
            ds = self.create_new_column(ds)
        elif job_id == 'copy_column':
            ds = self.copy_column(ds)
        else:
            print('#### Redirect Preprocess {} Error'.format(ds.job_id))

        ds.data_types = ds.get_types()
        return ds

    ##########################################################################
    ##########################################################################
    # 전처리 동작
    ##########################################################################
    # 1. 열 삭제
    def drop_column(self, ds):
        if 'columns' not in ds.job_content:
            raise CustomUserError(502, '잘못된 요청입니다.', 'BAD_REQUEST')

        ds.dataset = ds.dataset.drop(columns = ds.job_content['columns'], axis = 1)
        ds.sync_dataset_with_dtypes()
        return ds

    ##########################################################################
    # 2. 결측치 처리
    def missing_value(self, ds):
        self.app.logger.info('missing_value / ' + str(ds.job_content))

        return self.handling_missing_value(ds)

    def handling_missing_value(self, ds):
        missing_value = ds.job_content['options']
        if missing_value == 'remove':  # ok
            ds = self.remove_missing_value(ds)
        elif missing_value == 'mean':  # ok
            ds = self.fill_missing_value_mean(ds)
        elif missing_value == 'median':  # ok
            ds = self.fill_missing_value_median(ds)
        elif missing_value in {'ffill', 'bfill'}:  # ok
            ds = self.fill_missing_value(ds)
        elif missing_value == 'input':  # ok
            ds = self.fill_missing_value_specified_value(ds)
        return ds

    def remove_missing_value(self, ds):
        if 'columns' in ds.job_content:
            ds.dataset = ds.dataset.dropna(subset = ds.job_content['columns'], axis = 0)
        else:
            ds.dataset = ds.dataset.dropna(axis = 0)
        return ds

    def fill_missing_value(self, ds):
        if 'columns' in ds.job_content:
            ds.dataset[ds.job_content['columns']] = ds.dataset[ds.job_content['columns']].fillna(method = ds.job_content['options'], axis = 0)
        else:
            ds.dataset = ds.dataset.fillna(method = ds.job_content['options'], axis = 0)
        return ds

    def fill_missing_value_specified_value(self, ds):
        if 'columns' in ds.job_content:
            ds.dataset[ds.job_content['columns']] = ds.dataset[ds.job_content['columns']].fillna(value = ds.job_content['input'])
        else:
            ds.dataset = ds.dataset.fillna(value = ds.job_content['input'])
        return ds

    def fill_missing_value_median(self, ds):
        ds.dataset[ds.job_content['columns']] = ds.dataset[ds.job_content['columns']].fillna(ds.dataset[ds.job_content['columns']].median())
        return ds

    def fill_missing_value_mean(self, ds):
        ds.dataset[ds.job_content['columns']] = ds.dataset[ds.job_content['columns']].fillna(ds.dataset[ds.job_content['columns']].mean())
        return ds

    ##########################################################################
    # 3. 컬럼 데이터 타입 변경
    def set_col_prop(self, ds):
        props = {
            'STR': 'string',
            'INT': 'int64',
            'FLOAT': 'float64',
            'BOOL': 'bool',
            'CATEGORY': 'category',
            'OBJECT': 'object'
        }

        ds.dataset[ds.job_content['column']] = ds.dataset[ds.job_content['column']].astype(props[ds.job_content['options']])
        return ds

    ##########################################################################
    # 4. 선택 열 date time 으로 변환 후 추가
    def set_col_prop_to_datetime(self, ds):
        dt_format = ''
        for ch in ds.job_content['dt_format']:
            if ch == '?':
                ch = '%'
            dt_format += ch
        if 'target_column' in ds.job_content:
            ds.dataset[ds.job_content['target_column']] = pd.to_datetime(ds.dataset[ds.job_content['column']], format = dt_format)
        else:
            ds.dataset[ds.job_content['column']] = pd.to_datetime(ds.dataset[ds.job_content['column']], format = dt_format)
        dt_format2 = ''
        for ch in dt_format:
            if ch == '%':
                ch = '?'
            dt_format2 += ch
        ds.job_content['dt_format'] = dt_format2
        return ds

    ##########################################################################
    # 5. 날짜 형 컬럼 분할 ex 날짜 -> 년, 월, 일, 시 ..
    def split_datetime(self, ds):
        for unit in ds.job_content['unit_list']:
            temp = str(ds.job_content['column']) + '_' + unit
            ds.dataset[temp] = self.split_variable_to_unit(ds, unit = unit)
            
        return ds

    def split_variable_to_unit(self, ds, unit):
        if unit == 'year':
            return ds.dataset[ds.job_content['column']].dt.year
        elif unit == 'yy':
            return ds.dataset[ds.job_content['column']].dt.year.astype(str).str[-2:]
        elif unit == 'month':
            return ds.dataset[ds.job_content['column']].dt.month
        elif unit == 'month_name':
            return ds.dataset[ds.job_content['column']].dt.month_name()
        elif unit == 'day':
            return ds.dataset[ds.job_content['column']].dt.day
        elif unit == 'day_of_week':
            return ds.dataset[ds.job_content['column']].dt.dayofweek
        elif unit == 'day_name':
            return ds.dataset[ds.job_content['column']].dt.day_name()
        elif unit == 'hour':
            return ds.dataset[ds.job_content['column']].dt.hour
        elif unit == 'minute':
            return ds.dataset[ds.job_content['column']].dt.minute
        elif unit == 'second':
            return ds.dataset[ds.job_content['column']].dt.second
        else:
            print('############ Split Variable To Unit Error')
            return ds

    ##########################################################################
    # 6. 날짜 처리(문자열로)
    def dt_to_str_format(self, ds):
        dt_format = ''
        for ch in ds.job_content['dt_format']:
            if ch == '?':
                ch = '%'
            dt_format += ch

        if 'target_column' in ds.job_content:
            ds.dataset[ds.job_content['target_column']] = ds.dataset[ds.job_content['column']].dt.strftime(dt_format)
        else:
            ds.dataset[ds.job_content['column']] = ds.dataset[ds.job_content['column']].dt.strftime(dt_format)

        dt_format2 = ''
        for ch in dt_format:
            if ch == '%':
                ch = '?'
            dt_format2 += ch
        ds.job_content['dt_format'] = dt_format2

        return ds

    ##########################################################################
    # 7. 날짜 처리(기준 일로 부터 날짜 차이)
    def diff_datetime(self, ds):

        # date = ds.job_content['input']
        # if len(date) == 8:
        #     # ex 19871102
        #     year = int(date[0:3])
        #     month = int(date[4:5])
        #     day = int(date[6:7])
        # elif len(date) == 6:
        #     # ex 220301
        #     year = int(date[0:1])
        #     month = int(date[2:3])
        #     day = int(date[4:5])
        
        column = ds.job_content['column']
        option = ds.job_content['option']
        unit = ds.job_content['unit']
        dt = ds.job_content['datetime']
        target_column = 'datetime_diff_{}_{}'.format(column, dt)
        
        if option == 'input':
            dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        elif option == 'column':
            pass
        
        if unit == 'seconds':
            dt_diff = ds.dataset[column] - dt
            dt_diff = dt_diff.apply(lambda x : int(x.total_seconds()))
        elif unit == 'minute':
            dt_diff = ds.dataset[column] - dt
            dt_diff = dt_diff.apply(lambda x : int(x.total_seconds() / 60))
        elif unit == 'hour':
            dt_diff = ds.dataset[column] - dt
            dt_diff = dt_diff.apply(lambda x : int(x.total_seconds() / 60 / 60))
        elif unit == 'day':
            dt_diff = ds.dataset[column] - dt
            dt_diff = dt_diff.apply(lambda x : int(x.total_seconds() / 60 / 60 / 24))
        elif unit == 'month':
            dt_diff = (ds.dataset[column].dt.year - dt.year) * 12 + ds.dataset[column].dt.month - dt.month
        elif unit == 'year':
            dt_diff = ds.dataset[column].dt.year - dt.year
        
        ds.dataset[target_column] = dt_diff
                
        # if 'hour' in ds.job_content:
        #     hour = ds.job_content['hour']
        #     dt_diff = ds.dataset[ds.job_content['column']] - datetime(year, month, day, hour)
        # else:
        #     dt_diff = ds.dataset[ds.job_content['column']] - datetime(year, month, day)

        # target_column = 'diff' + str(year) + '-' + str(month) + '-' + str(day) + '_with_' + str(ds.job_content['column'])
        # if ds.job_content['unit'] == 'day':
        #     ds.dataset[target_column] = dt_diff.dt.days
        # elif ds.job_content['unit'] == 'minute':
        #     ds.dataset[target_column] = dt_diff.dt.total_seconds() / 60
        # elif ds.job_content['unit'] == 'hour':
        #     ds.dataset[target_column] = dt_diff.dt.total_seconds() / 360
        # elif ds.job_content['unit'] == 'year':
        #     ds.dataset[target_column] = dt_diff.dt.year / 360
        # elif ds.job_content['unit'] == 'month':
        #     ds.dataset[target_column] = dt_diff.dt.month / 360

        return ds

    ##########################################################################
    # 8. 컬럼 순서 변경
    def change_column_order(self, ds):
        ds.dataset = ds.dataset.loc[:, list(ds.job_content['col_order_list'])]

        return ds

    ##########################################################################
    # 9. 대소문자 변환
    def case_sensitive(self, ds):
        if ds.job_content['options'] == 'UPP':
            ds.dataset[ds.job_content['column']] = ds.dataset[ds.job_content['column']].str.upper()
        elif ds.job_content['options'] == 'LOW':
            ds.dataset[ds.job_content['column']] = ds.dataset[ds.job_content['column']].str.lower()
        elif ds.job_content['options'] == 'CAP':
            ds.dataset[ds.job_content['column']] = ds.dataset[ds.job_content['column']].str.capitalize()
        elif ds.job_content['options'] == 'TIT':
            ds.dataset[ds.job_content['column']] = ds.dataset[ds.job_content['column']].str.title()
        else:
            pass

        return ds

    ##########################################################################
    # 10. 치환 입력값으로 교체
    def replace_by_input_value(self, ds):
        if ds.job_content['options'] == 'default':
            ds.dataset[ds.job_content['column']] = ds.dataset[ds.job_content['column']].str.replace(ds.job_content['to_replace'], ds.job_content['input'])
        elif ds.job_content['options'] == 'regex':
            to_replace = '(.*)' + str(ds.job_content['to_replace']) + '(.*)'
            value = r'\1' + str(ds.job_content['input']) + r'\2'
            ds.dataset[ds.job_content['column']].replace(to_replace = to_replace, value = value, regex = True, inplace = True)

        return ds

    ##########################################################################
    # 11. 공백 제거 (앞 뒤만 해당, 문자 사이 X)
    def remove_space_front_and_rear(self, ds):
        ds.dataset[ds.job_content['column']] = ds.dataset[ds.job_content['column']].str.strip()
        return ds

    ##########################################################################
    # 12. 중복 행 삭제
    def drop_duplicate_row(self, ds):
        if 'keep' in ds.job_content:
            ds.dataset.drop_duplicates(subset = ds.job_content['column'], keep = ds.job_content['keep'], inplace = True)
        else:
            ds.dataset.drop_duplicates(subset = ds.job_content['column'], keep = 'first', inplace = True)
        return ds

    ##########################################################################
    # 조회 1. 중복 값 확인                                            (단순 조회)
    def show_duplicate_row(self, ds):
        return ds.dataset[ds.job_content['column']].value_counts()

    ##########################################################################
    # 13. 연산
    def calculating_column(self, ds):
        if ds.job_content['options'] == 'arithmetic':
            ds = self.calc_arithmetic(ds)

        elif ds.job_content['options'] == 'function':
            ds = self.calc_function(ds)
        return ds

    # 13-1. 연산 동작(함수 선택 시)
    def calc_function(self, ds):
        function = ds.job_content['calc_function']
        columns = ds.job_content['columns']
        # 여러 컬럼에서만 동작하는 함수 단일 컬럼 X
        # mean, max, min, median, std, var
        if function == 'mean':
            result = ds.dataset[columns].mean(axis = 1)
        elif function == 'max':
            result = ds.dataset[columns].max(axis = 1)
        elif function == 'min':
            result = ds.dataset[columns].min(axis = 1)
        elif function == 'median':
            result = ds.dataset[columns].median(axis = 1)
        elif function == 'std':
            result = ds.dataset[columns].std(axis = 1)
        elif function == 'var':
            result = ds.dataset[columns].var(axis = 1)

        # 단일 컬럼에서만 동작하는 함수
        # sin, cos, abs, log,

        elif function == 'sin':
            result = np.sin(ds.dataset[columns])
        elif function == 'cos':
            result = np.cos(ds.dataset[columns])
        elif function == 'abs':
            result = np.abs(ds.dataset[columns])
        elif function == 'log':
            result = np.log(ds.dataset[columns])

        if 'target_column' in ds.job_content:
            target_column = ds.job_content['target_column']
        else:
            if type(columns) == 'string':
                target_column = function + '(' + columns + ')'
            else:
                target_column = function + '(' + ','.join(columns) + ')'
        ds.dataset[target_column] = result
        return ds

    # 13-2. 연산 동작(산술 연산 선택 시)
    def calc_arithmetic(self, ds):
        operator = ds.job_content['operator']
        column1 = ds.job_content['column1']
        print(ds.dataset.head())
        operand1 = ds.dataset[column1]
        # 2번 피연산자
        # 1. column명
        # 2. 상수
        # 3. column의 집계함수 값
        operand2 = None

        if ds.job_content['value_type'] == 'column':
            column2 = ds.job_content['value']
            operand2 = ds.dataset[column2]
        elif ds.job_content['value_type'] == 'aggregate':
            operand2, column2 = self.calc_column_aggregate_function(ds)
        elif ds.job_content['value_type'] == 'constant':
            operand2 = float(ds.job_content['value'])
            column2 = operand2

        print('operand1', operand1)
        print('operand2', operand2)

        if operator == 'add':
            result = operand1 + operand2
            operator = '+'

        elif operator == 'min':
            result = operand1 - operand2
            operator = '-'

        elif operator == 'mul':
            result = operand1 * operand2
            operator = '*'

        elif operator == 'div':
            result = operand1 / operand2
            operator = '/'

        elif operator == 'remainder':
            result = operand1 % operand2
            operator = '%'

        if 'target_column' in ds.job_content:
            target_column = ds.job_content['target_column']
        else:
            target_column = column1 + operator + column2
        ds.dataset[target_column] = result
        return ds

    # 13-3. 두번째 피연산자 == 컬럼의 집계값 사용 시
    def calc_column_aggregate_function(self, ds):
        column2 = ds.job_content['value']
        function = ds.job_content['function']
        result = 0
        if function == 'max':
            result = ds.dataset[column2].max(axis = 0)
        elif function == 'min':
            result = ds.dataset[column2].min(axis = 0)
        elif function == 'mean':
            result = ds.dataset[column2].mean(axis = 0)
        elif function == 'median':
            result = ds.dataset[column2].median(axis = 0)
        elif function == 'std':  # 표준편차
            result = ds.dataset[column2].std(axis = 0)
        elif function == 'var':  # 분산
            result = ds.dataset[column2].var(axis = 0)

        column_name = function + '(' + column2 + ')'
        return result, column_name

    ##########################################################################
    # 14. 행 삭제
    def drop_row(self, ds):
        drop_type = ds.job_content['options']
        if drop_type == 'INPT':
            # 지정 값 일치 삭제 INPT
            index = ds.dataset[ds.dataset[ds.job_content['column']] == ds.job_content['input']].index
        elif drop_type == 'INVL':
            # 유효하지 않은 데이터 삭제 INVL
            # 일단 결측 삭제
            index = ds.dataset[ds.dataset[ds.job_content['column']].isna() == True].index
        elif drop_type == 'NEGA':
            # 음수 값 로우 삭제 NEGA
            if ds.data_types[ds.job_content['column']] not in ('int', 'float', 'int64', 'float64'):
                self.app.logger.info('column [%s] is not (Int, float) type' % ds.job_content['column'])
                return ds
            index = ds.dataset[ds.dataset[ds.job_content['column']] < 0].index
        else:
            pass
        ds.dataset.drop(index, inplace = True, axis = 0)
        return ds

    ##########################################################################
    # 15. 컬럼 이름 변경
    def rename_col(self, ds):
        ds.dataset.rename(columns = {ds.job_content['column']: ds.job_content['target_column']}, inplace = True)
        return ds

    ##########################################################################
    # 16. 컬럼 분할 (구분자, 컬럼 길이로 분할, 역분할(뒤에서 부터))
    def split_col(self, ds):
        reverse = False if 'reverse' not in ds.job_content else ds.job_content['reverse']
        target_col = ds.job_content['column']
        options = ds.job_content['options']
        position = ds.job_content['position']
        
        if options == 'SEP':
            # ds.dataset[target_col].str.split(sep_input)
            # col_name = '{}_{}_by_{}'.format(options, target_col, sep_input)
            # ds.dataset[col_name] = ds.dataset[target_col].str.split(sep_input).str[position - 1]
            sep_input = ds.job_content['input']
            
            def set_max_len(target_list, max_len):
                if target_list == float('nan'):
                    target_list = []
                    return target_list
                else:
                    if len(target_list) < max_len:
                        for x in range(max_len - len(target_list)):
                            target_list.append('None')
                        return target_list
                    else:
                        return target_list
                        
            ds.dataset['split'] = ds.dataset[target_col].apply(lambda x:str(x).split(sep_input))
            max_len = ds.dataset['split'].apply(lambda x:len(x)).max()

            ds.dataset['split'] = ds.dataset['split'].apply(lambda x:set_max_len(x, max_len))

            for n in range(max_len):
                col_name = '{}_{}_by_{}_{}'.format(options, target_col, sep_input, n + 1)
                ds.dataset[col_name] = ds.dataset['split'].apply(lambda x:x[n])
                # self.app.logger.info(ds.dataset)
            ds.dataset = ds.dataset.drop('split', axis = 1)

        elif options == 'LEN':
            if reverse is True:
                col_name = '{}_{}_by_{}'.format(options, target_col, str(position * -1))
                ds.dataset[col_name] = ds.dataset[target_col].str[position * -1:]
            else:
                col_name = '{}_{}_by_{}'.format(ds.job_content['options'], target_col, str(position))
                ds.dataset[col_name] = ds.dataset[target_col].str[:position]
        else:
            # error 상황
            pass

        return ds

    ##########################################################################
    # 17. 결측치 처리 머신 러닝 모델 활용
    def missing_value_model(self, ds):
        # 분류, 회귀 구분
        # options

        if ds.job_content['options'] == 'regression':
            return self.regression_model(ds)
        elif ds.job_content['options'] == 'classification':
            return self.classification_model(ds)

    def regression_model(self, ds):
        X_target_is_not_0, X_target_is_0 = self.split_null_or_not(ds)

        models = list()
        models.append(LinearRegression())
        models.append(Lasso())

        model = self.test_model(X_target_is_not_0, models, ds.job_content['target_column'])
        ds.dataset[ds.job_content['target_column']] = self.execute_model(ds, X_target_is_not_0, X_target_is_0, model)
        return ds

    def classification_model(self, ds):
        X_target_is_not_0, X_target_is_0 = self.split_null_or_not(ds)

        models = list()
        models.append(LogisticRegression(solver = 'saga', max_iter = 2000))
        models.append(RandomForestClassifier(max_depth = 10))

        model = self.test_model(X_target_is_not_0, models, ds.job_content['target_column'])
        ds.dataset[ds.job_content['target_column']] = self.execute_model(ds, X_target_is_not_0, X_target_is_0, model)
        return ds

    def split_null_or_not(self, ds):
        target_column = ds.job_content['target_column']
        feature_list = list(ds.job_content['feature_list'])

        df = pd.DataFrame()
        # feature_scaling
        # 나중에 테스트 -> 사용자 이해도 문제
        for feature in feature_list:
            if ds.data_types[feature] in ('categories', 'string', 'object'):
                # 그냥 다 더미화
                scaled_data = pd.get_dummies(ds.dataset[feature])
                scaled_df = scaled_data
            elif ds.data_types[feature] in ('int64', 'float64', 'int', 'float'):
                # 그냥 다 정규화 해버리자
                scaler = StandardScaler()
                scaled_data = scaler.fit_transform(ds.dataset[[feature]])
                scaled_df = pd.DataFrame(scaled_data, columns = [feature])
            elif ds.data_types[feature] in ('datetime64[ns]'):
                # 그냥 다 정규화 해버리자
                scaled_df = pd.DataFrame()
                scaled_df['year'] = ds.dataset[feature].dt.year
                scaled_df['month'] = ds.dataset[feature].dt.month
                scaled_df['day'] = ds.dataset[feature].dt.day

            df = pd.concat([df, scaled_df], axis = 1)

        df = pd.concat([df, ds.dataset[target_column]], axis = 1)

        X_target_is_0 = df[df[target_column].isnull()]
        X_target_is_not_0 = df[df[target_column].notnull()]

        # df[target_column].fillna(0, inplace = True)
        # X_target_is_0 = df[df[target_column] == 0]
        # X_target_is_not_0 = df[df[target_column] != 0]

        return X_target_is_not_0, X_target_is_0

    def test_model(self, data_is_not_0, models, target_column):
        target = data_is_not_0[target_column]
        data_is_not_0.drop(columns = [target_column], inplace = True)
        x_train, x_test, y_train, y_test = train_test_split(data_is_not_0,
                                                            target,
                                                            test_size = 0.2, random_state = 5)

        RMSE = list()
        i = 0
        for model in models:
            model.fit(x_train, y_train)
            RMSE.append(mean_squared_error(y_test, model.predict(x_test), squared = False))
            print('RMSE_model_%d : %f' % (i + 1, RMSE[i]))
            i += 1

        tmp = min(RMSE)
        index = RMSE.index(tmp)

        print('%s / %f' % (models[index], tmp))

        return models[index]

    def execute_model(self, ds, X_target_is_not_0, X_target_is_0, model):
        y_train = ds.dataset[ds.job_content['target_column']]
        y_train.columns = [ds.job_content['target_column']]

        X_target_is_0.drop(columns = [ds.job_content['target_column']], inplace = True)
        target_predict = model.predict(X_target_is_0)
        X_target_is_0[ds.job_content['target_column']] = target_predict
        X_target_is_not_0[ds.job_content['target_column']] = y_train

        data = pd.concat([X_target_is_not_0, X_target_is_0])
        data[ds.job_content['target_column']] = data[ds.job_content['target_column']].round(2)
        data.sort_index(inplace = True)

        return data[ds.job_content['target_column']]

    ##########################################################################
    # 조회 2. 수식 비교 조회 ex) 몸무게 > 70 인 row (masking 검색)
    # 23. 조회 2. 수식 비교 조회 후 적용
    def conditioned_row(self, ds):
        column = ds.job_content['column']
        operator = ds.job_content['operator']
        value = (ds.job_content['value'])

        # print(ds.dataset[ds.dataset[column]].type())

        operator_list = [
            '==',
            '!=',
            '<=',
            '<',
            '>=',
            '>',
            'between',
            'else'
        ]

        if operator not in operator_list:
            return ds

        if operator == '==':
            # 다른 타입도 가능
            
            ds.dataset = ds.dataset[ds.dataset[column] == value]
        elif operator == '!=':
            ds.dataset =  ds.dataset[ds.dataset[column] != value]
        elif operator == '<=':
            ds.dataset =  ds.dataset[ds.dataset[column] <= value]
        elif operator == '<':
            ds.dataset =  ds.dataset[ds.dataset[column] < value]
        elif operator == '>=':
            ds.dataset =  ds.dataset[ds.dataset[column] >= value]
        elif operator == '>':
            ds.dataset =  ds.dataset[ds.dataset[column] > value]
        elif operator == 'between':
            value2 = ds.job_content['value2']
            ds.dataset =  ds.dataset[(value <= ds.dataset[column]) & (value2 >= ds.dataset[column])]
        elif operator == 'else':
            value2 = ds.job_content['value2']
            ds.dataset =  ds.dataset[(value > ds.dataset[column]) | (value2 < ds.dataset[column])]

        return ds

    ##########################################################################
    # 18. 단위 변환 ex) kg -> g
    def unit_conversion(self, ds):

        options = ds.job_content['options']
        current_unit = ds.job_content['current_unit']
        conversion_unit = ds.job_content['conversion_unit']

        column = ds.job_content['column']
        if current_unit in column:
            target_column = column.split('(')[0]
        else:
            target_column = column

        target_column = '%s(%s)' % (target_column, conversion_unit)

        if options == 'temperature':
            if current_unit == 'Celsius':
                # 섭씨 -> 화씨
                # F = C(1.8) + 32
                ds.dataset[target_column] = (ds.dataset[column] * 1.8) + 32
            elif current_unit == 'Fahrenheit':
                # 화씨 -> 섭씨
                # C = (F-32) / 1.8
                ds.dataset[target_column] = (ds.dataset[column] - 32) / 1.8
        else:
            unit_data = self.get_unit(options)
            ds.dataset[target_column] = ds.dataset[column] / unit_data[current_unit] * unit_data[conversion_unit]
        ds.dataset[target_column] = ds.dataset[target_column].round(2)
        return ds

    def get_unit(self, options):
        if options == 'length':
            # 기준 m
            return {
                'cm': 100,
                'mm': 1000,
                'm': 1,
                'km': 0.001,
                'in': 39.370079,
                'ft': 3.28084,
                'yd': 1.093613,
                'mile': 0.000621
            }
        elif options == 'weight':
            # 기준 kg
            return {
                'mg': 1000000,
                'g': 1000,
                'kg': 1,
                't': 0.001,
                'kt': 1e-6,
                'gr': 15432.3584,
                'oz': 35.273962,
                'lb': 2.204623
            }
        elif options == 'area':
            # 기준 m^2
            return {
                'm^2': 1,
                'a': 0.01,
                'ha': 0.0001,
                'km^2': 1e-6,
                'ft^2': 10.76391,
                'yd^2': 15432.3584,
                'ac': 0.000247105,
                '평': 0.3025
            }
        elif options == 'volume':
            # 기준 m
            return {
                'l': 1,
                'cc': 1000,
                'ml': 1000,
                'dl': 10,
                'cm^3': 1000,
                'm^3': 0.001,
                'in^3': 61.023744,
                'ft^3': 0.035314667,
                'yd^3': 0.001307951,
                'gal': 0.264172052,
                'bbl': 0.0062932662
            }
        elif options == 'speed':
            # 기준 m/s
            return {
                'm/s': 1,
                'm/h': 3600,
                'km/s': 0.001,
                'km/h': 3.6,
                'in/s': 39.370079,
                'in/h': 141732.283,
                'ft/s': 3.28084,
                'ft/h': 11811.0236,
                'mi/s': 0.000621,
                'mi/h': 2.236936,
                'kn': 1.943844,
                'mach': 0.002941
            }

    ##########################################################################
    # 19. row, column concat(연결) (다중 가능)
    # 기능 : 행, 열 선택 / join 방식 선택 / 행 concat 시 중복 컬럼 삭제 여부

    def concat(self, ds):
        if 'content_of_load_dataset' in dict(ds.job_content):
            ds.content_of_load_dataset = ds.job_content['content_of_load_dataset']
        
        # concat 대상 데이터셋 로드
        content = ds.content_of_load_dataset[0]

        content['project_id'] = ds.project_id
        ds_new = self.dataset.Dataset(content)

        ds_new.load_dataset_from_warehouse_server()

        join = 'outer' if 'join' not in ds.job_content else ds.job_content['join']
        axis = ds.job_content['axis']

        if 'job_id_list' in ds.job_content['content_of_load_dataset'][0]:
            job_id_list = tuple(ds.job_content['content_of_load_dataset'][0]['job_id_list'])

            if str(job_id_list)[-2] == ',':
                job_id_list = str(job_id_list)[:-2] + str(job_id_list)[-1:]

            redo_job_history = self.jhDAO.select_redo_job_history(job_id_list)
            i = 0
            for row in redo_job_history:
                ds_new.job_id = row['job_id']
                ds_new.job_content = row['content']
                i += 1
                self.app.logger.info('Redo Action ' + str(i) + '. ')
                ds_new = self.redirect_preprocess(ds = ds_new)
                ds_new.data_types = ds_new.get_types()

        else:
            ds_new = self.redo_job_history(ds = ds_new)
         
        ds.dataset = pd.concat([ds.dataset, ds_new.dataset], axis = axis, join = join, ignore_index = True).reset_index(drop = True)

        # # concat 할 데이터 셋들
        # content_of_load_dataset = list(ds.content_of_load_dataset)

        # ds_list = self.load_datasets_for_concat(ds.status, ds.project_id, content_of_load_dataset)

        # # join
        # join = 'outer' if 'join' not in ds.job_content else ds.job_content['join']

        # # ignore_index
        # if 'ignore_index' in ds.job_content:
        #     ignore_index = True if ds.job_content['ignore_index'] == 1 else False
        # else:
        #     ignore_index = False

        # axis = ds.job_content['axis']
        # if axis == 1:
        #     if ds.job_content['drop_duplicate_column'] == 1:
        #         ds_list = self.drop_duplicate_column_in_datasets(ds.dataset.columns, ds_list)
        #     else:
        #         ds_list = self.suffix_duplicate_column_in_datasets(ds_list)
        # # else:
        # #     ds_list = self.suffix_duplicate_column_in_datasets(ds_list)

        # ds_list.insert(0, ds)

        # dataset_list = list()
        # for ds_dataset in ds_list:
        #     dataset_list.append(ds_dataset.dataset)

        # ds.dataset = pd.concat(dataset_list, axis = axis, join = join, ignore_index = ignore_index)
        # ds.dataset.reset_index(inplace = True)
        return ds

    # def load_datasets_for_concat(self, ds_status, project_id, content_of_load_dataset):
    #     ds_list = list()
    #     for content in content_of_load_dataset:
    #         # content ['file_id', 'version']
    #         content['project_id'] = project_id
    #         ds_new = self.dataset.Dataset(content)
    #         ds_new.status = ds_status
    #         ds_new.load_dataset_from_warehouse_server()
    #         ds_list.append(ds_new)
    #     return ds_list

    # def suffix_duplicate_column_in_datasets(self, ds_list):
    #     for ds_new in ds_list:
    #         suffix = '_%s' % ds_new.file_id
    #         ds_new.dataset = ds_new.dataset.add_suffix(suffix)
    #     return ds_list

    # def drop_duplicate_column_in_datasets(self, origin_columns, ds_list):
    #     # 중복 컬럼 삭제 선택 시
    #     unique_columns = origin_columns
    #     for ds_new in ds_list:
    #         cols_to_use = ds_new.dataset.columns.difference(unique_columns)
    #         unique_columns.append(cols_to_use)
    #         ds_new.dataset = ds_new.dataset[cols_to_use]
    #     return ds_list

    ##########################################################################
    # 20. merge(병합) (2개 데이터 셋만 가능)
    def merge(self, ds):
        if 'content_of_load_dataset' in dict(ds.job_content):
            ds.content_of_load_dataset = ds.job_content['content_of_load_dataset']
                
        # 추가할 데이터 셋 불러와야 함
        content = ds.content_of_load_dataset[0]

        content['project_id'] = ds.project_id
        ds_new = self.dataset.Dataset(content)

        ds_new.load_dataset_from_warehouse_server()
        # print(ds_new.dataset.dtypes)

        # 필요한 파라미터들
        # how               -- (not essential)
        # both              -- key
        # left_right_column -- left_column, right_column
        # left_right_index  -- none
        options = ds.job_content['options']

        how = 'inner' if ds.job_content['how'] == '선택' else ds.job_content['how']
        # how = 'inner' if 'how' not in ds.job_content else ds.job_content['how']

        # 병합 대상 데이터셋 redo
        if 'job_id_list' in ds.job_content['content_of_load_dataset'][0] and len(ds.job_content['content_of_load_dataset'][0]['job_id_list']) != 0:
            job_id_list = tuple(ds.job_content['content_of_load_dataset'][0]['job_id_list'])

            if str(job_id_list)[-2] == ',':
                job_id_list = str(job_id_list)[:-2] + str(job_id_list)[-1:]
            # trans_table = job_id_list.maketrans('[]', '()')
            # job_id_list = job_id_list.translate(trans_table)
            redo_job_history = self.jhDAO.select_redo_job_history(job_id_list)
            i = 0
            for row in redo_job_history:
                ds_new.job_id = row['job_id']
                ds_new.job_content = row['content']
                i += 1
                self.app.logger.info('Redo Action ' + str(i) + '. ')
                ds_new = self.redirect_preprocess(ds = ds_new)
                ds_new.data_types = ds_new.get_types()
        
        else:
            ds_new = self.redo_job_history(ds = ds_new)

            # 병합 데이터셋 redo job 목록 입력
            redo_job_list = self.get_job_historys(ds_new)
            job_id_list = [x['id'] for x in redo_job_list]
            content['job_id_list'] = job_id_list
            # self.app.logger.info("----------------------------------------------------------")
            # self.app.logger.info(redo_job_list)

        # if options == 'both':
        if '동일' in options:
            ds.dataset = pd.merge(ds.dataset, ds_new.dataset, left_on = ds.job_content['key'], right_on = ds.job_content['key'], how = how)
        # elif options == 'left_right_column':
        #     ds.dataset = pd.merge(ds.dataset, ds_new.dataset, left_on = ds.job_content['left_column'], right_on = ds.job_content['right_column'], how = how)
        # elif options == 'left_right_index':
        elif '인덱스' in options:
            ds.dataset = pd.merge(ds.dataset, ds_new.dataset, left_index = True, right_index = True, how = 'outer')

        return ds

    ###########################################################################
    # 21. 작업 규칙 저장
    def save_work_step(self, ds, results):
        work_step_id = uuid.uuid1()
        # 임시 ID
        group_id = 'group' + str(random.randint(100, 500))
        department_id = 'department' + str(random.randint(100, 500))
        user_id = 'user' + str(random.randint(100, 500))
        
        work_step = {
            'id' : work_step_id,
            'group_id': group_id,
            'department_id': department_id,
            'user_id': user_id,
            'project_id': ds.project_id,
            'file_id': ds.file_id,
            'version': ds.version,
            'work_step_name': ds.job_content['work_step_name'],
            'description': ds.job_content['description']
        }
        
        self.jhDAO.insert_work_step(work_step = work_step)
        
        for result in results:
            work_step_details = {
                'id' : uuid.uuid1(),
                'group_id': group_id,
                'department_id': department_id,
                'user_id': user_id,
                'work_step_id' : work_step_id,
                'user_id': user_id,
                'job_id': result['job_id'],
                'content': re.sub('\'', '\"', str(result['content'])),
            }

            self.jhDAO.insert_work_step_details(work_step_details = work_step_details)

    def get_work_step(self, params):
        return self.jhDAO.select_work_step(params)

    ###########################################################################    
    # 22. 작업 규칙 적용하기
    def apply_work_step(self, ds):
        work_step_id = ds.job_content['id']
        results = self.jhDAO.select_work_step_details(work_step_id = work_step_id)
        
        for result in results:
            ds.job_id = result['job_id']
            ds.job_content = result['content']
            ds = self.redirect_preprocess(ds = ds)
            ds.data_types = ds.get_types()
            
        ds.job_content = {'id': work_step_id}
        ds.job_id = 'apply_work_step'
        
        return ds

    ###########################################################################    
    # 22-1. 작업 규칙 삭제하기
    def delete_work_step(self, payload):
        work_step_id = payload['id']
        self.jhDAO.delete_work_step(work_step_id)
        self.jhDAO.delete_work_step_details(work_step_id)

    ###########################################################################
    # 24. 결측 수식 적용
    def missing_value_calc(self, ds):
        df_col_not_null, df_col_is_null = self.split_row_null_or_not(ds)

        ds_is_null = copy.deepcopy(ds)
        ds_is_null.dataset = df_col_is_null
        if ds.job_content['options'] == 'arithmetic':
            ds_is_null = self.calc_arithmetic(ds_is_null)
        elif ds.job_content['options'] == 'function':
            ds_is_null = self.calc_function(ds_is_null)

        df = pd.concat([df_col_not_null, ds_is_null.dataset], axis = 0)
        df = df.sort_index()

        ds.dataset = df
        return ds

    def split_row_null_or_not(self, ds):
        target_column = ds.job_content['target_column']

        df_col_is_null = ds.dataset[ds.dataset[target_column].isnull()]
        df_col_not_null = ds.dataset[ds.dataset[target_column].notnull()]

        return df_col_not_null, df_col_is_null

    ###########################################################################
    # 25. 논리 반전
    def reverse_boolean(self, ds):
        column = ds.job_content['column']

        if ds.data_types[column] != 'bool':
            self.app.logger.info('해당 컬럼({})은 bool 타입이 아닙니다.'.format(column))
            return ds

        ds.dataset[column] = ~ds.dataset[column]
        return ds

    ###########################################################################
    # 26. 컬럼을 인덱스로 설정
    def set_col_to_index(self, ds):
        column = ds.job_content['column']
        if ds.dataset[column].nunique() < len(ds.dataset[column]):
            raise CustomUserError(502, '해당 컬럼({})은 unique 조건을 충족하지 못했습니다.'.format(column), 'BAD_REQUEST')
            return ds
        if ds.dataset[column].isna().sum() > 0:
            raise CustomUserError(502, '해당 컬럼({})은 결측치가 존재합니다.'.format(column), 'BAD_REQUEST')
            return ds

        ds.dataset.set_index(column, inplace = True)
        return ds

    ###########################################################################
    # 27. 음수 값 처리
    def cleansing_negative_value(self, ds):
        options = ds.job_content['options']
        column = ds.job_content['column']

        if ds.data_types[column] not in ('int64', 'float64'):
            self.app.logger.info('해당 컬럼(%s[%s])은 수치형 컬럼이 아닙니다.' % (column, ds.data_types[column]))
            return ds

        mask = ds.dataset[column] < 0
        if options == 'to_zero':
            ds.dataset.loc[mask, column] = 0
        elif options == 'to_input':
            input_value = ds.job_content['input']
            ds.dataset.loc[mask, column] = input_value
        else:
            # error
            pass
        return ds

    ###########################################################################
    # 28. 소수점 처리
    def round_value(self, ds):
        options = ds.job_content['options']
        column = ds.job_content['column']

        print(ds.data_types[column])

        if ds.data_types[column] not in ('int64', 'float64'):
            self.app.logger.info('해당 컬럼(%s[%s])은 수치형 컬럼이 아닙니다.' % (column, ds.data_types[column]))
            return ds

        if options == 'DOWN': # 내림
            ds.dataset[column] = ds.dataset[column].apply(np.floor)
        elif options == 'CEIL': # 올림
            ds.dataset[column] = ds.dataset[column].apply(np.ceil)
        elif options == 'HALF': # 반올림
            num_of_digits = ds.job_content['digits'] if 'digits' in ds.job_content else 2  # DEFAULT
            ds.dataset[column] = ds.dataset[column].round(decimals = num_of_digits)
        elif options == 'TRUNC': # 버림
            ds.dataset[column] = ds.dataset[column].apply(np.trunc)

        return ds
    
    ###########################################################################
    # 29. 정렬
    def sort_col(self, ds):
        options = ds.job_content['options']
        column = ds.job_content['column']
        if options == 'ASC': 
            ds.dataset = ds.dataset.sort_values(by = column, ascending = True)
        elif options == 'DESC':
            ds.dataset = ds.dataset.sort_values(by = column, ascending = False)
    
        return ds   

    ###########################################################################
    # 30. 그룹별 집계 
    def group_by(self, ds):
        group_list = ds.job_content['columns'] # 리스트 형식
        agg_col_list = ds.job_content['target_columns'] # 리스트 형식
        options_list = ds.job_content['options'] # 리스트 내 리스트 형식
        dic = {}
        for x in range(len(agg_col_list)):
            dic[agg_col_list[x]] = options_list[x]

        gb_df = ds.dataset.groupby(group_list).agg(dic)

        multi_col = gb_df.columns.tolist()
        rename_col = ['{}_{}'.format(x[0], x[1]) for x in multi_col]

        gb_df = gb_df.droplevel(0, 1)
        gb_df.columns = rename_col
        gb_df = gb_df.reset_index()

        ds.dataset = ds.dataset.merge(gb_df, how = 'left', on = group_list)

        return ds
        # options = ds.job_content['options']
        # column = ds.job_content['column']
        
        # if options == 'mean':
        #     ds.dataset = ds.dataset.groupby([column], as_index = False)[ds.job_content['target_column']].mean()
        # elif options == 'sum':
        #     ds.dataset = ds.dataset.groupby([column], as_index = False)[ds.job_content['target_column']].sum()
        # elif options == 'count':
        #     ds.dataset = ds.dataset.groupby([column], as_index = False)[ds.job_content['target_column']].count()

        # return ds
    
    ###########################################################################
    # 31. 행/열 전환 
    def transpose_df(self, ds):
        ds.dataset = ds.dataset.transpose()
        return ds
 
    ###########################################################################   
    # 32. 필터 
    def show_filter(self, ds) :
        options = ds.job_content['options'] 
        column = ds.job_content['column']
        
        if options == 'valid':
            #유효한 값 행
            ds.dataset =  ds.dataset[ds.dataset[column].notnull()]
        elif options == 'invalid':
            pass
        elif options == 'empty':
            #비어있는 행
            ds.dataset =  ds.dataset[ds.dataset[column].isnull()]
        return ds

    ###########################################################################    
    # 33. 추출 - 찾아서 입력값으로 삭제
    def remove_by_input_value(self, ds):
        options = ds.job_content['options'] 
        column = ds.job_content['column']     
        to_remove = ds.job_content['to_remove']     
          
        if options == 'INPT':
            ds.dataset[column] = ds.dataset[column].str.replace(to_remove, '')
        elif options == 'PATTN':
            pass
        return ds
    
    ###########################################################################
    # 34. 컬럼 병합
    def concat_col(self, ds):
        column_name = ds.job_content['column_name']
        options = ds.job_content['options']

        if options == 'CONT':
            columns = [ds.job_content['column'], ds.job_content['target_column']] 
            sep = ds.job_content['sep']         
            for idx, column in enumerate(columns):
                if idx == 0:
                    if column in ds.dataset.columns:
                        ds.dataset[column_name] = ds.dataset[column]
                    else:
                        ds.dataset[column_name] = column
                else:
                    if column in ds.dataset.columns:
                        ds.dataset[column_name] = ds.dataset[column_name].astype(str) + sep + ds.dataset[column].astype(str)
                    else:
                        ds.dataset[column_name] = ds.dataset[column_name].astype(str) + sep + column
        elif options == 'STR':
            column = ds.job_content['column']
            type = ds.job_content['type']
            input = ds.job_content['input']
            if '뒤' in type:
                ds.dataset[column_name] = ds.dataset[column].astype(str) + input
            else:
                ds.dataset[column_name] = input + ds.dataset[column].astype(str)
                    
        return ds
    
    ###########################################################################    
    # 35. 이상처 처리-iqr
    def outlier_iqr(self,ds):
        column = ds.job_content['column']    
        q1 = ds.dataset[column].quantile(0.25)
        q3 = ds.dataset[column].quantile(0.75)
        iqr = q3 - q1
        outlier_col = ds.dataset[column][(q3 + 1.5  * iqr < ds.dataset[column]) | (q1 - 1.5 * iqr > ds.dataset[column])].index
        ds.dataset.drop(outlier_col, axis = 0, inplace = True)

        return ds    
    
    ###########################################################################   
    # 36. 이상치처리-입력값
    def outlier_inpt(self, ds):
        options = ds.job_content['options'] 
        column = ds.job_content['column']
        lower_bound = ds.job_content['lower'] 
        upper_bound = ds.job_content['upper'] 
        job_type = ds.job_content['job_type']

        if options =='VAL':
            if job_type == 'CHANGE':
                lower = ds.dataset[column][ds.dataset[column] < lower_bound].index
                upper = ds.dataset[column][ds.dataset[column] > upper_bound].index
                ds.dataset.loc[lower, column] = lower_bound
                ds.dataset.loc[upper, column] = upper_bound
            elif job_type == 'REMOVE':
                outlier_col = ds.dataset[column][(ds.dataset[column] < lower_bound) | (ds.dataset[column] > upper_bound)].index
                ds.dataset.drop(outlier_col, axis = 0, inplace = True)
        
        elif options == 'PER':
            lower_bound = lower_bound
            upper_bound = upper_bound
            if job_type == 'CHANGE':
                lower_outlier = ds.dataset[column].quantile(lower_bound)
                upper_outlier = ds.dataset[column].quantile(upper_bound)
                lower = ds.dataset[column][ds.dataset[column] < lower_outlier].index
                upper = ds.dataset[column][ds.dataset[column] > upper_outlier].index
                ds.dataset.loc[lower, column] = lower_outlier
                ds.dataset.loc[upper, column] = upper_outlier             
            elif job_type == 'REMOVE':
                outlier_col = ds.dataset[column][(ds.dataset[column] < lower_bound) | (ds.dataset[column] > upper_bound)].index
                ds.dataset.drop(outlier_col, axis = 0, inplace = True)
                
        return ds
    
    ###########################################################################
    # 37. 이상치처리-빈도
    def outlier_value_counts(self, ds):
        column = ds.job_content['column']
        count = ds.job_content['count']
        options = ds.job_content['options']
        
        vc_df = pd.DataFrame(ds.dataset[column].value_counts()).reset_index()
        
        index_list = list(vc_df[vc_df[column] <= vc_df[column][len(vc_df) - count]]['index'])
        
        if options == 'input':
            ds.dataset.loc[ds.dataset[column].isin(index_list), column] = ds.job_content['input']
        elif options == 'mode':
            ds.dataset.loc[ds.dataset[column].isin(index_list), column] = ds.dataset[column].mode()[0]
        
        return ds
    
    # 37. 이상치처리-밀도 - 검토
    # def outlier_clustering(self, ds):
    #     X_train = ds.dataset[[ds.job_content['column']]]
    #     options = ds.job_content['options'] 
    #     if options == 'AUTO':
    #         cd = cdist(X_train, X_train)
    #         distance = np.quantile(cd, 0.1)
    #         cluster_model = DBSCAN(eps = distance, min_samples = 2).fit(X_train)
    #     elif options == 'INPT':
    #         eps = ds.job_content['eps']
    #         min_samples = ds.job_content['min_samples']
    #         cluster_model = DBSCAN(eps = eps, min_samples = min_samples).fit(X_train)

    #     X_train = X_train[cluster_model.labels_ != -1]
    #     X_train = X_train.rename(columns = {ds.job_content['column']: 'new'})
    #     ds.dataset = pd.concat([ds.dataset, X_train], axis = 1)
    #     ds.dataset.dropna(subset = ['temp'], inplace = True)
    #     ds.dataset.drop(columns = ['temp'], inplace = True)

    #     return ds

    ###########################################################################   
    # 38. 테이블 편집 - 비어있는 모든 행 처리
    def missing_value_df(self, ds):
        missing_value = ds.job_content['options']
        if missing_value == 'remove':  # ok
            ds.dataset.dropna(inplace = True)
        elif missing_value == 'input':  # ok
            ds.dataset.fillna(ds.job_content['input'], inplace = True)
        elif missing_value == 'show':  # ok
            ds.dataset[ds.dataset.isna().any(axis = 1)]
        
        return ds
    
    # 39. 테이블 편집 - 중복된 모든 행 처리
    def duplicated_df(self, ds):
        duplicated = ds.job_content['options']
        if duplicated == 'remove':
            ds.dataset = ds.dataset.drop_duplicates()
        elif duplicated == 'show':
            ds.dataset = ds.dataset[ds.dataset.duplicated()]
            
        return ds
    
    ###########################################################################
    # 41. 컬럼 생성
    def create_new_column(self, ds):
        column_name = ds.job_content['column_name']
        
        ds.dataset[column_name] = None
        return ds
    
    # 42. 컬럼 복사
    def copy_column(self, ds):
        column_name = ds.job_content['column_name']
        column = ds.job_content['column']
        
        ds.dataset[column_name] = ds.dataset[column].copy()
        return ds

    ###########################################################################
    # 2. 추출 동작
    ###########################################################################

    # 원본 파일 동작 재 수행
    def redo_job_history(self, ds):
        i = 0
        for row in self.get_job_historys(ds):
            ds.job_id = row['job_id']
            ds.job_content = row['content']
            i += 1
            self.app.logger.info('Redo Action ' + str(i) + '. ')
            ds = self.redirect_preprocess(ds = ds)
            ds.data_types = ds.get_types()

        return ds

    # 3-1. job_history load
    def get_job_historys(self, ds):
        return self.jhDAO.select_job_history_by_file_id_and_version(project_id = ds.project_id, file_id = ds.file_id, version = ds.version)
        # return self.jhDAO.select_job_history_by_file_id_and_version(id)

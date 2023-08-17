import json, datetime
from .util.custom_exception import CustomTypeError
from .util.custom_exception import CustomUserError
from .util.validator import preprocessing_validator
from .util.validator import undo_redo_validator
import os
from flask import send_file, g
from sqlalchemy import create_engine

import time

import chardet
import pandas as pd
import json
import re

import uuid

from pathlib import Path
import traceback

class Preprocessing:

    def __init__(self, app, hd, dataset):
        self.app = app
        self.hd = hd
        self.dataset = dataset
        # self.hd = handling_dataset.HandlingDataset(app, dsDAO, jhDAO)

    def before_request(self, request):
        g.history_id = uuid.uuid1()
        addr = request.remote_addr
        method = request.method
        path = request.path

        if path == '/project/file':
            return

        api_history = {
            'id' : g.get('history_id'),
            'group_id' : 'group001',
            'department_id' : 'department001',
            'user_id' : 'user001',
            'ip' : addr,
            'method' : method,
            'path' : path
        }

        if method == 'POST':
            params = request.get_json(force = True).copy()
            if 'dataset' in params.keys():
                params.pop('dataset')
            if 'dataset_dtypes' in params.keys():
                params.pop('dataset_dtypes')
            params = json.dumps(params, ensure_ascii = False)
            api_history.update({'params' : params})

        self.hd.insert_api_history(api_history)

    def after_requset(self, response):
        api_history = {
            'id' : g.get('history_id'),
            'response' : response.status_code
        }

        self.hd.update_api_history(api_history)

    #######################################################################################
    #######################################################################################
    #######################################################################################

    def create_directory(self, directory):
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print('Error: Failed to create the directory.')

    def set_directory(self, project_id):
        original_dir = os.path.join('server', project_id, 'original_data')
        sample_dir = os.path.join('server', project_id, 'sample_data')
        preprocessing_dir = os.path.join('server', project_id, 'preprocessing_data')
        profiling_dir = os.path.join('server', project_id, 'profiling_data')
        download_dir = os.path.join('server', project_id, 'download_data')

        if not os.path.exists(original_dir):
            self.create_directory(original_dir)
            self.create_directory(sample_dir)
            self.create_directory(preprocessing_dir)
            self.create_directory(profiling_dir)
            self.create_directory(download_dir)

        return original_dir, sample_dir, preprocessing_dir, profiling_dir, download_dir

    # 파일 리스트 호출
    def read_file_list(self, path):
        try:
            file_list = list([])

            for dir_path, _, file_name in os.walk(path):
                for f in file_name:
                    try:
                        file_list.append(os.path.abspath(os.path.join(dir_path, f)))
                    except:
                        print('######## Read File \"{}\" Error'.format(file_name))

            return file_list
        except:
            print('############ Read Path \"{}\" Error'.format(path))

    def check_file_dup(self, path, name):
        file_list = [os.path.split(file)[1] for file in self.read_file_list(path)]

        if name not in file_list:
            return True
        else:
            return False

    def check_extension(self, extension):
        allowed_extension = ['csv', 'json', 'xlsx', 'xls']

        if extension in allowed_extension:
            return True
        else:
            return False

    def detect_encoding(self, file):
        detector = chardet.universaldetector.UniversalDetector()

        with open(file, 'rb') as f:
            for line in f:
                detector.feed(line)
                if detector.done:
                    break
        detector.close()

        return detector.result

    def list_normalize(self, key, _list):
        if len(_list) == 0:
            df = pd.DataFrame([], columns = [key])
            df[key] = [None]
            return df

        if isinstance(_list[0], dict):
            df = pd.DataFrame([])
            for value in _list:
                df = pd.concat([df, pd.json_normalize(value)], axis = 0).reset_index(drop = True)

            rename_dict = {}
            for column in df.columns:
                rename_dict.update({column : '{}.{}'.format(key, column)})

            df = df.rename(rename_dict, axis = 1)
        else:
            df = pd.DataFrame({key : _list})

        return df

    def transform_dataframe(self, df):
        list_column = [column for column in df.columns if isinstance(df.loc[0, column], list)]

        if len(list_column) == 0:
            return df

        value_df = df[[column for column in df.columns if column not in list_column]]
        # value_df.drop_duplicates(replace = True)
        value_df.drop_duplicates()

        for column in list_column:
            list_df = pd.concat(list(df[column].apply(lambda x : self.list_normalize(column, x))), axis = 0)
            value_df = value_df.merge(list_df, 'cross')

        return self.transform_dataframe(value_df)

    def json_to_dataframe(self, json):
        return self.transform_dataframe(pd.json_normalize(json))

    # 샘플링
    def sampling_dataset(self, df, sampling):
        sampling_dict = eval(sampling)

        sample_type = sampling_dict['sample_type']
        count_type = sampling_dict['count_type']
        if 'sample_count' in sampling_dict.keys():
            sample_count = sampling_dict['sample_count']
            
            if sample_count > 500:
                sample_count = 500
        else:
            if sample_type == 'default':
                if count_type == 'count':
                    sample_count = 500
                elif count_type == 'percent':
                    sample_count = 10
            else:
                self.app.logger.error('#### 샘플링 설정 수를 입력하세요.')

        if count_type == 'count' and sample_count > len(df):
            self.app.logger.error('#### 샘플링 설정 수가 최대값을 초과합니다.')
        if count_type == 'percent' and sample_count > 100:
            self.app.logger.error('#### 샘플링 설정 수가 최대값을 초과합니다.')

        if sample_type == 'default' or sample_type == 'front':
            if count_type == 'count':
                return df[:sample_count]
            elif count_type == 'percent':
                return df[:int(len(df) * sample_count / 100)]
            else:
                self.app.logger.error('#### Plz Check Count Type')
        elif sample_type == 'back':
            if count_type == 'count':
                return df[-sample_count:]
            elif count_type == 'percent':
                return df[int(-len(df) * sample_count / 100):]
            else:
                self.app.logger.error('#### Plz Check Count Type')
        elif sample_type == 'random':
            if count_type == 'count':
                return df.sample(n = sample_count)
            elif count_type == 'percent':
                return df.sample(frac = sample_count / 100)
            else:
                self.app.logger.error('#### Plz Check Count Type')
        else:
            self.app.logger.error('#### Plz Check Sample Type')

    def set_data(self, file_full_path, file_name, extension, sample_dir, preprocessing_dir, sampling, options = None):
        detect_result = self.detect_encoding(file_full_path)
        encoding_type = detect_result['encoding']

        # preprocessing_data 내 json 형식으로 저장해야함
        # 현 임시 csv만 읽음
        if extension == 'csv':
            df = pd.read_csv(file_full_path, encoding = encoding_type)
        elif extension in ['xlsx', 'xls']:
            if options == None:
                df = pd.read_excel(file_full_path)
            else:
                options = eval(options)
                sheet_idx = int(options['sheet_idx']) -1
                header_idx = int(options['header_idx']) -1
                skip_row_idx = int(options['skip_row_idx']) -1
                if options['col_idx'] == None:
                    df = pd.read_excel(file_full_path, sheet_name = sheet_idx, header = header_idx, skiprows = skip_row_idx)
                else:
                    col_idx = int(options['col_idx']) - 1
                    pre_df = pd.read_excel(file_full_path)
                    col_max = len(pre_df.columns)
                    df = pd.read_excel(file_full_path, sheet_name = sheet_idx, header = header_idx, skiprows = skip_row_idx, usecols = list(range(col_idx, col_max)))
        elif extension == 'json':
            with open(file_full_path, encoding = 'UTF-8') as file:
                json_dict = json.load(file)

            df = self.json_to_dataframe(json_dict)

            rename_dict = {}
            for column in df.columns:
                rename_dict.update({column : re.sub('[.]', '_', column)})

            df = df.rename(rename_dict, axis = 1)
        else:
            raise Exception('허용되지 않은 파일 형식입니다.')

        sample_df = self.sampling_dataset(df, sampling)

        sample_df.to_json(sample_dir + '/' + file_name + '_V1.00.' + 'json', force_ascii=False)
        df.to_json(preprocessing_dir + '/' + file_name + '_V1.00.' + 'json', force_ascii=False)

        return df, encoding_type

    # 파일 업로드
    def upload_file(self, request):
        try:
            if 'file' not in request.files:
                print('######## No File')
                raise Exception()

            if 'project_id' not in request.form:
                print('######## No Project ID')
                raise Exception()

            if request.files['file'] == '':
                print('######## No File Name')
                raise Exception()

            project_id = request.form.get('project_id', False)
            file = request.files['file']
            sampling = request.form.get('sampling', False)
            options = request.form.get('options', False)

            full_file_name = file.filename
            file_name = os.path.splitext(full_file_name)[0]
            extension = os.path.splitext(full_file_name)[-1].split('.')[-1].lower()

            original_dir, sample_dir, preprocessing_dir, profiling_dir, download_dir = self.set_directory(project_id)

            file_full_path = original_dir + '/' + full_file_name
            if self.check_file_dup(original_dir, full_file_name) and self.check_extension(extension):
                file.save(file_full_path)
            elif not self.check_file_dup(original_dir, full_file_name):
                self.app.logger.error('#### 중복된 파일입니다.')
                return {'status' : 'Fail'}
            elif not self.check_extension(extension):
                self.app.logger.error('#### 허용되지 않은 파일 형식입니다.')
                return {'status' : 'Fail'}

            if extension == 'csv':
                df, encoding_info = self.set_data(file_full_path, file_name, extension, sample_dir, preprocessing_dir, sampling)
            elif extension in ['xlsx', 'xls']:
                df, encoding_info = self.set_data(file_full_path, file_name, extension, sample_dir, preprocessing_dir, sampling, options)
            elif extension == 'json':
                df, encoding_info = self.set_data(file_full_path, file_name, extension, sample_dir, preprocessing_dir, sampling)

            file = {
                'id': uuid.uuid1(),
                'group_id': 'group001',
                'department_id': 'department001',
                'user_id': 'user001',
                'project_id' : project_id,
                'file_id' : file_name,
                'version': round(1, 2),
                'format' : extension,
                'encoding_info' : encoding_info,
                'extra_options' : sampling
            }

            self.hd.insert_file_info_into_database(file)

            return df.to_json(force_ascii = False)
        except Exception as E:
            payload = {
                'project_id' : project_id,
                'file_id' : file_name,
                'format' : extension,
                'version' : 1
            }
            self.delete(payload)

            self.app.logger.error('#### Upload {}/{}.{}/{} Fail'.format(payload['project_id'], payload['file_id'], payload['format'], payload['version']))
            self.app.logger.error(E)

            return {'status' : 'Fail'}

    def upload_db(self, payload):
        try:
            project_id = payload.get('project_id')
            ip = payload.get('ip')
            port = payload.get('port')
            usr = payload.get('user')
            pw = payload.get('password')
            db_nm = payload.get('db_name')
            table = payload.get('table')
            system = payload.get('system')
            sampling = json.dumps(payload.get('sampling'), ensure_ascii = False)

            if system == 'postgres':
                engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(usr, pw, ip, port, db_nm))
                conn = engine.connect()
                df = pd.read_sql_query('select * from {}'.format(table), conn)
            elif system == 'mysql':
                engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(usr, pw, ip, port, db_nm))
                conn = engine.connect()
                df = pd.read_sql_query('select * from `{}`'.format(table), conn)

            # conn = 'postgresql://{}:{}@{}:{}/{}'.format(usr, pw, ip, port, db_nm)
            # engine = create_engine(conn)

            # query = 'select * from {}'.format(table)
            # df = pd.read_sql_query(query, engine)

            original_dir, sample_dir, preprocessing_dir, profiling_dir, download_dir = self.set_directory(project_id)

            now = datetime.datetime.now()
            file_name = table + '_' + str(int(now.timestamp()))
            df.to_json(original_dir + '/' + file_name + '.json', force_ascii = False)
            df.to_json(preprocessing_dir + '/' + file_name + '_V1.00.' + 'json', force_ascii = False)

            sample_df = self.sampling_dataset(df, sampling)
            sample_df.to_json(sample_dir + '/' + file_name + '_V1.00.' + 'json', force_ascii=False)

            file = {
                'id': uuid.uuid1(),
                'group_id': 'group001',
                'department_id': 'department001',
                'user_id': 'user001',
                'project_id': project_id,
                'file_id': table,
                'version': round(1, 2),
                'format': 'database table',
                # 'encoding_info': None,
                'extra_options': sampling
            }

            self.hd.insert_file_info_into_database(file)

            return df.to_json(force_ascii = False)
        except Exception as E:
            payload = {
                'project_id' : project_id,
                'file_id' : file_name,
                'format' : 'json',
                'version' : 1
            }
            self.delete(payload)

            self.app.logger.error('#### Upload {}/{}.{}/{} Fail'.format(payload['project_id'], payload['file_id'], payload['format'], payload['version']))
            self.app.logger.error(E)

            return {'status' : 'Fail'}

    def get_file_list(self, project_id, params):        
        result = self.hd.select_file_list(project_id, params)
        
        file_list = list()

        for file in result:
            file_dict = {
                'id': file['id'],
                'filename': file['file_id'],
                'version': file['version'],
                'created_date': file['created'],
                'modified_date': file['created']
            }
            file_list.append(file_dict)
            
        # return json.dumps(self.hd.get_work_step(params), ensure_ascii = False, default = str)

        # file_path = os.path.join(self.app.root_path, 'server', project_name, 'preprocessing_data', '*')

        # file_name_list = [os.path.split(file)[1] for file in self.read_file_list(file_path)]

        # file_with_ver_mod = []

        # for file in file_name_list:
        #     file_name = file.split('_V')[0]
        #     version = file.split('_V')[-1].split('.json')[0]
        #     modified_date = time.ctime(
        #         os.path.getmtime(os.path.join(self.app.root_path, 'server', project_name, 'preprocessing_data', file)))

        #     file_with_ver_mod.append({
        #             'filename': file_name,
        #             'version': version,
        #             'modified_date': modified_date
        #         }
        #     )

        return {'fileList' : file_list}

    # (처음) 불러오기
    def load(self, payload):
        ds = self.dataset.Dataset(payload)
        # start_time = datetime.datetime.now()

        # try:
        #     result = self.hd.get_file_info_from_db(ds)
        #     print(list(result))
        #     file_info = {
        #         'file_id': result[0],
        #         'project_id': result[1],
        #         'file_format': result[2],
        #         'encoding_info': result[3],
        #         'extra_options': result[4]
        #     }

        #     # if result[4] is not None:
        #     #     file_info['extra_options'] = result[4]

        #     ds.load_dataset_with_file_info(file_info)
        # except:
        #     print('DB에 정보 없음')
        #     ds.load_dataset_from_warehouse_server()

        # self.hd.get_file_info_from_db(ds)

        ds.load_sample_dataset_from_warehouse_server()

        # redo
        ds = self.hd.redo_job_history(ds = ds)
        # print('Run Time : {}'.format(str(datetime.datetime.now() - start_time)))

        return ds.dataset_and_dtypes_to_json()

    # 파일 삭제
    def delete(self, payload):
        ds = self.dataset.Dataset(payload)
        # ds.set_format(payload['format'])

        # original_data = './server/{}/original_data/{}.{}'.format(ds.project_id, ds.file_id, ds.format)
        preprocessing_data = './server/{}/preprocessing_data/{}_V{:.2f}.json'.format(ds.project_id, ds.file_id, ds.version)
        profiling_data = './server/{}/profiling_data/{}_V{:.2f}.json'.format(ds.project_id, ds.file_id, ds.version)
        sample_data = './server/{}/sample_data/{}_V{:.2f}.json'.format(ds.project_id, ds.file_id, ds.version)

        if float(ds.version) == 1:
            # path_list = [original_data, preprocessing_data, profiling_data, sample_data]
            path_list = [preprocessing_data, profiling_data, sample_data]
        else:
            path_list = [preprocessing_data, profiling_data, sample_data]

        for path in path_list:
            if os.path.exists(path):
                os.remove(path)
            else:
                print('############ No File \"{}\"'.format(path))

        return {'status' : 'success'}

    # 데이터 다운로드
    def download(self, params):
        ds = self.dataset.Dataset(params)

        ds.set_format(params['format'])

        ds.load_dataset_from_warehouse_server()

        ds = self.hd.redo_job_history(ds = ds)

        zip_url = ds.download_zip()

        return send_file(zip_url, as_attachment = True)

    # 작업 이력 List 조회
    def get_job_history(self, payload):
        ds = self.dataset.Dataset(payload)

        result = self.hd.get_job_historys(ds)
        # print(type(result))

        # for row in result:
        #     print(type(row))
        #     print(row)
        return json.dumps(result, ensure_ascii = False, default = str)

    def get_work_step(self, params):
        return json.dumps(self.hd.get_work_step(params), ensure_ascii = False, default = str)

    def undo(self, payload):
        redo_undo_vldtr = undo_redo_validator()
        if not redo_undo_vldtr.validate(payload):
            raise TypeError(redo_undo_vldtr.errors)

        ds = self.dataset.Dataset(payload)

        ds.set_job_active('MAX', False)

        self.hd.undo_redo(ds)

        return self.load(payload = payload)

    def redo(self, payload):
        redo_undo_vldtr = undo_redo_validator()
        if not redo_undo_vldtr.validate(payload):
            raise TypeError(redo_undo_vldtr.errors)

        ds = self.dataset.Dataset(payload)

        ds.set_job_active('MIN', True)

        self.hd.undo_redo(ds)

        return self.load(payload = payload)

    def preprocessing_init(self, payload):
        ds = self.dataset.Dataset(payload)

        self.hd.init_job_history(ds)

        return self.load(payload = payload)

    # 가공 처리 동작
    def preprocessing(self, payload, job_id):
        payload_vldtr = preprocessing_validator(job_id)
        if not payload_vldtr.validate(payload):
            raise TypeError(payload_vldtr.errors)

        # col_vldtr = dataset_dtypes_validator(payload)
        # if not col_vldtr:
        #     raise ValueError()

        ds = self.dataset.Dataset(payload)
        ds.load_dataset_from_request(payload)
        ds.set_job_id(job_id)

        # 가공동작 통합 redirect
        ds = self.hd.redirect_preprocess(ds)

        # 사용자 정보 불러오기
        ds.group_id = 'group001'
        ds.department_id = 'department001'
        ds.user_id = 'user001'

        self.hd.insert_job_history_into_database(ds)
        self.hd.delete_inactive_job_history(ds)
        return ds.dataset_and_dtypes_to_json()

    def json_default(self, value):
        print('value', value)

        if isinstance(value, datetime.date):
            return value.strftime('%Y-%m-%d')
        raise TypeError('not JSON serializable')

    # 조회동작 구분 필요
    def show(self, payload, job_id):
        ds = self.dataset.Dataset(payload)
        ds.load_dataset_from_request(payload)
        ds.set_job_id(job_id)

        result = {
            'dataset': ds.dataset_to_json(),
            'dataset_dtypes': ds.get_types()
        }

        if job_id == 'show_duplicate_row':
            result[job_id] = self.hd.show_duplicate_row(ds).to_dict()
        elif job_id == 'show_conditioned_row':
            result[job_id] = self.hd.conditioned_row(ds).dataset.to_dict()
        else:
            raise CustomTypeError('job id가 없습니다.')

        return json.dumps(result, ensure_ascii=False, default=self.json_default)

    # 작업 규칙 저장
    def save_work_step(self, payload):
        try:
            ds = self.dataset.Dataset(payload)

            results = self.hd.get_job_historys(ds)

            self.hd.save_work_step(ds, results)

            return {'status' : 'success'}
        except:
            return {'status' : 'fail'}

    # 작업 규칙 저장
    def delete_work_step(self, payload):
        try:
            self.hd.delete_work_step(payload)

            return {'status' : 'success'}
        except:
            self.app.logger.error('#### Delete Work Step Error')
            self.app.logger.error(traceback.format_exc())
            return {'status' : 'fail'}

    # 데이터셋 추출
    # 프로파일링 추출 기능 추가 위치 예정
    def export(self, payload):
        start_time = datetime.datetime.now()
        ds = self.dataset.Dataset(payload)
        ds.status = 'export'
        ds.load_dataset_from_warehouse_server()

        # redo
        ds = self.hd.redo_job_history(ds = ds)
        ds.export_dataset()
        ds.export_sample()
        ds.export_profiling()

        result = self.hd.get_file_info_from_db(ds)

        file = {
            'id': uuid.uuid1(),
            'group_id': 'group001',
            'department_id': 'department001',
            'user_id': 'user001',
            'project_id': result[0]['project_id'],
            'file_id': result[0]['file_id'],
            'version': float(result[0]['version']) + 0.01,
            'format': result[0]['format'],
            'encoding_info': result[0]['encoding'],
            'extra_options': json.dumps(result[0]['extra'], ensure_ascii = False)            
        }

        self.hd.insert_file_info_into_database(file)

        self.app.logger.info('추출 완료\n런타임 : {}'.format(str(datetime.datetime.now() - start_time)))

        return '추출 완료\n런타임 : {}'.format(str(datetime.datetime.now() - start_time))

    # 임시 위치 data options setting(update)
    def set_data_options(self, payload):
        return self.hd.set_data_options(payload)

    # 데이터셋 이름 변경
    def rename_file(self, payload):
        ds = self.dataset.Dataset(payload)
        rename_id = payload.get('rename_id')
        file_format = payload.get('file_format')

        preprocessing_path = './server/{}/preprocessing_data/{}_V{:.2f}.json'.format(ds.project_id, ds.file_id, ds.version)

        if os.path.exists(preprocessing_path):
            original_path = './server/{}/original_data/{}.{}'.format(ds.project_id, ds.file_id, file_format)
            rename_path = './server/{}/original_data/{}.{}'.format(ds.project_id, rename_id, file_format)
            os.rename(original_path, rename_path)

            if float(ds.version) == 1:
                dir_list = ['preprocessing_data', 'sample_data']
                for target_dir in dir_list:
                    target_path = './server/{}/{}/{}_V{:.2f}.json'.format(ds.project_id, target_dir, ds.file_id, ds.version)
                    rename_path = './server/{}/{}/{}_V{:.2f}.json'.format(ds.project_id, target_dir, rename_id, ds.version)
                    os.rename(target_path, rename_path)

                self.hd.jhDAO.rename_job_history(ds.project_id, ds.file_id, ds.version, rename_id)
                self.hd.fileDAO.rename_file_info(ds.project_id, ds.file_id, ds.version, rename_id)
                # self.hd.jhDAO.rename_job_history(ds.id, ds.rename_id)
                # self.hd.fileDAO.rename_file_info(ds.id, ds.rename_id)
            else:
                dir_list = ['preprocessing_data', 'profiling_data', 'sample_data']
                for target_dir in dir_list:
                    target_path = './server/{}/{}/{}_V{:.2f}.json'.format(ds.project_id, target_dir, ds.file_id, ds.version)
                    rename_path = './server/{}/{}/{}_V{:.2f}.json'.format(ds.project_id, target_dir, rename_id, ds.version)
                    os.rename(target_path, rename_path)

                self.hd.jhDAO.rename_job_history(ds.project_id, ds.file_id, ds.version, rename_id)
                self.hd.fileDAO.rename_file_info(ds.project_id, ds.file_id, ds.version, rename_id)
                # self.hd.jhDAO.rename_job_history(ds.id, ds.rename_id)
                # self.hd.fileDAO.rename_file_info(ds.id, ds.rename_id)
        else:
            print('######## No File \"{}\"'.format(preprocessing_path))
            return {'status' : 'fail'}

        return {'status' : 'success'}

    # 데이터셋 추출 to DB
    def download_db(self, payload):
        start_time = datetime.datetime.now()

        ds = self.dataset.Dataset(payload)
        ds.load_dataset_from_warehouse_server()

        ds = self.hd.redo_job_history(ds = ds)

        ip = payload.get('ip')
        port = payload.get('port')
        usr = payload.get('user')
        pw = payload.get('password')
        db_nm = payload.get('db_name')
        system = payload.get('system')

        if system == 'postgres':
            # conn = 'postgresql://{}:{}@{}:{}/{}'.format(usr, pw, ip, port, db_nm)
            # engine = create_engine(conn)
            # table_list = pd.read_sql_query("select tablename from pg_catalog.pg_tables where tableowner = '{}'".format(usr), engine)
            # result = pd.read_sql_query('select tablename from pg_catalog.pg_tables', engine_conn)
            engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(usr, pw, ip, port, db_nm))
            conn = engine.connect()
            result = pd.read_sql_query('select tablename from pg_catalog.pg_tables', conn)
            table_list = result['tablename'].tolist()
        elif system == 'mysql':
            engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(usr, pw, ip, port, db_nm))
            conn = engine.connect()
            result = pd.read_sql_query('show tables;', conn)
            table_list = result['Tables_in_{}'.format(db_nm)].tolist()

        table_name = '{}_{}_{}'.format(ds.project_id, ds.file_id, ds.version)

        if table_name in table_list:
            self.app.logger.info('이미 존재하는 테이블입니다.')
            raise CustomUserError(500, '이미 존재하는 테이블입니다.', 'TABLE_EXISTS')
        else:
            if len([x for x in ds.dataset.columns.tolist() if ' ' in x]) > 0:
            # True in [True if ' ' in x else False for x in ds.dataset.columns.tolist()]:
                incorrect_col_list = str([x for x in ds.dataset.columns.tolist() if ' ' in x])
                self.app.logger.info('{} 컬럼명에 공백을 포함할 수 없습니다.'.format(incorrect_col_list))
                raise CustomUserError(500, '{} 컬럼명에 공백을 포함할 수 없습니다.'.format(incorrect_col_list), 'INCORRECT_COLUMN_NAME')
            else:
                ds.dataset.to_sql(table_name, engine, if_exists = 'replace', index = False)

        conn.close()

        self.app.logger.info('DB 추출 완료\n런타임 : {}'.format(str(datetime.datetime.now() - start_time)))

        return 'DB 추출 완료\n런타임 : {}'.format(str(datetime.datetime.now() - start_time))
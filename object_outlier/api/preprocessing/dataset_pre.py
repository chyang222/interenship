import json

import numpy as np
import pandas as pd

import zipfile

import os

class Dataset:

    def __init__(self):
        pass

    # 선언 할 때 초기화 -> 기본 parameter 등록
    def __init__(self, content):
        self.project_id = content['project_id']
        self.file_id = content['file_id']
        self.version = float(content['version'])
        self.format = None
        self.dataset = None
        self.data_types = None
        self.profiling_regex = None
        self.job_id = None
        self.status = 'preprocessing'
        self.error_code = None
        if 'content' in content:
            self.job_content = content['content']
            if 'content_of_load_dataset' in dict(self.job_content):
                self.content_of_load_dataset = self.job_content['content_of_load_dataset']

    ###############################################
    # init
    def load_dataset_from_warehouse_server(self):

        # 테스트용 코드
        project_dir = './server/{}/preprocessing_data/'.format(self.project_id)
        full_file_name = '{}_V{:.2f}.json'.format(self.file_id, self.version)

        url = project_dir + full_file_name

        self.dataset = pd.read_json(url).replace('', np.NAN).replace('NaT', np.NAN)
        self.dataset.convert_dtypes()
        self.data_types = self.get_types()

        # if self.status == 'preprocessing':
        #     self = self.sampling_dataset()
        ##################
        # 개발 서버에서 실제 코드
        # 데이터를 보관한 서버에서 데이터셋을 가져와야함

        return self
    
    # 샘플데이터
    def load_sample_dataset_from_warehouse_server(self):
        # 테스트용 코드
        project_dir = './server/{}/sample_data/'.format(self.project_id)
        full_file_name = '{}_V{:.2f}.json'.format(self.file_id, self.version)

        url = project_dir + full_file_name

        self.dataset = pd.read_json(url).replace('', np.NAN).replace('NaT', np.NAN)
        self.dataset.convert_dtypes()
        self.data_types = self.get_types()
        
        ##################
        # 개발 서버에서 실제 코드
        # 데이터를 보관한 서버에서 데이터셋을 가져와야함

        return self

    def load_dataset_with_file_info(self, info):
        extra_options = info['extra_options']

        # 테스트용 프로젝트 내 server 폴더
        project_dir = './server/{}/preprocessing_data/'.format(info['project_id'])
        full_file_name = '{}.{}'.format(info['file_id'], info['file_format'])
        url = project_dir + full_file_name

        print(url)

        if info['file_format'] == 'json':
            self.dataset = pd.read_json(url)
        elif info['file_format'] == 'csv':
            if 'sep' in extra_options:
                self.dataset = pd.read_csv(url, sep=extra_options['sep'])
            else:
                self.dataset = pd.read_csv(url)
        elif info['file_format'] == 'xlsx':
            if 'sheet_name' in extra_options:
                self.dataset = pd.read_excel(url, sheet_name=extra_options['sheet_name'])
            else:
                self.dataset = pd.read_excel(url)
        else:
            # Error Occurred
            # 해당 클래스에 에러메시지 추가하여 return 할까????
            print('noFile')
            return

        # self.dataset = pd.read_json(url)
        self.dataset.convert_dtypes()
        self.data_types = self.get_types()

        # if self.status == 'preprocessing':
        #     self = self.sampling_dataset()
        ##################
        # 개발 서버에서 실제 코드
        # 데이터를 보관한 서버에서 데이터셋을 가져와야함

        return self

    def load_dataset_from_request(self, payload):
        # json에서 load
        # dataset_type도 객체 내 저장
        self.dataset = pd.read_json(payload['dataset']).replace('None', None).replace('Nan', np.NaN).replace('True', True).replace('TRUE', False).replace('False', False).replace('FALSE', False)
        self.data_types = payload['dataset_dtypes']
        # columns = [k for k in self.data_types.keys()]
        # types = [v for v in self.data_types.values()]
        # self.dataset = self.dataset[columns]
        self.dataset.columns = list(map(lambda x: str(x), self.dataset.columns))
        self.dataset = self.dataset.astype(self.data_types)
        return self

    ###############################################

    # 데이터셋 추출
    def export_dataset(self):
        url = './server/{}/preprocessing_data/{}_V{:.2f}.json'.format(self.project_id, self.file_id, self.version + 0.01)
        self.dataset.to_json(url, force_ascii = False, indent = 4)
        
        return url
            
    def export_sample(self):
        url = './server/{}/sample_data/{}_V{:.2f}.json'.format(self.project_id, self.file_id, self.version + 0.01)
        
        self.dataset = self.dataset.loc[:499, :]
        
        self.dataset.to_json(url, force_ascii = False, indent = 4)
        
        return url

    # 프로파일링 추출
    def export_profiling(self):
        df = self.dataset
        
        # 데이터셋의 컬럼 조회
        column_list = df.columns
        
                # 데이터셋 전체 통계
        profile_dict = {}
        profile_dict['number_of_variables'] = len(column_list)
        profile_dict['number_of_observations'] = len(df)
        profile_dict['missing_rows'] = int((df.isnull().sum(axis = 1) > 0).sum())
        profile_dict['missing_rows(%)'] = '{}%'.format(round(profile_dict['missing_rows'] / profile_dict['number_of_observations'] * 100, 1))
        profile_dict['missing_cells'] = int(df.isna().sum().sum())
        profile_dict['missing_cells(%)'] = '{}%'.format(round(profile_dict['missing_cells'] / (profile_dict['number_of_variables'] * profile_dict['number_of_observations']) * 100, 1))
        profile_dict['duplicate_rows'] = int(df.duplicated().sum())
        profile_dict['duplicate_rows(%)'] = '{}%'.format(round(profile_dict['duplicate_rows'] / profile_dict['number_of_observations'] * 100, 1))
        profile_dict['categorical'] = len([val for key, val in self.get_types().items() if val == 'object'])
        profile_dict['numeric'] = len([val for key, val in self.get_types().items() if val == 'int64' or val == 'float64'])
        profile_dict['datetime'] = len([val for key, val in self.get_types().items() if val == 'datetime64[ns]'])
        profile_dict['bool'] = len([val for key, val in self.get_types().items() if val == 'bool'])
        profile_dict['variables'] = {}
        
        # 컬럼별 프로파일링 추가
        for column in column_list:
            profile_dict['variables'].update(json.loads(self.get_profiling_column(column)))
        
        url = './server/{}/profiling_data/{}_V{:.2f}.json'.format(self.project_id, self.file_id, self.version + 0.01)
        if self.status == 'download':
            url = './server/{}/download_data/{}_profile_V{:.2f}.json'.format(self.project_id, self.file_id, self.version)
        
        with open(url, 'w', encoding = 'UTF-8') as file:
            json.dump(profile_dict, file, indent = 4, ensure_ascii = False)
            
        return url
    
    def get_profiling_column(self, column):
        df = self.dataset
        
        if len(df[column].dropna()) <= 0:
            return json.dumps({column : {}}, ensure_ascii = False)
        
        # 컬럼의 데이터타입 조회
        datatype = str(df[column].dtype)
                
        column_profile = {}
        column_profile['datatype'] = datatype
        column_profile['count'] = len(df)
        column_profile['distinct'] = len(df[column].dropna().unique())
        column_profile['distinct(%)'] = '{}%'.format(round(column_profile['distinct'] / column_profile['count'] * 100, 1))
        column_profile['missing'] = int(df[column].isnull().sum())
        column_profile['missing(%)'] = '{}%'.format(round(column_profile['missing'] / column_profile['count'] * 100, 1))
        column_profile['unique'] = len(df[column].drop_duplicates(False))
        column_profile['is_unique'] = column_profile['distinct'] == column_profile['unique']
        
        # 데이터타입 별 통계값 적용
        if datatype == 'object':
            pass
        #     column_profile['unique'] = len(df[column].drop_duplicates(False))
        #     column_profile['is_unique'] = column_profile['distinct'] == column_profile['unique']
        elif datatype== 'bool':
            pass
        elif datatype == 'datetime64[ns]':
            column_profile['minimum'] = str(min(df[column].dropna()))
            column_profile['maximum'] = str(max(df[column].dropna()))
        elif datatype == 'int64' or datatype == 'float64':
            column_profile['infinite'] = int(np.isinf(df[column]).sum())
            column_profile['infinite(%)'] = '{}%'.format(round(column_profile['infinite'] / column_profile['count'] * 100, 1))
            column_profile['mean'] = float(df[column].dropna().mean())
            column_profile['minimum'] = min(df[column].dropna())
            column_profile['maximum'] = max(df[column].dropna())
            column_profile['range'] = column_profile['maximum'] - column_profile['minimum']
            column_profile['sum'] = sum(df[column].fillna(0))
            column_profile['zeros'] = len(df[df[column] == 0])
            column_profile['zeros(%)'] = '{}%'.format(round(column_profile['zeros'] / column_profile['count'] * 100, 1))
            column_profile['negative'] = len(df[df[column] < 0])
            column_profile['negative(%)'] = '{}%'.format(round(column_profile['negative'] / column_profile['count'] * 100, 1))
            column_profile['standard_deviation'] = float(df[column].dropna().std())
            column_profile['variance'] = float(df[column].dropna().var())
            column_profile['skewness'] = float(df[column].dropna().skew())
            column_profile['kurtosis'] = float(df[column].dropna().kurtosis())
            column_profile['5(%)'] = float(np.percentile(df[column].dropna(), 5))
            column_profile['25(%)'] = float(np.percentile(df[column].dropna(), 25))
            column_profile['50(%)'] = float(np.percentile(df[column].dropna(), 50))
            column_profile['75(%)'] = float(np.percentile(df[column].dropna(), 75))
            column_profile['95(%)'] = float(np.percentile(df[column].dropna(), 95))
            column_profile['IQR'] = column_profile['75(%)'] - column_profile['25(%)']
        else:
            print('#### Check Data Type Plz!')
        
        profile_dict = {
            column : column_profile
        }
        
        return json.dumps(profile_dict, ensure_ascii = False)
    
    def create_directory(self, directory):
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print('Error: Failed to create the directory.')

    # 데이터셋 CSV 추출
    def download_dataset_to_csv(self):
        url = './server/{}/download_data/{}_V{:.2f}.csv'.format(self.project_id, self.file_id, self.version)
        self.dataset.to_csv(url, index = False, encoding = 'UTF-8-SIG')
        
        return url
            
    # 데이터셋 JSON 추출
    def download_dataset(self):
        url = './server/{}/download_data/{}_V{:.2f}.json'.format(self.project_id, self.file_id, self.version)
        self.dataset.to_json(url, force_ascii = False, indent = 4)
        
        return url
    
    def zip_csv_profiling(self, csv_url, profiling_url):
        zip_url = './server/{}/download_data/{}_{}_V{:.2f}.zip'.format(self.project_id, self.file_id, self.format, self.version)
        
        my_zip = zipfile.ZipFile(zip_url, 'w')
        
        my_zip.write(csv_url)
        my_zip.write(profiling_url)
        
        my_zip.close()
        
        return zip_url
        
    def download_zip(self):
        self.status = 'download'
        
        self.create_directory('./server/{}/download_data/'.format(self.project_id))
        
        if self.format == 'csv':
            download_url = self.download_dataset_to_csv()
        elif self.format == 'json':
            download_url = self.download_dataset()
        
        profiling_url = self.export_profiling()
        zip_url = self.zip_csv_profiling(download_url, profiling_url)
        
        path_list = [download_url, profiling_url]
        
        for path in path_list:
            if os.path.exists(path):
                os.remove(path)
            else:
                print('######## No File \"{}\"'.format(path))
        
        return zip_url

    def get_types(self):
        data_types = self.dataset.dtypes.to_dict()
        self.data_types = dict()
        for k, v in data_types.items():
            self.data_types[k] = str(v)
        return self.data_types

    def sync_dataset_with_dtypes(self):
        self.data_types = self.get_types()
        self.dataset = self.dataset.astype(self.data_types).replace({'nan': np.nan})
        return self

    def dataset_to_json(self):
        return self.dataset.to_json(force_ascii = False)

    def job_content_to_json(self):
        return json.dumps(self.job_content, ensure_ascii = False)

    def dataset_and_dtypes_to_json(self):
        self.dataset = self.dataset.astype(str)
        return json.dumps({
            'dataset': self.dataset_to_json(),
            'dataset_dtypes': self.data_types,
            'dataset_index': self.dataset.index.values.tolist(),
        }, ensure_ascii=False)

    def set_format(self, format):
        self.format = format
        return self

    def set_job_content(self, job_content):
        self.job_content = job_content
        return self

    def set_job_id(self, job_id):
        self.job_id = job_id
        return self

    def set_job_active(self, min_max, job_active):
        self.min_max = min_max
        self.job_active = job_active
        return self

    # # 0-2. dataset 샘플링
    # def sampling_dataset(self):
    #     sampling_method = 'SEQ'
    #     ord_value = 500
    #     ord_row = 'ROW'
    #     ord_set = 'FRT'

    #     if sampling_method == 'RND':
    #         # Data Frame 셔플 수행
    #         self.dataset = self.dataset.sample(frac = 1).reset_index(drop=True)
    #     sampled_df = pd.DataFrame()
    #     if ord_row == 'ROW':
    #         if ord_set == 'FRT':
    #             sampled_df = self.dataset.iloc[:ord_value]
    #         elif ord_set == 'BCK':
    #             sampled_df = self.dataset.iloc[-ord_value:, :]
    #     elif ord_row == 'PER':
    #         df_len = len(self.dataset)
    #         df_per = int(df_len * ord_value / 100)
    #         if ord_set == 'FRT':
    #             sampled_df = self.dataset.iloc[:df_per, :]
    #         elif ord_set == 'BCK':
    #             sampled_df = self.dataset.iloc[-df_per:, :]

    #     self.dataset = sampled_df
    #     return self

    def dataset_and_dtypes_and_meta_to_json(self):
        self.dataset = self.dataset.astype(str)
        return json.dumps({
            'dataset': self.dataset_to_json(),
            'dataset_dtypes': self.data_types,
            'profiling': self.profiling_regex
        }, ensure_ascii=False)

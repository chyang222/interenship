import json
import os

import numpy as np
import pandas as pd

import os


class Dataset:

    def __int__(self):
        pass

    # 선언 할 때 초기화 -> 기본 parameter 등록
    def __init__(self, content):
        self.project_id = content['project_id']
        self.file_id = content['file_id']
        self.version = float(content['version'])
        self.dataset = None
        self.data_types = None
        self.profiling_regex = None
        if 'content' in content:
            self.job_content = content['content']
            if 'content_of_load_dataset' in dict(self.job_content):
                self.content_of_load_dataset = self.job_content['content_of_load_dataset']
        self.job_id = None
        self.error_code = None
        self.status = ''
        if 'data_format' in content:
            self.data_format = content['data_format']
        else:
            self.data_format = 'json'

    ###############################################
    # init
    def load_dataset_from_warehouse_server(self):

        full_file_name = '{}_V{:.2f}.'.format(self.file_id, self.version)

        url = os.path.join('server', self.project_id, 'preprocessing_data', full_file_name)
        
        df = None
        
        try:
            if self.data_format is None:
                df = pd.read_json(url + 'json').replace('', np.NAN)
            elif self.data_format == 'json':

                df = pd.read_json(url + 'json').replace('', np.NAN)
            elif self.data_format == 'execl':
                print(3)
                df = pd.read_excel(url + 'xlsx').replace('', np.NAN)
            elif self.data_format == 'csv':
                print(4)
                df = pd.read_csv(url + 'csv').replace('', np.NAN)
            else:
                print(5)
                df = pd.read_json(url + 'json').replace('', np.NAN)

        except Exception:
            print(Exception)


        self.dataset = df
        self.dataset.convert_dtypes()
        self.data_types = self.get_types()

        ##################
        # 개발 서버에서 실제 코드
        # 데이터를 보관한 서버에서 데이터셋을 가져와야함

        return self

    ###############################################

    def get_types(self):
        data_types = self.dataset.dtypes.to_dict()
        self.data_types = dict()
        for k, v in data_types.items():
            self.data_types[k] = str(v)
        return self.data_types

    # def sync_dataset_with_dtypes(self):
    #     self.data_types = self.get_types()
    #     self.dataset = self.dataset.astype(self.data_types).replace({'nan': np.nan})
    #     return self

    def dataset_to_json(self):
        return self.dataset.to_json(force_ascii=False)

    def job_content_to_json(self):
        return json.dumps(self.job_content, ensure_ascii=False)

    def dataset_and_dtypes_to_json(self):
        self.dataset = self.dataset.astype(str)
        return json.dumps({
            'dataset': self.dataset_to_json(),
            'dataset_dtypes': self.data_types,
            'dataset_index': self.dataset.index.values.tolist(),
        }, ensure_ascii=False)

    # 0-2. dataset 샘플링
    def sampling_dataset(self):
        sampling_method = 'SEQ'
        ord_value = 500
        ord_row = 'ROW'
        ord_set = 'FRT'

        if sampling_method == 'RND':
            # Data Frame 셔플 수행
            self.dataset = self.dataset.sample(frac=1).reset_index(drop=True)
        sampled_df = pd.DataFrame()
        if ord_row == 'ROW':
            if ord_set == 'FRT':
                sampled_df = self.dataset.iloc[:ord_value]
            elif ord_set == 'BCK':
                sampled_df = self.dataset.iloc[-ord_value:, :]
        elif ord_row == 'PER':
            df_len = len(self.dataset)
            df_per = int(df_len * ord_value / 100)
            if ord_set == 'FRT':
                sampled_df = self.dataset.iloc[:df_per, :]
            elif ord_set == 'BCK':
                sampled_df = self.dataset.iloc[-df_per:, :]

        self.dataset = sampled_df
        return self

    def dataset_and_dtypes_and_meta_to_json(self):
        self.dataset = self.dataset.astype(str)
        return json.dumps({
            'dataset': self.dataset_to_json(),
            'dataset_dtypes': self.data_types,
            'profiling': self.profiling_regex
        }, ensure_ascii=False)

    def sync_dataset_with_dtypes(self):
        self.data_types = self.get_types()
        self.dataset = self.dataset.astype(self.data_types).replace({'nan': np.nan})
        return self
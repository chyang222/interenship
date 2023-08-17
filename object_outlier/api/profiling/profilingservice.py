import json
import math
import os
import random
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from numpyencoder import NumpyEncoder
from flask import session

from .util.validator import profiling_validator

class Profiling:

    def __init__(self, app, pd, hd, dataset):
        self.app = app
        self.pd = pd
        self.hd = hd
        self.dataset = dataset
        # self.hd = handling_dataset.HandlingDataset(app, dsDAO, jhDAO)

    # (정규식)패턴을 통해 컬럼의 특징 알아내기
    def profiling_regex(self, payload):
        ds = self.dataset.Dataset(payload)
        ds.status = 'profiling'
        ds.load_dataset_from_warehouse_server()

        if 'column' in payload:
            return self.pd.profiling_regex_by_column(ds, payload['column'])
        else:
            return self.pd.profiling_regex(ds)

    # (데이터 사전)패턴을 통해 컬럼의 특징 알아내기
    def profiling_data_dict(self, payload):
        ds = self.dataset.Dataset(payload)
        ds.load_dataset_from_request(payload)

        ds = self.pd.profiling_data_dict(ds)

        return ds.dataset_and_dtypes_and_meta_to_json()

    # return df.describe
    def get_dataframe_describe(self, payload):
        ds = self.dataset.Dataset(payload)
        ds.status = 'profiling'

        ds.load_dataset_from_warehouse_server()

        return self.pd.get_dataframe_describe(ds)

    # return profiling describe (json format)
    def get_pandas_profiling_describe(self, payload):
        ds = self.dataset.Dataset(payload)
        ds.status = 'profiling'

        ds.load_dataset_from_warehouse_server()

        if 'column' in payload:
            return self.pd.get_pandas_profiling_describe_by_column(ds, payload['column'])
        else:
            return self.pd.get_pandas_profiling_describe(ds)

    # retrun profiling info (html format)
    def get_pandas_profiling_iframe(self, payload):
        ds = self.dataset.Dataset(payload)
        ds.status = 'profiling'
        ds.load_dataset_from_warehouse_server()

        return self.pd.get_pandas_profiling_iframe(ds)

    # 
    def get_profiling(self, payload):
        payload_vldtr = profiling_validator(payload)
        if not payload_vldtr.validate(payload):
            raise TypeError(payload_vldtr.errors)
        
        ds = self.dataset.Dataset(payload)
        ds.status = 'profiling'
        ds.load_dataset_from_warehouse_server()
        
        ds = self.hd.redo_job_history(ds = ds)

        if 'column' in payload:
            return self.pd.get_profiling_column(ds, payload['column'])
        else:
            return self.pd.get_profiling_dataset(ds)


    # 
    # def get_profiling_dataset(self, payload):
    #     ds = self.dataset.Dataset(payload)
    #     ds.status = 'profiling'
    #     ds.load_dataset_from_warehouse_server()

    #     return self.pd.get_profiling_dataset(ds)

    # 원본 데이터 불러오기 + (예정) 불러온 데이터 profiling
    # 일단 원본 데이터 불러오기만!!
    def get_dataset_and_profiling(self, payload):
        ds = self.dataset.Dataset(payload)
        # DB에서 해당 file의 정보 불러와야함
        # e.g.
        # format : [ xlsx | json | csv | xml ],
        # encoding : [euc-kr | utf-8 | cp949 | ...]
        # csv파일 일 때 {seperator : [ ',', '|'...]}
        # 엑셀일 때 {sheet : n, header : m, ignore_row : i, ignore_col : j}
        # json 일 때 { maintain_empty_str : True/False}
        ds.load_dataset_from_warehouse_server()

        # 불러온 데이터 Profiling
        # 프로파일링 정보(사전/정규식 프로파일링, pandas_profiling, ...) + dataset(default ||sampling || origin) response
        # return ds.dataset.profiling()

    def set_file_options(self, payload):
        # file을 불러올때의 option, 설정 DB내 저장
        # e.g.
        # file_id, project_id, {format, encoding, extra_options{sep, sheet, header, ...}}
        # DB file 정보 테이블 내 update where file_id == file_id and project_id == project_id
        pass

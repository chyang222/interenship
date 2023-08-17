from model.data_ditctionary_dao import DataDictionaryDao as dd
from service.train import main as trainer
from service.inference import main as inference
from config import train_data_path

import os
from datetime import datetime
import shutil
import pandas as pd
import re

class Service:
    def __init__(self, app):
        self.app = app
    
    def train_db(self, body):
        dictionary_info_list = dd.select_dictionary_info()
        dictionary_category_list = dd.select_dictionary_category()
        
        # 파일 저장 경로 (tsv)
        file_path = os.path.join(train_data_path, 'train_data.tsv')
        
        # 파일 중복시에 백업
        Service.bkup_file(file_path)
        
        # DB에 적재된 사전 처리
        for idx, dict_info in enumerate(dictionary_info_list):
            category = dict_info['subject']
            seperator = dict_info['words_split']
            tokens = dict_info['words'].split(seperator)
        
            # 사전 카테고리 매핑
            code = [dictionary_category for dictionary_category in dictionary_category_list if dictionary_category['sub_name'] == category.split('_')[0]][0]['sub_id']
            
            # B, I 태그 세팅
            tags = list(map(lambda x : x + code, ['B-', 'I-'])) + ['O']
            
            df = pd.DataFrame([], columns = ['token', 'bio'])
            
            # 사전 인식 개수 제한 옵션
            if 'limit' in body.keys():
                limit = body['limit']
                tokens = tokens[:limit]
            
            # 앞뒤 공백 제거
            tokens = list(map(lambda x : x.strip(), tokens))
            
            # 토큰 문장화
            data = ','.join(tokens)
            
            # 문장 문자화
            token = re.split('', data)[1:-1]

            df['token'] = token

            # I 태그
            df['bio'] = df['token'].apply(Service.get_bio, args = (tags, ))

            # B 태그
            b_list = list(df[df['token'] == seperator].index + 1)

            df.loc[0, 'bio'] = tags[0]
            df.loc[b_list, 'bio'] = tags[0]
            
            # 파일 저장 (tsv 형식으로 구분자 : \t)
            if idx == 0:
                df.to_csv(file_path, sep = '\t', index = False, encoding = 'UTF-8-SIG', header = False)
            else:
                # 문장 구분을 위해 \n 입력
                with open(file_path, 'a') as file:
                    file.write('\n')
                df.to_csv(file_path, sep = '\t', index = False, encoding = 'UTF-8-SIG', header = False, mode = 'a')
        
        # model 정의 없을 시 default 모델(kykim/bert-kor-base)로 학습
        if 'model' in body.keys():
            pretrained_model_name = body['model']
            return Service.train(file_path, pretrained_model_name)
        else:
            return Service.train(file_path, 'kykim/bert-kor-base')
    
    def get_bio(char, tags):
        if char.isalnum():  # 영어 or 숫자 판단 -> 맞으면 I, 틀리면 O
            return tags[1]
        else:
            return tags[2]
    
    def train_file(self, train, form):
        train_fn = train.filename

        # 파일 없는 경우
        if train_fn == '':
            raise Exception('The train dataset was not found on this request. If you want to train model please input file and try again.')
        
        # 파일 저장 경로
        file_path = os.path.join(train_data_path, train_fn)
        
        # 파일 중복시에 백업
        Service.bkup_file(file_path)
        
        # 파일 저장
        train.save(file_path)
        
        # model 정의 없을 시 default 모델(kykim/bert-kor-base)로 학습
        if 'model' in form:
            pretrained_model_name = form.get('model')
            return Service.train(file_path, pretrained_model_name)
        else:
            return Service.train(file_path, 'kykim/bert-kor-base')
    
    def bkup_file(file_path):
        # 파일 중복시 bkup 폴더에 ms 단위로 백업
        if os.path.exists(file_path):
            bkup_dir_path = os.path.join(os.path.split(file_path)[0], 'bkup')
            file_name = os.path.split(file_path)[1]
            
            if not os.path.exists(bkup_dir_path):
                os.makedirs(bkup_dir_path)
            
            file_bkup_path = os.path.join(bkup_dir_path, '{}_{}{}'.format(os.path.splitext(file_name)[0], datetime.now(), os.path.splitext(file_name)[1]))
            shutil.move(file_path, file_bkup_path)
    
    def train(file_path, pretrained_model_name):
        trainer(file_path, pretrained_model_name)
        
        return {'result' : 'Success'}
    
    def inference(self, data):
        # 데이터가 없는 경우
        if data == '':
            raise Exception('Data was not found on this request. If you want to inference please input data and try again.')
        
        # 머신러닝 모델 구동
        data = inference(data)
        
        # 결과의 Label만 수집
        label_list = [r['label'].split('-')[1] for r in data]
        
        # Label 중 가장 많이 추론된 Label 저장
        label_max = max(label_list, key = lambda x: label_list.count(x))
        
        # dictionary_category 테이블 수집
        dictionary_category_list = dd.select_dictionary_category()
        
        # Label로 카테고리명 매핑
        result = [category['sub_name'] for category in dictionary_category_list if category['sub_id'] == label_max][0]
        
        return {'result' : result}
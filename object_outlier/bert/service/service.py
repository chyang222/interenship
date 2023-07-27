from model.data_ditctionary_dao import DataDictionaryDao as dd
from service.train import main as trainer
from service.inference import main as inference
from config import train_data_path

import os
from datetime import datetime
import shutil
import pandas as pd
import re
import json

from gensim.models.keyedvectors import KeyedVectors
import unicodedata
from googletrans import Translator


class Service:
    def __init__(self, app):
        self.app = app
    
    def train_db(self, body):
        dictionary_info_list = dd.select_dictionary_info()
        dictionary_category_list = dd.select_dictionary_category()
        
        file_path = os.path.join(train_data_path, 'train_data.tsv')
        
        Service.bkup_file(file_path)
                
        for idx, dict_info in enumerate(dictionary_info_list):
            category = dict_info['subject']
            seperator = dict_info['words_split']
            tokens = dict_info['words'].split(seperator)
        
            code = [dictionary_category for dictionary_category in dictionary_category_list if dictionary_category['sub_name'] == category.split('_')[0]][0]['sub_id']
            
            tags = list(map(lambda x : x + code, ['B-', 'I-'])) + ['O']
            
            df = pd.DataFrame([], columns = ['token', 'bio'])
            
            tokens = list(map(lambda x : x.strip(), tokens))
            
            if 'limit' in body.keys():
                limit = body['limit']
                tokens = tokens[:limit]
            
            data = ','.join(tokens)
            
            token = re.split('', data)[1:-1]

            df['token'] = token

            df['bio'] = df['token'].apply(Service.get_bio, args = (tags, ))

            b_list = list(df[df['token'] == seperator].index + 1)

            df.loc[0, 'bio'] = tags[0]
            df.loc[b_list, 'bio'] = tags[0]
            
            if idx == 0:
                df.to_csv(file_path, sep = '\t', index = False, encoding = 'UTF-8-SIG', header = False)
            else:
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
        
        # 데이터의 경로
        file_path = os.path.join(train_data_path, train_fn)
        
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
    

    #한글, 영어 판별
    def detect_language(text):
        cleaned_text = ''.join(char for char in text if char.isalpha())
        for char in cleaned_text:
            char_name = unicodedata.name(char, 'UNKNOWN')
            if "HANGUL" in char_name:
                return 1
            elif "LATIN" in char_name:
                return 0
        return None

    #한글, 영어 변환
    def translate_word(word, lang):
        translator = Translator()
        if lang == 'eg':
            translated_word = translator.translate(word, src= 'ko')
        elif lang == 'ko':
            translated_word = translator.translate(word, dest='ko')  

        return translated_word.text


    #word2vec모델의 유사성
    def get_word_similarity(word1, word2, model):
        try:
            if pd.isna(word2):
                return -10
            similarity = model.similarity(word1, word2)
            return similarity
        except KeyError:
            return None
    

    def object_outlier(self, data, col):
        
        data_dict = json.loads(data)
        df = pd.DataFrame.from_dict(data_dict)

        #return str(df.city.unique())
        # 데이터가 없는 경우    
        if data == '':
            raise Exception('Data was not found on this request. If you want to inference please input data and try again.')

        
        #칼럼과 데이터의 영어 한국어 구별
        else:
            co_da = [Service.detect_language(col), Service.detect_language(df[col].iloc[0])]
            
            #return str(co_da[0]), str(co_da[1])
            if co_da[1] == 1:
                if co_da[0] != co_da[1]:
                    re_col = Service.translate_word(col, lang='ko') #칼럼이 영어 -> 한국어로 번역해서 한국어 모델

                else:
                    re_col = col
            
                ko_model = KeyedVectors.load_word2vec_format('/home/datanuri/object_outlier/bert/service/word2vec_model/wiki.ko.vec', limit=150000)
                #value_counts의 상위 5개가 모두 10개 이상이면 제일 빈도수 많은 값으로 유사도 측정
                if all(count >= 10 for count in df[col].value_counts()[:5]):
                    compare = df[col].mode().iloc[0]
                    df["similarity"] = df[col].apply(lambda x: Service.get_word_similarity(compare, x, ko_model))
                    Outlier = df[(df['similarity'] <= 0.2) | (pd.isna(df.similarity))]
                    return {"Outlier": Outlier.index.to_list()}
                    
                
                #value_counts의 상위 5개가 모두 10개 이하면 칼럼으로 유사도 측정
                else:
                    #return re_col
                    df["similarity"] = df[col].apply(lambda x: Service.get_word_similarity(re_col, x, ko_model))    
                    Outlier = df[(df['similarity'] <= 0.2) | (pd.isna(df.similarity))]
                    return {"Outlier": Outlier.index.to_list()}
                    
            
            if co_da[1] == 0:
                if co_da[0] != co_da[1]:
                    re_col = Service.translate_word(col, lang='eg') #칼럼이 한국어 -> 영어로 번역해서 한국어 모델
                else:
                    re_col = col
    
                eg_model = KeyedVectors.load_word2vec_format('/home/datanuri/object_outlier/bert/service/word2vec_model/GoogleNews-vectors-negative300.bin.gz', binary=True,limit=100000)

                if all(count >= 10 for count in df[col].value_counts()[:5]):
                    compare = df[col].mode().iloc[0]
                    df["similarity"] = df[col].apply(lambda x: Service.get_word_similarity(compare, x, eg_model))
                    #return str(df["similarity"])
                    Outlier = df[(df['similarity'] <= 0.25) | (pd.isna(df.similarity))]

                    return {"Outlier": Outlier.index.to_list()}
        
                else:
                    df["similarity"] = df[col].apply(lambda x: Service.get_word_similarity(re_col, x, eg_model))
                    Outlier = df[(df['similarity'] <= 0.25) | (pd.isna(df.similarity))]
                    
                    return {"Outlier": Outlier.index.to_list()}

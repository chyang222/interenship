import os
import pandas as pd
import math
import re
from logging.config import dictConfig
import logging
import datetime
import traceback
from dateutil.parser import parse
import numpy as np
import sys

#filePath, fileName = os.path.split(__file__)
filePath, fileName = '/home/datanuri/3_ppk_2023/2_데이터_가공', 'proc_raw_datetest.py'
logFolder = os.path.join(filePath , 'logs')
os.makedirs(logFolder, exist_ok = True)
# os.makedirs(os.path.join(filePath, 'Processed_data'), exist_ok = True)
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


def main():
    # try:
    logging.info("START")

    for_mapping_dic_kor = {
        1 : '전기차 충전 이력',
        2 : '전기차 충전 요금 이력',
        3 : '전기차 운행이력',
        4 : '전기차 배터리 상태 이력',
        5 : '전기차 충전기 운영 알람 이력',
        6 : '전기차 충전기 대기 전력량',
        7 : '전기차 배터리 충전 공급 전압/전류',
        8 : '수소버스 운행이력',
        9 : '전기차 충전기 DR 운영 이력',
        10 : '전기버스 노선별 충전이력',
        11 : '전기버스 노선 운영지표',
        12 : '전기버스 노선 굴곡도 대비 에너지 사용량',
        13 : '개방충전소(택시) 사용전력',
        14 : '개방충전소(택시) 사용요금',
        15 : '개방충전소 승용 및 택시 이력 데이터',
        16 : '한전요금제별 충전요금 분석',
        17 : '충전사업자 기준 전기택시 정보 및 차량보유현황',
        18 : '지역별 충전 전력 소모량',
        19 : '기온/강수량과 충전전력사용량 데이터',
        20 : '기온/강수량과 충전전력요금 데이터'
    }

    mapping_date_dic =  {
        1 : 'ELCTC_DTM',
        2 : 'ELCTC_DTM',
        3 : 'ELCTC_DTM',
        4 : 'ELCTC_DTM',
        5 : 'OCRN_DTM',
        6 : 'ELCTC_DTM',
        7 : 'ELCTC_DTM',
        8 : 'RUN_DTM',
        9 : 'EXECUT_YMD',
        10 : 'RUN_YMD',
        11 : 'RUN_YMD',
        12 : 'ELCTC_DTM',
        13 : 'STRT_DTM',
        14 : 'STRT_DTM',
        15 : 'STAT_RWL_DTM',
        16 : 'STRT_DTM',
        18 : 'ELCTC_DTM',
        19 : 'STRT_DTM',
        20 : 'STRT_DTM'
    }
    
    processing_plan_path = os.path.join(filePath, '펌프킨_데이터_정제계획서.xlsx') # 정제계획서 읽기
    processing_plan = make_df(processing_plan_path) # make_df는 정제계획서의 상품명 빈 공백을 채워주는 함수

    # product_list에 경로를 넣어서 읽는 형태
    product_list = []
    
    # product_list.extend(["/nas/PPK/product8/PPK_product8_Y_2022-02-01_2022-02-28.csv"])
    # product_list.extend(read_filelist('/nas/PPK'))
    product_list.extend(read_filelist('/nas/PPK/product1')) # 1분기 완료
    #product_list.extend(read_filelist('/nas/PPK/product2')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product3')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product4')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product5')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product6')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product7')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product8')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product9')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product10')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product11')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product12')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product13')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product14')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product15')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product16')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product17')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product18')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product19')) # 1분기 완료
    # product_list.extend(read_filelist('/nas/PPK/product20')) # 1분기 완료
    logging.info(product_list)

    # 숫자 찾기 정규표현식
    find_number_format = re.compile('[0-9]+')
    for path in product_list:
        product_number = int(find_number_format.search(path.split('/')[3]).group()) # 상품리스트의 경로에서 상품번호 추출
        logging.info(f'Start {product_number}')
        product_processing_plan = processing_plan[processing_plan['상품\n번호'] == product_number] # 정제계획서에서 상품번호가 해당 반복문에서 같은 애들을 추출
        date_str = os.path.splitext(os.path.basename(path))[0].split('_')[-2:] # path를 통하여 날짜 기간 추출
        chunks = pd.read_csv(path, encoding = 'utf-8-sig', chunksize = 1000000) # 청크사이즈 100만으로 조정 후 읽기
                
        pk_index_df = pd.DataFrame([]) # PK 중복용 인덱스 담기 위한 데이터프레임
        for divided_file, sample_df in enumerate(chunks):
            # 컬럼명 조정
            sample_df.columns = [i.upper() for i in sample_df.columns]
            
            # 비고, 생성일자 컬럼 조정
            logging.info(f'{for_mapping_dic_kor[product_number]}|정제전데이터수|{len(sample_df)}')
            sample_df['PRDCT_DTM'] = sample_df['PRDCT_DTM'].apply(lambda x : parse(x).strftime('%Y-%m-%d %H:%M:%S')) # 생성일자 조정
            sample_df['RM'] = sample_df['PRDCT_DTM'].apply(lambda x : note_column_maker(x, for_mapping_dic_kor[product_number])) # 비고 일자 ('%Y-%m-%d') + ' ' + 상품명 해당 형식으로 조정

            # 정제용 데이터프레임 선언
            error_df = pd.DataFrame(columns = ['column', 'processing', 'index_list', 'format_error', 'default'])
            
            # 오류 날짜 체크용 데이터프레임
            date_error_df = pd.DataFrame(columns = ['date', 'index'])
            
            if product_number in mapping_date_dic.keys():
            # 날짜 오류 체크 
                date_error_list = find_outdate(product_processing_plan["상품명(한글)"].iloc[0], sample_df, mapping_date_dic[product_number], date_str) # 날짜를 비교
                date_error_df['index'] = date_error_list # 인덱스 번호와 오류 날짜 데이터프레임 할당
                date_error_df['date'] = sample_df[mapping_date_dic[product_number]].iloc[date_error_list]

            # 날짜 오류 체크 
            if divided_file == 0:
                date_error_df.to_csv(os.path.join('/nas/PPK_date_error/', f'PPK-{product_processing_plan["상품명(영어)"].iloc[0]}{",".join(date_str)}-error_date.csv'), encoding = 'utf-8-sig', index = False) # 첫번째 청크일때는 csv 생성
            else:
                date_error_df.to_csv(os.path.join('/nas/PPK_date_error/', f'PPK-{product_processing_plan["상품명(영어)"].iloc[0]}{",".join(date_str)}-error_date.csv'), encoding = 'utf-8-sig', index = False, header = False, mode = 'a') # 이후 뒤에 더하는 방식

            # 오류유형 탐지
            for error_df_idx, index in enumerate(product_processing_plan.index) :
                상품명, 상품명_영어, 컬럼명, 검증기준값, 길이, 형식, 오류유형, 정제계획, 기본값, 타입 = read_needs(product_processing_plan, index) # 필요한 변수들 엑셀에서 읽어오기
                total_index = []
                try :
                    # NUMBER 중 int 형 변환
                    if (len(str(길이).split(',')) == 1) & (타입 == 'NUMBER') & (re.match('[a-z]+',str(sample_df[컬럼명].dtypes)).group() != 'int'):
                        sample_df[컬럼명] = sample_df[컬럼명].fillna(-99999998)
                        sample_df[컬럼명] = sample_df[컬럼명].astype('int64')
                        sample_df[컬럼명] = sample_df[컬럼명].replace(-99999998, None)
                        logging.info(f"{상품명}|{컬럼명}|integer 형식 변환")


                    for 오류 in 오류유형:
                        if 오류 == 'NULL':
                            total_index.extend(find_null(상품명, sample_df, 컬럼명))

                        elif 오류 == '이상치':
                            total_index.extend(find_outlier(상품명, sample_df, 컬럼명, 검증기준값))

                        elif 오류 == '길이오류':
                            total_index.extend(find_length(상품명, sample_df, 컬럼명, 길이))

                        elif 오류 == '형식오류':
                            format_error_list = find_format(상품명, sample_df, 컬럼명, 형식)
                            total_index.extend(format_error_list)
                            error_df.loc[error_df_idx, 'format_error'] = format_error_list
                        else :
                            pass

                except Exception as E:
                    logging.info(traceback.format_exc())


                total_index = list(dict.fromkeys(total_index))
            
                # 정제용 데이터프레임 생성
                error_df.loc[error_df_idx, ['column']] = 컬럼명
                error_df.loc[error_df_idx, 'processing'] = 정제계획
                error_df.loc[error_df_idx, 'index_list'] = total_index
                error_df.loc[error_df_idx, 'format_error'] = 형식
                error_df.loc[error_df_idx, 'default'] = 기본값
                    
            #### 정제 시작
            
            # 정제(2단계)
            delete_list = []
            for idx, index_list in enumerate(error_df['index_list']) :
                temporary_list = []
                # index_list = list(np.setdiff1d(np.array(index_list),np.array(pk_duplicated_index))) # PK 중복 인덱스와
                index_list = list(np.setdiff1d(np.array(index_list),np.array(delete_list))) # 삭제된 리스트의 차집합을 정제 인덱스로 사용

                for processing_work in error_df.loc[idx, 'processing']:
                    if processing_work == '형식조정시도':
                        suplus_error_list = processing_process(상품명, sample_df, index_list, error_df.loc[idx, 'column'], processing_work, error_df.loc[idx, 'default'], error_df.loc[idx, 'format_error'])
                        temporary_list = suplus_error_list # 형식 조정시도를 마친 데이터는 temporary_list에 넣어준다.(후에 가공에 영향주지 않기 위해 )
                    
                    elif processing_work == '오류유형 해당행 삭제' :
                        index_list = list(np.setdiff1d(np.array(index_list),np.array(temporary_list))) # temporary_list의 차집합을 인덱스 리스트로 받고 출발
                        suplus_error_list = processing_process(상품명, sample_df, index_list, error_df.loc[idx, 'column'], processing_work, error_df.loc[idx, 'default'], error_df.loc[idx, 'format_error'])
                        if isinstance(suplus_error_list, list): 
                            if len(suplus_error_list) > 0:
                                delete_list.extend(suplus_error_list)
                    
                    elif processing_work == '오류유형 해당 데이터 NULL 처리' :
                        index_list = list(np.setdiff1d(np.array(index_list),np.array(temporary_list))) # temporary_list의 차집합을 인덱스 리스트로 받고 출발
                        suplus_error_list = processing_process(상품명, sample_df, index_list, error_df.loc[idx, 'column'], processing_work, error_df.loc[idx, 'default'], error_df.loc[idx, 'format_error'])
                    
                    else :
                        suplus_error_list = processing_process(상품명, sample_df, index_list, error_df.loc[idx, 'column'], processing_work, error_df.loc[idx, 'default'], error_df.loc[idx, 'format_error'])

            # PK check 1 more
            ##### 추가 #######################################
            # PK 중복 인덱스 탐지
            pk_column_list = list(product_processing_plan.loc[product_processing_plan['PK'] == "●", '컬럼명(영어)']) # 정제계획서 내 PK 표시된 컬럼 리스트 추출
            pk_index_df = pd.concat([pk_index_df, sample_df[pk_column_list]], axis = 0) # PK 중복된 컬럼 리스트를 pk_index_df에 합치고
            pk_duplicated_index = pk_find_duplicated(for_mapping_dic_kor[product_number], pk_index_df) # 중복된 항목을 제거하는 방식(청크 합쳐질 때마다 해당 방식 계속 적용)
            pk_index_df.drop(index = pk_duplicated_index, inplace = True)
            
            # PK 정제
            sample_df.drop(pk_duplicated_index, inplace = True)
            if len(pk_duplicated_index) != 0:
                logging.info(f"{상품명}|PK 기준|PK 기준 중복행 삭제|{len(pk_duplicated_index)}")
            ##### 추가 #######################################

            
            sample_df.reset_index(drop = True, inplace = True) # sample_df의 인덱스를 리셋
            logging.info(divided_file)

            if re.search('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]', path.split('_')[-1]) is None:
                date_one = '-' + re.sub('-', '', path.split('/')[-1].split('_')[3]) + '-' + re.sub('-', '', path.split('/')[-1].split('_')[4]) + '-' + re.sub('-', '', path.split('/')[-1].split('_')[5])
            else :
                date_one = '-' + re.sub('-', '', path.split('/')[-1].split('_')[3]) + '-' + re.sub('-', '', path.split('/')[-1].split('_')[4].split('.')[0])

            if divided_file == 0:
                sample_df.to_csv(os.path.join('/nas/PPK_process/', f'PPK-{상품명_영어}{date_one}-N-P.csv'), encoding = 'utf-8-sig', index = False) # 첫번째 청크일때는 csv 생성

            else:
                sample_df.to_csv(os.path.join('/nas/PPK_process/', f'PPK-{상품명_영어}{date_one}-N-P.csv'), encoding = 'utf-8-sig', index = False, header = False, mode = 'a') # 이후 뒤에 더하는 방식
                
            logging.info(f'{for_mapping_dic_kor[product_number]}|정제후데이터수|{len(sample_df)}')
            del sample_df # 메모리를 위해서 sample_df란 변수를 삭제
        logging.info(f"######################################## {product_processing_plan.loc[list(product_processing_plan['상품명(한글)'].index)[0],'상품명(한글)']} Complete")

    del pk_index_df
    # except Exception as E:
    #     logging.error(E)

'''function'''
def read_filelist(path):
    try:
        logging.info('#### Read path {}'.format(path))
        file_list = list([])
        
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                try:
                    file_list.append(os.path.abspath(os.path.join(dirpath, f)))
                except:
                    logging.info('######## Read file error : {}'.format(filenames))
                    logging.info('############ {}'.format(traceback.format_exc()))
                
        return file_list
    except:
        logging.info('#### Read file list error')
        logging.info('######## {}'.format(traceback.format_exc()))

def make_df(path):
    processing_plan = pd.read_excel(path, sheet_name = '데이터상세', index_col = 0, skiprows = [0, 1, 2])
    
    processing_plan.index = range(1, len(processing_plan) + 1)
    
    temporary_str = ''

    for index, product_name in enumerate(processing_plan['상품명(한글)']):
        if pd.isnull(product_name) == False :
            temporary_str = product_name
        processing_plan.loc[index + 1, '상품명(한글)'] = temporary_str

    temporary_str = ''
    for index, product_name in enumerate(processing_plan['상품명(영어)']):
        if pd.isnull(product_name) == False :
            temporary_str = product_name
        processing_plan.loc[index + 1, '상품명(영어)'] = temporary_str

    temporary_str = ''
    for index, product_number in enumerate(processing_plan['상품\n번호']):
        if pd.isnull(product_number) == False :
            temporary_str = product_number
        processing_plan.loc[index + 1, '상품\n번호'] = temporary_str
    
    processing_plan = processing_plan[:len(processing_plan) - 1]
    
    processing_plan['상품\n번호'] = processing_plan['상품\n번호'].astype(int)

    return processing_plan

def read_needs(df, index) :
    try :
        오류유형 = df.loc[index, '오류유형'].split(',')
    except :
        오류유형 = ''
    try :
        정제계획 = df.loc[index, '정제계획'].split(',')
    except :
        정제계획 = ''
    return df.loc[index, '상품명(한글)'], df.loc[index, '상품명(영어)'],  df.loc[index, '컬럼명(영어)'], df.loc[index, '검증 기준값\n(원천데이터 1건(행))'], df.loc[index, '길이'], df.loc[index, '형식'], 오류유형, 정제계획, df.loc[index, '기본값'], df.loc[index, '타입']

def note_column_maker(x, product_name):
    return parse(x).strftime('%Y-%m-%d') + ' ' + product_name

''' Find Error type '''

# 날짜 오류값 찾기
def find_outdate(product_name, df, column_name, date_s):
    exe_df = df.copy()
    exe_df[column_name] = pd.to_datetime(df[column_name]).dt.date 
    date_s = [pd.to_datetime(date).date() for date in date_s]
    indices_without_dates = [index for index, date in enumerate(exe_df[column_name]) if date < date_s[0] or date > date_s[1]]
    if indices_without_dates != 0:
        logging.info(f"{product_name}|{column_name}|date 오류|{len(indices_without_dates)}")

    return indices_without_dates

# 결측치 찾기
def find_null(product_name, df, column_name):
    null_index = list(df.loc[df[column_name].isnull(), column_name].index) # 해당 컬럼에서 NULL값 찾기
    if len(null_index) != 0:
        logging.info(f"{product_name}|{column_name}|Null 오류|{len(null_index)}")
    return null_index

# 중복값 찾기
def pk_find_duplicated(product_name, pk_index_df) :
    pk_dup_index = list(pk_index_df[pk_index_df.duplicated(keep = 'first')].index) # 해당 컬럼에서 중복값 찾기
    if len(pk_dup_index) != 0:
        logging.info(f"{product_name}|PK 기준|PK 기준 중복행 오류|{len(pk_dup_index)}")
    return pk_dup_index

# def find_duplicated(df, column_name):
#     duplicated_index = list(df.loc[df.duplicated(subset=column_name) == True, column_name].index)
#     logging.info(f"Duplicated Error : {len(duplicated_index)}")
#     return duplicated_index

# 이상치 찾기
def find_outlier(product_name, df, column_name, examine_value):
    df = df[df[column_name].notnull()]
    examine_value_list = examine_value.replace(',', '').split(' ')
    if len(examine_value_list) == 5:
        minimum = examine_value_list[0]
        code1 = examine_value_list[1]
        code2 = examine_value_list[3]
        maximum = examine_value_list[4]
        maximum_index = list(eval(f"list(df[(df[column_name]{code2}{maximum}) == False].index)"))
        minimum_index = list(eval(f"list(df[({minimum}{code1}df[column_name]) == False].index)"))
        outlier_index = list(set(minimum_index)|set(maximum_index))
        if len(outlier_index) != 0:
            logging.info(f"{product_name}|{column_name}|이상치 오류|{len(outlier_index)}")
        return outlier_index
    elif len(examine_value_list) == 3:
        if examine_value_list[0]=='n': # n<0 꼴
            maximum = examine_value_list[2]
            code1 = examine_value_list[1]
            maxmum_index = list(eval(f"list(df[(df[column_name]{code1}{maximum}) == False].index)"))
            outlier_index = list(set(maxmum_index))
        elif examine_value_list[-1]=='n': # 0<n 꼴
            minimum = examine_value_list[0]
            code1 = examine_value_list[1]
            minimum_index = list(eval(f"list(df[({minimum}{code1}df[column_name]) == False].index)"))
            outlier_index = list(set(minimum_index))        
        if len(outlier_index) != 0:
            logging.info(f"{product_name}|{column_name}|이상치 오류|{len(outlier_index)}")
        return outlier_index

# 형식 찾기
def find_format(product_name,df, column_name, format):
    df = df[df[column_name].notnull()]
    if format == "2자리":
        format_index = list(df[df[column_name].apply(lambda x: len(str(x)))!=2].index)
        if len(format_index) != 0:
            logging.info(f"{product_name}|{column_name}|형식 오류|{len(format_index)}")
        return format_index
    elif format == "3자리":
        format_index = list(df[df[column_name].apply(lambda x: len(str(x)))!=3].index)
        if len(format_index) != 0:
            logging.info(f"{product_name}|{column_name}|형식 오류|{len(format_index)}")
        return format_index
    elif format == "yyyy-MM-dd HH:mm:ss":
        format_index = list(df[df[column_name].apply(lambda x: is_valid_format_date_1(x))==False].index)
        if len(format_index) != 0:
            logging.info(f"{product_name}|{column_name}|형식 오류|{len(format_index)}")
        return format_index
    elif format == "yyyy-MM-dd":
        format_index = list(df[df[column_name].apply(lambda x: is_valid_format_date_2(x))==False].index)
        if len(format_index) != 0:
            logging.info(f"{product_name}|{column_name}|형식 오류|{len(format_index)}")
        return format_index
    elif format == "HH:mm:ss":
        format_index = list(df[df[column_name].apply(lambda x: is_valid_format_date_3(x))==False].index)
        if len(format_index) != 0:
            logging.info(f"{product_name}|{column_name}|형식 오류|{len(format_index)}")
        return format_index
    elif format == "Y,N":
        format_index = list(df[df[column_name].apply(lambda x: x not in ['Y','N'])].index)
        if len(format_index) != 0:
            logging.info(f"{product_name}|{column_name}|형식 오류|{len(format_index)}")
        return format_index
    else :
        format_index = []
        if len(format_index) != 0:
            logging.info(f"{product_name}|{column_name}|형식 오류|{len(format_index)}")
        return format_index

def is_valid_format_date_1(dateformat) :

    regex = r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$'
    try :
        result_bool = bool(re.match(regex, dateformat))
        return  result_bool
    except :
        pass

def is_valid_format_date_2(dateformat) :
    regex = r'\d{4}-\d{2}-\d{2}$'
    return  bool(re.match(regex, dateformat))

def is_valid_format_date_3(dateformat) :
    regex = r'\d{2}:\d{2}:\d{2}$'
    return  bool(re.match(regex, dateformat))

# 길이 오류 찾기
def find_length(product_name, df, column_name, length):
    df = df[df[column_name].notnull()]
    if ',' not in str(length):
        length_index=list(df[df[column_name].apply(lambda x: len(str(x)))>int(length)].index)
    elif ',' in str(length):
        front_length, end_length=str(length).split(',')

        # 정수 필터링
        frontlength_index=list(df[df[column_name].apply(lambda x: len(str(math.floor(x))))>int(front_length)].index)

        # 소수점 필터링
        try:
            endlength_index=list(df[df[column_name].apply(lambda x: (len(str(x).split('.')[1])))>int(end_length)].index)
        except:
            endlength_index=[]

        length_index = list(dict.fromkeys(frontlength_index + endlength_index))
    if len(length_index) !=0 :
        logging.info(f"{product_name}|{column_name}|길이 오류|{len(length_index)}")
    return length_index

# def remove_processing_list(error_df, error_list):
#     error_df = pd.read_csv('/home/datanuri/2_ppk_2022/2_데이터_가공/error_df.csv')
#     for index, i in error_df['index_list']:
#         error_df[index, 'index_list'] = list(set(list(i)) - set(error_list))
#     return error_df

def format_changer(datetime_str, format_):
    try:
        return parse(datetime_str).strftime(format_)
    except:
        return datetime_str

def try_to_change_format(product_name, df, error_list, column_name, format_):
    if format_ == 'yyyy-MM-dd HH:mm:ss':
        fmt = '%Y-%m-%d %H:%M:%S'
    elif format_ == 'yyyy-MM-dd':
        fmt = '%Y-%m-%d'
    elif format_ == 'HH:mm:ss':
        fmt = '%H:%M:%S'
    elif format_ == '2자리' or format_ == '3자리':
        df2 = df.copy()
        df2.loc[error_list, column_name + '2'] = df2.loc[error_list, column_name].apply(lambda x: int(x))
        df2 = df2[df2[column_name + '2'].notna()]
        df2['equal'] = str(df2[column_name]) == str(df2[column_name + '2'])
        equal_list = list(df2[df2['equal'] == False].index)
        df.loc[error_list, column_name] = df.loc[error_list, column_name].apply(lambda x: int(x))
        if len(equal_list) != 0:
            logging.info(f"{product_name}|{column_name}|형식 변환|{len(equal_list)}")
        return equal_list

    df2 = df.copy()
    df2.loc[error_list, column_name + '2'] = df2.loc[error_list, column_name].apply(lambda x: format_changer(x, fmt))
    df2 = df2[df2[column_name + '2'].notna()]
    df2['equal'] = df2[column_name] == df2[column_name + '2']
    equal_list = list(df2[df2['equal'] == False].index)
    df.loc[error_list, column_name] = df.loc[error_list, column_name].apply(lambda x: format_changer(x, fmt))
    if len(equal_list) != 0:
        logging.info(f"{product_name}|{column_name}|형식 변환|{len(equal_list)}")
    return equal_list

# processing_process(상품명, sample_df, index_list, error_df.loc[idx, 'co'lumn'], processing_work, error_df.loc[idx, 'default'], error_df.loc[idx, 'format_error'])
def processing_process(product_name , df, error_list, column_name, processing_work, pri_key, format_):
    processing_work = processing_work.strip()
    if processing_work == '형식조정시도':
        equal_list = try_to_change_format(product_name, df,error_list, column_name, format_)
        return equal_list

    elif processing_work == '오류유형 해당행 삭제':
        df.drop(error_list, inplace = True)
        if len(error_list) != 0:
            logging.info(f"{product_name}|{column_name}|오류유형 행 삭제|{len(error_list)}")
        return error_list

    elif processing_work[0:3] == '기본값':
        df.loc[error_list, column_name] = pri_key
        if len(error_list) != 0:
            logging.info(f"{product_name}|{column_name}|오류유형 데이터 기본값 변환|{len(error_list)}")
        return error_list

    elif processing_work == '오류유형 해당 데이터 NULL 처리': 
        df.loc[error_list, column_name] = ''
        if len(error_list) != 0:
            logging.info(f"{product_name}|{column_name}|오류유형 데이터 NULL 변환|{len(error_list)}")
        return error_list
    else :
        pass

if __name__=="__main__":
    main()

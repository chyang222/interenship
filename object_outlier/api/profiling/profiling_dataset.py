# import copy
# import json
# import math
# import os
# from datetime import datetime, timedelta
# from sklearn.linear_model import LinearRegression, Lasso, LogisticRegression
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.preprocessing import StandardScaler
# from sklearn.model_selection import train_test_split
# from sklearn.metrics import mean_squared_error
#
import json
import numpy as np

from PyQt5 import QtCore
# from pandas_profiling import ProfileReport

# import numpy as np
#

#
# 출처
# 1. https://domdom.tistory.com/entry/%EC%A0%95%EA%B7%9C%ED%91%9C%ED%98%84%EC%8B%9D-%EC%A0%95%EA%B7%9C%EC%8B%9D%EC%9C%BC%EB%A1%9C-%EB%8D%B0%EC%9D%B4%ED%84%B0%EA%B0%80-%EA%B0%9C%EC%9D%B8%EC%A0%95%EB%B3%B4%EC%9D%B8%EC%A7%80-%EC%95%8C%EC%95%84%EB%82%B4%EB%8A%94-%EB%B0%A9%EB%B2%95
# 2. https://info-lab.tistory.com/292

# 예시 -> DB 호출로 바꿈
validation_dict = {
    "URL": r"http[s]?://(?:[a-zA-Z]|[0-9]|[$\-@\.&+:/?=]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+",
    "날짜(년/월/일)": r"[0-2]\d{3}(\-|\/|\년)( )?[0-1]?\d(\-|\/|\월)( )?[0-3]?\d[\일]?$",
    "날짜(년/월)": r"[0-2]\d{3}(\-|\/|\년)( )?[0-1]?\d(\-|\/|\월)",
    "주소(시군구)": r"[가-힣]*(\시|\군|\구)",
    "e-mail": r"^[a-zA-Z0-9+-_.]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
    "숫자": r"^[0-9]*$",
    "한글": r"^[가-힣]*$",
    "영문": r"^[a-zA-Z]*$",
    "휴대폰": r"^01(?:0|1|[6-9]) - (?:\d{3}|\d{4}) - \d{4}$",
    "주민등록번호": r"\d{6} \- [1-4]\d{6}",
    "영문+숫자": r"^[a-zA-Z0-9]*$",
    "~동": r"[가-힣]*(동)",
    "영문 월(Month)": r"^(january|february|march|april|may|june|july|august|september|october|november|december)",
    "영문 월(Month 약어)": r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|dec|nov)$",
    # 출처1
    "여권번호": r"([a-zA-Z]{1}|[a-zA-Z]{2})\d{8}",
    "외국인등록번호": r"([01][0-9]{5}[[:space:]~-]+[1-8][0-9]{6}|[2-9][0-9]{5}[[:space:]~-]+[1256][0-9]{6})",
    "운전면허번호": r"(\d{2}-\d{2}-\d{6}-\d{2})",
    "이름(한글)": r"^[가-힣]{2,3,4}$",
    "도로명주소": r"(([가-힣A-Za-z·\d~\-\.]{2,}(로|길).[\d]+)|([가-힣A-Za-z·\d~\-\.]+(읍|동)\s)[\d]+)",
    "지번주소": r"(([가-힣A-Za-z·\d~\-\.]+(읍|동)\s)[\d-]+)|(([가-힣A-Za-z·\d~\-\.]+(읍|동)\s)[\d][^시]+)",
    "날짜(yyyy-mm-dd)": r"^([12]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01]))$",
    "날짜(yyyymmdd)": r"^((19|20)\d{2})(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[0-1])",
    "전화번호": r"(\d{2,3}[ ,-]-?\d{2,4}[ ,-]-?\d{4})",
    "계좌번호": r"([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-])",
    "건강보험번호": r"[1257][-~.[:space:]][0-9]{10}",
    "신용카드번호": r"[34569][0-9]{3}[-~.[ ]][0-9]{4}[-~.[ ]][0-9]{4}[-~.[ ]][0-9]{4}",
    "자동차번호1": r"^[가-힣]{2}\\d{2}[가-힣]{1}\\d{4}$",
    "자동차번호2": r"^\\d{2}[가-힣]{1}\\d{4}$",
    "메일주소": r"(([\w!-_\.])*@([\w!-_\.])*\.[\w]{2,3})",
    "IPv4주소": r"(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}",
    "IPv6주소": r"(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))",
    "MAC주소": r"([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}",
    "아이디": r"^[A-Za-z]{1}[A-Za-z0-9]{3,19}$",
    "군번": r"^[0-9a-zA-Z]+([_0-9a-zA-Z]+)*$",
    "사업자등록번호": r"^(\d{3,3})+[-]+(\d{2,2})+[-]+(\d{5,5})",
}


class PandasProfilingThread(QtCore.QThread):
    def __init__(self, df):
        super().__init__()
        self.profile = None
        self.df = df
        self.error = None

    def run(self):
        # Without these lines the same error occurs, adding them solves the issue
        import matplotlib
        import os
        matplotlib.use('Agg')
        # ----------------------------------------------------------------------
        try:
            # Generates
            self.profile = ProfileReport(self.df, title='Summary Report', html={'style': {'full_width': True}})
            return self.profile.to_html()
        except Exception as error:
            self.error = error


class ProfilingDataset:
    def __init__(self, app, dataset, fileDAO, regularExpressionDAO):
        self.app = app
        self.fileDAO = fileDAO
        self.dataset = dataset
        self.regularExpressionDAO = regularExpressionDAO

    def profiling_regex(self, ds):

        # 정규식으로 컬럼의 특징을 추출하는 방법
        # 1. 컬럼명을 통해 추측 (1순위)
        #   1-1. 컬럼명을 정규식 및 사전 정의 값과 같은지 비교를 통해 추측
        #   1-2. 정규식 및 값이 컬럼 명과 일치 할 경우
        #       1-2-1. 값(row값)을 확인하면서 정규식과 일치하는 지 확인,
        #               다른 값을 가진 index 저장 -> UI에서 표시하기 위함
        # 2. 컬럼명으로 추측할 수 없을때 컬럼의 row 값을 확인하여 추측(2순위, 최다 빈도 정규식)

        #
        # 1. 정규 표현식 불러오기 -> regex_dict(dict)?
        # 2. ds의 object, string 타입만 regex 가능 (수치형도 string 변환?)
        #       -> regex 적용 가능한 컬럼 불러오기
        #
        # 방법 1
        # 자료 구조 : 3(n)차원 배열[컬럼 수(index), 정의한 정규 표현식 수 + 1, 정규식에 해당하는 row index]
        # 정의한 정규 표현식 수 + 1 == 아무것도 일치하지 않는 값 index
        #
        # 3. 컬럼의 수 * 컬럼의 로우 수 만큼 반복
        #   3-1. 반복하면서 row 값과 정규식 비교
        #   3-2. 정규식과 일치하면 컬럼의 속성 메타데이터 추가 e.g. 03-02 -> 월-일 + 1
        #       3-2-1. 일치 하지 않는 row의 index 저장 -> UI에 표시하기 위함
        #   3-3. count가 가장 많이 된 정규식 재전송
        #   3-4. 빈도가 가장 높은 정규식과 일치하지 않는 row index 저장?
        #
        # 방법 2 (방법1 보다 나아보임)
        # 자료구조 : 2차원 배열[컬럼 수(index), 정의한 정규 표현 수] = 정규식과 일치하는 row 합계
        # series.str.contains(r'regex').sum() -> 가장 큰 값을 가진 regex 선택
        # series.str.contains(r'regex')
        #
        # 3. 컬럼 * 정규식 regex contain.sum 컬럼 별 비교
        #   3-1. 컬럼 별 가장 빈도 수가 높은 regex 선택
        #   3-2. 선택된 regex의 contain 값 -> index 정보 list화
        #   3-3. list화 된 regex 정보와 해당하는 index response
        #
        # 추가사항 : 비율 정보??     e.g. 년-월-일 83%, 년/월/일 15%, 불일치 2%
        # response 양식 재 수정 필요 or 추가

        # 1. 정규 표현식 불러오기 -> regex_dict(dict)?
        # regex_dict = dict()  # 임시
        regex_dict = self.regularExpressionDAO.get_all_from_regular_expression()  # 임시 2

        # print(regex_dict)

        if len(regex_dict) == 0:
            print("len(regex_dict) == 0")
            return {}
        # 2. ds의 object, string 타입만 regex 가능 (수치형도 string 변환?)
        #       -> regex 적용 가능한 컬럼 불러오기
        # 일단 object, string 타입만 받아오기
        obj_str_columns = list()
        for k, v in ds.data_types.items():
            if v == 'string' or v == 'object':
                obj_str_columns.append(k)

        # regex_df = ds.dataset[obj_str_columns]

        regex_df = ds.dataset[obj_str_columns]

        # 방법 2 (방법1 보다 나아보임)
        # 자료구조 : 2차원 배열[컬럼 수(index), 정의한 정규 표현 수] = 정규식과 일치하는 row 합계
        # or dict in dict
        # { "컬럼명1":{'이메일': n, '전화번호': m,...}, "컬럼명2":{...}, ....}
        # 개수만 취급
        # index는 ??
        # series.str.contains(r'regex').sum() -> 가장 큰 값을 가진 regex 선택
        # series.str.contains(r'regex')
        profile_data_regex = dict()
        profile_data_regex['total_len'] = len(ds.dataset)

        # 3. 컬럼 * 정규식 regex contain.sum 컬럼 별 비교
        for column_name in obj_str_columns:

            reg_result = dict()

            num_of_max = 0
            num_of_max_reg = ''
            outlier_index_of_most_match_reg = []

            for reg_item in regex_dict:
                reg_column = regex_df[column_name].str.contains(reg_item["regexp"], regex=True, na=False)
                result = reg_column.sum()

                if result != 0:
                    reg_result[reg_item["regexp_type"]] = {"num_of_match_row": result}
                    if result > num_of_max:
                        num_of_max = result
                        num_of_max_reg = reg_item["regexp_type"]
                        outlier_index_of_most_match_reg = reg_column.index[reg_column == False].tolist()

            # 가장 많이 정규식과 행이 일치하는 정규식 찾기
            if num_of_max / profile_data_regex['total_len'] > 0.5:
                reg_result["most_match_reg"] = num_of_max_reg
                reg_result["outlier_index_of_most_match_reg"] = outlier_index_of_most_match_reg

            # 3-1. 컬럼 별 가장 빈도 수가 높은 regex 선택
            if reg_result is not None:
                sorted_num_of_regex = sorted(reg_result.items(), reverse=True)
                profile_data_regex[column_name] = reg_result
            else:
                profile_data_regex[column_name] = None

            # ds.profiling_regex = profile_data_regex
            #   3-2. 선택된 regex의 contain 값 -> index 정보 list화
            #   3-3. list화 된 regex 정보와 해당하는 index response
            #
            # 추가사항 : 비율 정보??     e.g. 년-월-일 83%, 년/월/일 15%, 불일치 2%
            # response 양식 재 수정 필요 or 추가

        return json.dumps(profile_data_regex, ensure_ascii=False, default=str)

    def profiling_regex_by_column(self, ds, column):

        # 정규식으로 컬럼의 특징을 추출하는 방법
        # 1. 컬럼명을 통해 추측 (1순위)
        #   1-1. 컬럼명을 정규식 및 사전 정의 값과 같은지 비교를 통해 추측
        #   1-2. 정규식 및 값이 컬럼 명과 일치 할 경우
        #       1-2-1. 값(row값)을 확인하면서 정규식과 일치하는 지 확인,
        #               다른 값을 가진 index 저장 -> UI에서 표시하기 위함
        # 2. 컬럼명으로 추측할 수 없을때 컬럼의 row 값을 확인하여 추측(2순위, 최다 빈도 정규식)

        #
        # 1. 정규 표현식 불러오기 -> regex_dict(dict)?
        # 2. ds의 object, string 타입만 regex 가능 (수치형도 string 변환?)
        #       -> regex 적용 가능한 컬럼 불러오기
        #
        # 방법 1
        # 자료 구조 : 3(n)차원 배열[컬럼 수(index), 정의한 정규 표현식 수 + 1, 정규식에 해당하는 row index]
        # 정의한 정규 표현식 수 + 1 == 아무것도 일치하지 않는 값 index
        #
        # 3. 컬럼의 수 * 컬럼의 로우 수 만큼 반복
        #   3-1. 반복하면서 row 값과 정규식 비교
        #   3-2. 정규식과 일치하면 컬럼의 속성 메타데이터 추가 e.g. 03-02 -> 월-일 + 1
        #       3-2-1. 일치 하지 않는 row의 index 저장 -> UI에 표시하기 위함
        #   3-3. count가 가장 많이 된 정규식 재전송
        #   3-4. 빈도가 가장 높은 정규식과 일치하지 않는 row index 저장?
        #
        # 방법 2 (방법1 보다 나아보임)
        # 자료구조 : 2차원 배열[컬럼 수(index), 정의한 정규 표현 수] = 정규식과 일치하는 row 합계
        # series.str.contains(r'regex').sum() -> 가장 큰 값을 가진 regex 선택
        # series.str.contains(r'regex')
        #
        # 3. 컬럼 * 정규식 regex contain.sum 컬럼 별 비교
        #   3-1. 컬럼 별 가장 빈도 수가 높은 regex 선택
        #   3-2. 선택된 regex의 contain 값 -> index 정보 list화
        #   3-3. list화 된 regex 정보와 해당하는 index response
        #
        # 추가사항 : 비율 정보??     e.g. 년-월-일 83%, 년/월/일 15%, 불일치 2%
        # response 양식 재 수정 필요 or 추가

        # 1. 정규 표현식 불러오기 -> regex_dict(dict)?
        # regex_dict = dict()  # 임시
        regex_dict = self.regularExpressionDAO.get_all_from_regular_expression()

        # print(regex_dict)

        if len(regex_dict) == 0:
            print("len(regex_dict) == 0")
            return {}
        # 2. ds의 object, string 타입만 regex 가능 (수치형도 string 변환?)
        #       -> regex 적용 가능한 컬럼 불러오기
        # 일단 object, string 타입만 받아오기
        obj_str_columns = list()
        for k, v in ds.data_types.items():
            if v == 'string' or v == 'object':
                obj_str_columns.append(k)

        regex_df = ds.dataset[obj_str_columns]

        profile_data_regex = dict()
        profile_data_regex['total_len'] = len(ds.dataset)

        reg_result = dict()

        match_column_list = []

        num_of_max = 0
        num_of_max_reg = ''
        outlier_index_of_most_match_reg = []

        print(column)

        # 3. 컬럼 * 정규식 regex contain.sum 컬럼 별 비교
        for reg_item in regex_dict:

            reg_column = regex_df[column].str.contains(reg_item["regexp"], regex=True, na=False)
            result = reg_column.sum()

            print(reg_item["regexp_type"], result)
            print(reg_item["regexp"])

            # print(regex_df[column_name].str.contains(v, regex=True, na=False))
            if result != 0:
                reg_result[reg_item["regexp_type"]] = {"num_of_match_row": result}
                match_column_list.append(reg_item["regexp_type"])
                if result > num_of_max:
                    num_of_max = result
                    num_of_max_reg = reg_item["regexp_type"]
                    outlier_index_of_most_match_reg = reg_column.index[reg_column == False].tolist()

        # 가장 많이 정규식과 행이 일치하는 정규식 찾기
        if num_of_max / profile_data_regex['total_len'] > 0.5:
            reg_result["most_match_reg"] = num_of_max_reg
            reg_result["outlier_index_of_most_match_reg"] = outlier_index_of_most_match_reg

        if reg_result is not None:
            # sorted_num_of_regex = sorted(reg_result.items(), reverse=True)
            profile_data_regex[column] = reg_result
            profile_data_regex["match_column_list"] = match_column_list
        else:
            profile_data_regex[column] = None

        return json.dumps(profile_data_regex, ensure_ascii=False, default=str)

    def profiling_data_dict(self, ds):

        # 사전데이터를 비교하여 컬럼의 특징 도출
        # 방법
        # data_dict == 사전에 정의한 데이터
        # dict(key:list())
        # key == 분류
        # list() == 비교(in 연산자) 할 사전 데이터

        # 1. data_dict 불러오기
        # data_dict = dict()  # 임시

        file_path = './data_dict.json'
        with open(file_path, encoding='utf8') as read_file:
            data_dict = json.load(read_file)

        # 2. ds의 object, string 타입만 regex 가능 (수치형도 string 변환?)
        #       -> regex 적용 가능한 컬럼 불러오기
        # 일단 object, string 타입만 받아오기
        obj_str_columns = list()
        for k, v in ds.data_types.values:
            if v == 'string' or v == 'object':
                obj_str_columns.append(k)
        compare_df = ds.dataset[obj_str_columns]

        # 추출한 정보를 저장할 자료 구조
        # (dict with list value) in dict
        # {'컬럼명':{'분류 명':[일치 하는 index]}}
        profile_data_dict = dict()

        # 3. 컬럼 * 사전 데이터 수, 사전 데이터와 컬럼 별 비교
        for column_name in obj_str_columns:

            num_of_dict_data = dict()

            for k, v in data_dict.items():
                num_of_dict_data[k] = compare_df.index[compare_df[column_name].isin(v)].tolist()

            if num_of_dict_data:
                profile_data_dict[column_name] = num_of_dict_data

        ds.profiling_regex = profile_data_dict
        return ds

    def get_dataframe_describe(self, ds):
        return ds.dataset.describe().to_json()

    def get_pandas_profiling_describe(self, ds):
        return json.dumps(ProfileReport(ds.dataset).get_description(), ensure_ascii=False, default=str)
        # return json.dumps(ProfileReport(ds.dataset, minimal=True).get_description(), ensure_ascii=False, default=str)

    def get_pandas_profiling_describe_by_column(self, ds, column):
        # print(type(ds.dataset[column]))
        # print(ds.dataset[column])

        # print(type(ds.dataset))
        # print(ds.dataset)

        return json.dumps(ProfileReport(ds.dataset[column].to_frame(), minimal=True).get_description(),
                          ensure_ascii=False,
                          default=str)

    def get_pandas_profiling_iframe(self, ds):
        return PandasProfilingThread(ds.dataset).run()

    def get_profiling_column(self, ds, column):
        # 컬럼의 데이터타입 조회
        datatype = str(ds.dataset[column].dtype)
        
        if len(ds.dataset[column].dropna()) <= 0:
            return json.dumps({column : {}}, ensure_ascii = False)
                
        column_profile = {}
        column_profile['datatype'] = datatype
        column_profile['count'] = len(ds.dataset)
        column_profile['distinct'] = len(ds.dataset[column].dropna().unique())
        column_profile['distinct_per'] = '{}%'.format(round(column_profile['distinct'] / column_profile['count'] * 100, 1))
        column_profile['missing'] = int(ds.dataset[column].isnull().sum())
        column_profile['missing_per'] = '{}%'.format(round(column_profile['missing'] / column_profile['count'] * 100, 1))
        column_profile['unique'] = len(ds.dataset[column].drop_duplicates(False))
        column_profile['is_unique'] = str(column_profile['distinct'] == column_profile['unique'])
        
        # 데이터타입 별 통계값 적용
        if datatype == 'object':
            pass
        #     column_profile['unique'] = len(ds.dataset[column].drop_duplicates(False))
        #     column_profile['is_unique'] = str(column_profile['distinct'] == column_profile['unique'])
        elif datatype == 'bool':
            pass
        elif datatype == 'datetime64[ns]':
            column_profile['minimum'] = str(min(ds.dataset[column].dropna()))
            column_profile['maximum'] = str(max(ds.dataset[column].dropna()))
        elif datatype == 'int64' or datatype == 'float64':
            column_profile['infinite'] = int(np.isinf(ds.dataset[column].dropna()).sum())
            column_profile['infinite_per'] = '{}%'.format(round(column_profile['infinite'] / column_profile['count'] * 100, 1))
            column_profile['mean'] = round(float(ds.dataset[column].dropna().mean()), 2)
            column_profile['minimum'] = min(ds.dataset[column].dropna())
            column_profile['maximum'] = max(ds.dataset[column].dropna())
            column_profile['range'] = column_profile['maximum'] - column_profile['minimum']
            column_profile['sum'] = sum(ds.dataset[column].fillna(0))
            column_profile['zeros'] = len(ds.dataset[ds.dataset[column] == 0])
            column_profile['zeros_per'] = '{}%'.format(round(column_profile['zeros'] / column_profile['count'] * 100, 1))
            column_profile['negative'] = len(ds.dataset[ds.dataset[column] < 0])
            column_profile['negative_per'] = '{}%'.format(round(column_profile['negative'] / column_profile['count'] * 100, 1))
            column_profile['standard_deviation'] = round(float(ds.dataset[column].dropna().std()), 2)
            column_profile['variance'] = round(float(ds.dataset[column].dropna().var()), 2)
            column_profile['skewness'] = round(float(ds.dataset[column].dropna().skew()), 2)
            column_profile['kurtosis'] = round(float(ds.dataset[column].dropna().kurtosis()), 2)
            column_profile['5(%)'] = round(float(np.percentile(ds.dataset[column].dropna(), 5)), 2)
            column_profile['25(%)'] = round(float(np.percentile(ds.dataset[column].dropna(), 25)), 2)
            column_profile['50(%)'] = round(float(np.percentile(ds.dataset[column].dropna(), 50)), 2)
            column_profile['75(%)'] = round(float(np.percentile(ds.dataset[column].dropna(), 75)), 2)
            column_profile['95(%)'] = round(float(np.percentile(ds.dataset[column].dropna(), 95)), 2)
            column_profile['IQR'] = column_profile['75(%)'] - column_profile['25(%)']
        else:
            print('#### Check Data Type Plz!')
        
        profile_dict = {
            column : column_profile
        }
        
        return json.dumps(profile_dict, ensure_ascii = False)

    def get_profiling_dataset(self, ds):
        # 데이터셋의 컬럼 조회
        column_list = ds.dataset.columns
        
        # 데이터셋 전체 통계
        profile_dict = {}
        profile_dict['number_of_variables'] = len(column_list)
        profile_dict['number_of_observations'] = len(ds.dataset)
        profile_dict['missing_rows'] = int((ds.dataset.isnull().sum(axis = 1) > 0).sum())
        profile_dict['missing_rows_per'] = '{}%'.format(round(profile_dict['missing_rows'] / profile_dict['number_of_observations'] * 100, 1))
        profile_dict['missing_cells'] = int(ds.dataset.isna().sum().sum())
        profile_dict['missing_cells_per'] = '{}%'.format(round(profile_dict['missing_cells'] / (profile_dict['number_of_variables'] * profile_dict['number_of_observations']) * 100, 1))
        profile_dict['duplicate_rows'] = int(ds.dataset.duplicated().sum())
        profile_dict['duplicate_rows_per'] = '{}%'.format(round(profile_dict['duplicate_rows'] / profile_dict['number_of_observations'] * 100, 1))
        profile_dict['categorical'] = len([val for key, val in ds.get_types().items() if val == 'object'])
        profile_dict['numeric'] = len([val for key, val in ds.get_types().items() if val == 'int64' or val == 'float64'])
        profile_dict['datetime'] = len([val for key, val in ds.get_types().items() if val == 'datetime64[ns]'])
        profile_dict['bool'] = len([val for key, val in ds.get_types().items() if val == 'bool'])
        profile_dict['variables'] = {}
        
        # 컬럼별 프로파일링 추가
        for column in column_list:
            profile_dict['variables'].update(json.loads(ProfilingDataset.get_profiling_column(self, ds, column)))
        
        return json.dumps(profile_dict, ensure_ascii = False)
from cerberus import Validator

''' cerberus validator 데이터타입
- string: 문자열
- integer: 정수
- float: 부동소수점 수
- number: 정수 및 부동소수점 수
- boolean: 불리언
- datetime: 날짜와 시간
- date: 날짜
- time: 시간
- dict: 사전(딕셔너리)
- list: 리스트
'''

''' 프로파일링 '''
profiling_column_schema = {
    'project_id' : {'type' : 'string', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['string', 'float'], 'required' : True},
    'column' : {'type' : 'string', 'required' : True}
}

profiling_dataset_schema = {
    'project_id' : {'type' : 'string', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['string', 'float'], 'required' : True}
}

def profiling_validator(payload):
    if 'column' in payload:
        return Validator(profiling_column_schema)
    else:
        return Validator(profiling_dataset_schema)
from cerberus import Validator
import pandas as pd
import numpy as np

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

''' 작업 이력 '''
undo_redo_schema = {
    'project_id' : {'type' : 'string', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True}
}

''' 전처리 '''
# 1. 열 삭제
drop_column_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'columns' : {'type' : 'list', 'required' : True}
        }}
}

# 2. 결측치 처리
missing_value_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['remove', 'mean', 'median', 'ffill', 'bfill', 'input']},
        'columns' : {'type' : 'list', 'required' : True},
        'input' : {'type' : ['integer', 'float', 'string']}
        }}
}

# 3. 컬럼 데이터 타입 변경
set_col_prop_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['STR', 'INT', 'BOOL', 'FLOAT', 'CATEGORY', 'OBJECT']},
        'column' : {'type' : 'string', 'required' : True}
        }}
}

# 4. 컬럼 속성 datetime 설정
set_col_prop_to_datetime_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True},
        'dt_format' : {'type' : 'string', 'required' : True},
        'target_column' : {'type' : 'string', 'required' : False}
        }}
}

# 5. 날짜형 컬럼 분할 년, 월, 일
split_datetime_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True},
        'unit_list' : {'type' : 'list', 'required' : True, 'allowed' : ['year', 'yy', 'month', 'month_name', 'day', 'day_of_week', 'day_name', 'hour', 'minute', 'second']}
        }}
}

# 6. 날짜형 문자열로
dt_to_str_format_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True},
        'dt_format' : {'type' : 'string', 'required' : True},
        'target_column' : {'type' : 'string', 'required' : False}
        }}
}

# 7. 날짜형 처리 (기준일부터 날짜 차이)
diff_datetime_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True},
        'option' : {'type' : 'string', 'required' : True},
        'datetime' : {'type' : 'string', 'required' : True},
        'unit' : {'type' : 'string', 'required' : True}
        }}
}

# 8. 컬럼 순서 변경
change_column_order_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'col_order_list' : {'type' : 'list', 'required' : True}
        }}
}

# 9. XXX 대소문자 변경(케이스 변경)
case_sensitive_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['UPP', 'LOW', 'CAP', 'TIT']},
        'column' : {'type' : 'string', 'required' : True}
        }}
}

# 10. 입력값으로 치환(교체)
replace_by_input_value_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['default', 'regex']},
        'column' : {'type' : 'string', 'required' : True},
        'to_replace' : {'type' : 'string', 'required' : True},
        'input' : {'type' : ['integer', 'float', 'string'], 'required' : True}
        }}
}

# 11. XXX 공백제거
remove_space_front_and_rear_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True}
        }}
}

# 12. XXX 중복 행 제거
drop_duplicate_row_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True},
        'keep' : {'type' : 'string', 'required' : True, 'allowed' : ['first', 'last']}
        }}
}

# 13. 연산(재개발 필요)
calculating_column_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True}
}

# 14. 행 삭제
drop_row_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['INPT', 'INVL', 'NEGA']},
        'column' : {'type' : 'string', 'required' : True},
        'input' : {'type' : ['integer', 'float', 'string'], 'required' : False}
        }}
}

# 15. 컬럼 이름 변경
rename_col_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True},
        'target_column' : {'type' : 'string', 'required' : True}
        }}
}

# 16. 컬럼 분할
split_col_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['SEP', 'LEN']},
        'column' : {'type' : 'string', 'required' : True},
        'input' : {'type' : ['integer', 'float', 'string'], 'required' : False},
        'position' : {'type' : 'integer', 'required' : True},
        'reverse' : {'type' : ['boolean', 'string'], 'required' : False}
        }}
}

# 17. 결측치 처리 머신 러닝 모델 활용
missing_value_model_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['regression', 'classification']},
        'feature_list' : {'type' : 'list', 'required' : True},
        'target_column' : {'type' : 'string', 'required' : False}
        }}
}

# 18. 단위 변환
unit_conversion_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['temperature', 'length', 'weight', 'area', 'volume', 'speed']},
        'column' : {'type' : 'string', 'required' : True},
        'current_unit' : {'type' : 'string', 'required' : True},
        'conversion_unit' : {'type' : 'string', 'required' : True}
        }}
}

# 19. 행열 연결
concat_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'axis' : {'type' : 'integer', 'required' : True, 'allowed' : [0, 1]},
        # 'drop_duplicate_column' : {'type' : 'integer', 'required' : True, 'allowed' : [0, 1]},
        # 'content_of_load_dataset' : {'type' : 'list', 'required' : True, 'schema' : {
        #     'merge_file_id' : {'type' : 'string', 'required' : True},
        #     'version' : {'type' : ['float', 'string'], 'required' : True}
        #     }},
        'content_of_load_dataset' : {'type' : 'list', 'required' : True},
        'join' : {'type' : 'string', 'required' : False, 'allowed' : ['inner', 'outer']}
        }}
}

# 20. 병합
merge_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        # 'options' : {'type' : 'integer', 'required' : True, 'allowed' : [0, 1]},
        # 'content_of_load_dataset' : {'type' : 'list', 'required' : True, 'schema' : {
        #     'file_id' : {'type' : 'string', 'required' : True},
        #     'version' : {'type' : ['float', 'string'], 'required' : True}
        #     }},
        'options' : {'type' : 'string', 'required' : True},
        # 'options' : {'type' : 'string', 'required' : True, 'allowed' : ['both', 'left_right_column', 'left_right_index']},
        'content_of_load_dataset' : {'type' : 'list', 'required' : True},
        'how' : {'type' : 'string', 'required' : True, 'allowed' : ['inner', 'outer']},
        'key' : {'type' : 'string', 'required' : False},
        'left_column' : {'type' : 'string', 'required' : False},
        'right_column' : {'type' : 'string', 'required' : False}
        }}
}

# 21. 작업 규칙 저장
save_work_step_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'work_step_name' : {'type' : 'string', 'required' : True},
        'description' : {'type' : 'string', 'required' : False}
        }}
}

# 22. 작업 규칙 적용
apply_work_step_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'id' : {'type' : 'string', 'required' : True}
        }}
}

# 23. 사용자 필터
conditioned_row = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True},
        'operator' : {'type' : 'string', 'required' : True},
        'value' : {'type' : ['string', 'float', 'integer'], 'required' : True},
        }}
}

# 24. 결측 수식 적용 (확인 필요)
missing_value_calc_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['arihmetic', 'function']},
        'columns' : {'type' : ['string', 'list'], 'required' : True},
        'column1' : {'type' : 'string', 'required' : True},
        'operator' : {'type' : 'string', 'required' : True, 'allowed' : ['add', 'min', 'mul', 'div', 'remainder']},
        'value_type' : {'type' : 'string', 'required' : True, 'allowed' : ['column', 'aggregate', 'constant']},
        'value' : {'type' : 'string', 'required' : True},
        'function' : {'type' : 'string', 'required' : False, 'allowed' : ['max', 'min', 'mean', 'median', 'std', 'var']},
        'input' : {'type' : ['integer', 'float', 'string'], 'required' : True},
        'columns' : {'type' : ['string', 'list'], 'required' : True},
        'target_column' : {'type' : 'string', 'required' : False}
        }}
}

# 25. 논리 반전
reverse_boolean_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True}
        }}
}

# 26. 컬럼 인덱스 지정
set_col_to_index_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True}
        }}
}

# 27. 음수 값 처리
cleansing_negative_value_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['to_zero', 'to_input']},
        'column' : {'type' : 'string', 'required' : True},
        'input' : {'type' : ['integer', 'float', 'string']}
        }}
}

# 28. 소수점 처리
round_value_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['DOWN', 'CEIL', 'HALF', 'TRUNC']},
        'column' : {'type' : 'string', 'required' : True},
        'digits' : {'type' : 'integer', 'required' : True}
        }}
}

# 29. 정렬
sort_col_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['ASC', 'DESC']},
        'column' : {'type' : 'string', 'required' : True}
        }}
}

# 30. 그룹별 집계 
group_by_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'columns' : {'type' : 'list', 'required' : True},
        'target_columns' : {'type' : 'list', 'required' : True},
        'options' : {'type' : 'list', 'required' : True}
        }}
}

# 31. 행/열 전환 
transpose_df_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True}
}

# 32. 필터 
show_filter_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['valid', 'invalid', 'empty']},
        'column' : {'type' : 'string', 'required' : True}
        }}
}

# 33. 추출 - 찾아서 입력값으로 삭제
remove_by_input_value_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['INPT', 'PATTN']},
        'column' : {'type' : 'string', 'required' : True},
        'to_remove' : {'type' : 'string', 'required' : True}
        }}
}

# 34. 컬럼 병합 (확인 필요)
concat_col_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column_name' : {'type' : 'string', 'required' : True},
        'column' : {'type' : 'string', 'required' : True},
        'options' : {'type' : 'string', 'required' : True},
        'target_column' : {'type' : 'string', 'required' : False},
        'sep' : {'type' : 'string', 'required' : False},
        'type' : {'type' : 'string', 'required' : False},
        'input' : {'type' : 'string', 'required' : False}
        }}
}

# 35. 이상처 처리-iqr
outlier_iqr_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True}
        }}
}

# 36. 이상치처리-입력값 (확인 필요)
outlier_inpt_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['VAL', 'PER']},
        'column' : {'type' : 'string', 'required' : True},
        'upper' : {'type' : 'integer'},
        'lower' : {'type' : 'integer'},
        'job_type' : {'type' : 'string', 'required' : True, 'allowed' : ['REMOVE', 'CHANGE']}
        }}
}

# 37. 이상치처리-빈도
outlier_value_counts_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['input', 'mode']},
        'column' : {'type' : 'string', 'required' : True},
        'count' : {'type' : 'integer', 'required' : True},
        'input' : {'required' : False}
        }}
}

# 37. 이상치처리-밀도 - 검토 (확인 필요)
outlier_clustering_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column' : {'type' : 'string', 'required' : True}
        }}
}

# 38. 테이블 편집 - 비어있는 모든 행 삭제 -> 결측치 중복(검토)
missing_value_df_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['remove', 'input', 'show']},
        'input' : {}
        }}
}

# 39. 테이블 편집 - 중복된 모든 행 처리
duplicated_df_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'options' : {'type' : 'string', 'required' : True, 'allowed' : ['remove', 'show']}
        }}
}

# 41. 컬럼 생성
create_new_column_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column_name' : {'type' : 'string', 'required' : True}
        }}
}

# 42. 컬럼 복사
copy_column_schema = {
    'dataset' : {'type' : 'string', 'required' : True},
    'dataset_dtypes' : {'type' : 'dict', 'required' : True},
    'file_id' : {'type' : 'string', 'required' : True},
    'project_id' : {'type' : 'string', 'required' : True},
    'version' : {'type' : ['float', 'string'], 'required' : True},
    'content' : {'type' : 'dict', 'required' : True, 'schema' : {
        'column_name' : {'type' : 'string', 'required' : True},
        'column' : {'type' : 'string', 'required' : True}
        }}
}

def undo_redo_validator():
    return Validator(undo_redo_schema)

def preprocessing_validator(job_id):
    if job_id == 'drop_column':
        return Validator(drop_column_schema)
    elif job_id == 'missing_value':
        return Validator(missing_value_schema)
    elif job_id == 'set_col_prop':
        return Validator(set_col_prop_schema)
    elif job_id == 'set_col_prop_to_datetime':
        return Validator(set_col_prop_to_datetime_schema)
    elif job_id == 'split_datetime':
        return Validator(split_datetime_schema)
    elif job_id == 'dt_to_str_format':
        return Validator(dt_to_str_format_schema)
    elif job_id == 'diff_datetime':
        return Validator(diff_datetime_schema)
    elif job_id == 'change_column_order':
        return Validator(change_column_order_schema)
    elif job_id == 'case_sensitive':
        return Validator(case_sensitive_schema)
    elif job_id == 'replace_by_input_value':
        return Validator(replace_by_input_value_schema)
    elif job_id == 'remove_space_front_and_rear':
        return Validator(remove_space_front_and_rear_schema)
    elif job_id == 'drop_duplicate_row':
        return Validator(drop_duplicate_row_schema)
    elif job_id == 'calculating_column':
        return Validator(calculating_column_schema)
    elif job_id == 'drop_row':
        return Validator(drop_row_schema)
    elif job_id == 'rename_col':
        return Validator(rename_col_schema)
    elif job_id == 'split_col':
        return Validator(split_col_schema)
    elif job_id == 'missing_value_model':
        return Validator(missing_value_model_schema)
    elif job_id == 'unit_conversion':
        return Validator(unit_conversion_schema)
    elif job_id == 'concat':
        return Validator(concat_schema)
    elif job_id == 'merge':
        return Validator(merge_schema)
    elif job_id == 'save_work_step':
        return Validator(save_work_step_schema)
    elif job_id == 'apply_work_step':
        return Validator(apply_work_step_schema)
    elif job_id == 'conditioned_row':
        return Validator(conditioned_row)
    elif job_id == 'missing_value_calc':
        return Validator(missing_value_calc_schema)
    elif job_id == 'reverse_boolean':
        return Validator(reverse_boolean_schema)
    elif job_id == 'set_col_to_index':
        return Validator(set_col_to_index_schema)
    elif job_id == 'cleansing_negative_value':
        return Validator(cleansing_negative_value_schema)
    elif job_id == 'round_value':
        return Validator(round_value_schema)
    elif job_id == 'sort_col':
        return Validator(sort_col_schema)
    elif job_id == 'group_by':
        return Validator(group_by_schema)
    elif job_id == 'transpose_df':
        return Validator(transpose_df_schema)
    elif job_id == 'show_filter':
        return Validator(show_filter_schema)
    elif job_id == 'remove_by_input_value':
        return Validator(remove_by_input_value_schema)
    elif job_id == 'concat_col':
        return Validator(concat_col_schema)
    elif job_id == 'outlier_iqr':
        return Validator(outlier_iqr_schema)
    elif job_id == 'outlier_inpt':
        return Validator(outlier_inpt_schema)
    elif job_id == 'outlier_value_counts':
        return Validator(outlier_value_counts_schema)
    elif job_id == 'outlier_clustering':
        return Validator(outlier_clustering_schema)
    elif job_id == 'missing_value_df':
        return Validator(missing_value_df_schema)
    elif job_id == 'duplicated_df':
        return Validator(duplicated_df_schema)
    elif job_id == 'create_new_column':
        return Validator(create_new_column_schema)
    elif job_id == 'copy_column':
        return Validator(copy_column_schema)

# 데이터셋(header), 데이터타입(key) validator (승리)
def dataset_dtypes_validator(payload):
    result = True
    ds = pd.read_json(payload['dataset']).replace('None', None).replace('Nan', np.NaN)
    ds_columns = list(ds.columns)
    dtypes_columns = list(payload.dataset_dtypes.keys())

    if sorted(ds_columns) != sorted(dtypes_columns):
        result = False

    return result
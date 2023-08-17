from pathlib import Path
from werkzeug.utils import send_from_directory

from flask import request, jsonify

from functools import wraps
import asyncio

# 비동기 처리
def async_action(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapped

def create_preprocessing_endpoints(app, preprocessing_service, bp_preprocessing):
    @app.before_request
    def before_request():
        ps.before_request(request)
        
    @app.after_request
    def after_request(response):
        ps.after_requset(response)
        return response
    
    ps = preprocessing_service
    
    # 파일 업로드 및 데이터셋 등록
    @app.route('/project/file', methods = ['POST'])
    def file_upload():
        return ps.upload_file(request)
    
    # 파일 업로드 및 데이터셋 등록
    @app.route('/project/db', methods = ['POST'])
    def db_upload():
        payload = request.get_json(force = True)
        return ps.upload_db(payload = payload)

    # 데이터 목록 조회
    # @app.route('/project/file/<project_id>', methods = ['GET'])
    # def get_file_list(project_id):
    #     params = request.args.to_dict()
    #     return ps.get_file_list(project_id, params)
    
    import glob
    import os
    import time
    @app.route('/project/file/<project_id>', methods = ['GET'])
    def get_file_list(project_id):
        if request.method == 'GET':
            project_name = project_id

            file_path = os.path.join(app.root_path, 'server', project_name, 'preprocessing_data', '*')

            file_name_list = [os.path.basename(file) for file in glob.glob(file_path)]

            file_with_ver_mod = []

            for file in file_name_list:
                file_name = file.rsplit('_V', maxsplit = 1)[0]
                version = file.split('_V')[-1].split('.json')[0]
                modified_date = time.ctime(
                    os.path.getmtime(
                        os.path.join(app.root_path, 'server', project_name, 'preprocessing_data', file)))

                file_with_ver_mod.append(
                    {
                        'filename': file_name,
                        'version': version,
                        'modified_date': modified_date
                    }
                )

        return {'fileList' : file_with_ver_mod}

    # 파일명 변경
    @app.route('/project/file/rename', methods = ['POST'])
    def rename_file():
        payload = request.get_json(force = True)
        return ps.rename_file(payload = payload)

    # 파일 삭제
    @app.route('/project/file/delete', methods = ['POST'])
    def delete():
        payload = request.get_json(force = True)
        return ps.delete(payload = payload)

    # 파일 다운로드
    @app.route('/project/file/download', methods = ['GET'])
    @async_action
    async def download():
        params = request.args.to_dict()
        return ps.download(params)

    # 데이터셋 to DB 추출
    @app.route('/project/download_db', methods = ['POST', 'GET'])
    @async_action
    async def download_db():
        payload = request.get_json(force = True)
        return ps.download_db(payload = payload)

    # (처음) 불러오기
    @app.route('/project/load', methods = ['POST'])
    def load():
        payload = request.get_json(force = True)
        return ps.load(payload = payload)

    # 데이터셋 추출
    @app.route('/project/export', methods = ['POST', 'GET'])
    def export_project():
        payload = request.get_json(force = True)
        return ps.export(payload = payload)

    # 작업 이력 List 조회
    @bp_preprocessing.route('/jobhistory', methods = ['POST'])
    def get_job_history():
        payload = request.get_json(force = True)
        return ps.get_job_history(payload)

    # 작업 규칙 조회
    @bp_preprocessing.route('/get_work_step', methods = ['GET'])
    def get_work_step():
        params = request.args.to_dict()
        return ps.get_work_step(params)
    
    # 되돌리기
    @bp_preprocessing.route('/undo', methods = ['POST'])
    def undo_job_history():
        payload = request.get_json(force = True)
        return ps.undo(payload)

    # 다시 하기
    @bp_preprocessing.route('/redo', methods = ['POST'])
    def redo_job_history():
        payload = request.get_json(force = True)
        return ps.redo(payload)
    
    # 추출 파일 (임시)
    @app.route('/fileserver/<path:filename>', methods = ['GET'])
    def get_file_from_server(filename):
        file_server_location = './server'
        file_path = Path(file_server_location) / filename

        if file_path.is_file():
            return send_from_directory(file_path.parent, file_path.name, as_attachment=True, environ=request.environ)
        else:
            return 'File not found', 404

    # 파일서버 디렉토리 파일 리스트 (임시)
    @app.route('/fileserver', methods = ['GET'])
    def get_all_files_from_server():
        file_server_location = './server'
        path = Path(file_server_location)
        all_files = [str(file.relative_to(path)) for file in path.glob('**/*') if file.is_file()]
        return jsonify(all_files)

    # 43. 작업 초기화
    @bp_preprocessing.route('/init', methods = ['POST'])
    def preprocessing_init():
        payload = request.get_json(force = True)
        return ps.preprocessing_init(payload = payload)    

    ###############################################################
    # 전처리 동작
    # 1. 열 삭제
    @bp_preprocessing.route('/drop_column', methods = ['POST', 'GET'])
    def drop_column():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'drop_column')

    # 2. 결측치 처리
    @bp_preprocessing.route('/missing_value', methods = ['POST', 'GET'])
    def missing_value():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'missing_value')

    # 3. 컬럼 속성 변경
    @bp_preprocessing.route('/set_col_prop', methods = ['POST'])
    def set_col_prop():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'set_col_prop')

    # 4. 날짜 처리(문자열 -> 날짜형)
    # 선택 열 [datetime] 으로 변환 후 추가(혹은 변경)
    @bp_preprocessing.route('/set_col_prop_to_datetime', methods = ['POST'])
    def set_col_prop_to_datetime():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'set_col_prop_to_datetime')

    # 5. 날짜 처리(분할 하기)
    @bp_preprocessing.route('/datetime/spite_variable', methods = ['POST'])
    def split_variable_datetime():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'split_datetime')

    # 6. 날짜 처리(날짜형 -> 문자열로)
    @bp_preprocessing.route('/datetime/dt_to_str_format', methods = ['POST'])
    def dt_to_str_format():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'dt_to_str_format')

    # 7. 날짜 처리(기준 일로 부터 날짜 차이)
    @bp_preprocessing.route('/datetime/diff_datetime', methods = ['POST'])
    def diff_datetime():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'diff_datetime')

    # 8. 컬럼 순서 변경
    @bp_preprocessing.route('/change_column_order', methods = ['POST'])
    def columns_order_change():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'change_column_order')

    # 9. 대, 소문자 변환
    @bp_preprocessing.route('/col_prop/string/case_sensitive', methods = ['POST'])
    def col_prop_string_change():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'case_sensitive')

    # 10. 치환 - 입력값으로 교체
    @bp_preprocessing.route('/col_prop/string/replace_by_input_value', methods = ['POST'])
    def col_prop_string_search_replace():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'replace_by_input_value')

    # 11. 공백 제거
    @bp_preprocessing.route('/col_prop/string/remove_space_front_and_rear', methods = ['POST'])
    def remove_space():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'remove_space_front_and_rear')

    # 조회 1. 중복 값 확인
    @bp_preprocessing.route('/row_control/show_duplicate_row', methods = ['POST'])
    def row_control_show_duplicate_row():
        payload = request.get_json(force = True)
        return ps.show(payload = payload, job_id = 'show_duplicate_row')

    # 12. 중복 행 삭제
    @bp_preprocessing.route('/row_control/drop_duplicate_row', methods = ['POST'])
    def row_control_duplicate_row():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'drop_duplicate_row')

    # 13. 연산 처리
    @bp_preprocessing.route('/calculating', methods = ['POST', 'GET'])
    def calculating():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'calculating_column')

    # 14. 행 삭제 (지정값 일치, 유효하지 않은 데이터, 음수(수치형만 해당))
    @bp_preprocessing.route('/row_control/drop_row', methods = ['POST', 'GET'])
    def drop_row():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'drop_row')

    # 15. 컬럼 이름 변경
    @bp_preprocessing.route('/rename_col', methods = ['POST', 'GET'])
    def rename_col():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'rename_col')

    # 16. 컬럼 분할 ( 구분자, 컬럼 길이로 분할, 역분할(뒤에서 부터))
    @bp_preprocessing.route('/col_prop/split_col', methods = ['POST', 'GET'])
    def split_col():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'split_col')

    # 17. 결측치 처리 머신 러닝 모델 활용
    @bp_preprocessing.route('/missing_value_model', methods = ['POST', 'GET'])
    def missing_value_model():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'missing_value_model')

    # 18. 단위 변환 ex) kg -> g
    @bp_preprocessing.route('/unit_conversion', methods = ['POST'])
    def unit_conversion():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'unit_conversion')

    # 19. row, column concat(연결) (다중 가능)
    @bp_preprocessing.route('/concat', methods = ['POST'])
    def concat():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'concat')

    # 20. merge(병합) (2개 데이터셋만 가능)
    @bp_preprocessing.route('/merge', methods = ['POST'])
    def merge():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'merge')

    # 조회 2. 수식 비교 조회 ex) 몸무게 > 70 인 row
    @bp_preprocessing.route('/show_conditioned_row', methods = ['POST'])
    def show_conditioned_row():
        payload = request.get_json(force = True)
        return ps.show(payload = payload, job_id = 'show_conditioned_row')

    # 21. 작업 규칙 저장
    @bp_preprocessing.route('/save_work_step', methods = ['POST'])
    def save_work_step():
        payload = request.get_json(force = True)
        return ps.save_work_step(payload = payload)

    # 22. 작업 규칙 적용
    @bp_preprocessing.route('/apply_work_step', methods = ['POST'])
    def apply_work_step():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'apply_work_step')

    # 22-1. 작업 규칙 삭제
    @bp_preprocessing.route('/delete_work_step', methods = ['POST'])
    def delete_work_step():
        payload = request.get_json(force = True)
        return ps.delete_work_step(payload = payload)

    # 23. 조회 2. 수식 비교 조회 후 적용
    @bp_preprocessing.route('/conditioned_row', methods = ['POST'])
    def conditioned_row():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'conditioned_row')

    # 24. 결측 수식 적용
    @bp_preprocessing.route('/missing_value_calc', methods = ['POST'])
    def missing_value_calc():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'missing_value_calc')

    # 25. 논리 반전
    @bp_preprocessing.route('/reverse_boolean', methods = ['POST'])
    def reverse_boolean():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'reverse_boolean')

    # 26. 컬럼을 인덱스로 설정
    @bp_preprocessing.route('/set_col_to_index', methods = ['POST'])
    def set_col_to_index():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'set_col_to_index')

    # 27. 음수 값 처리
    @bp_preprocessing.route('/cleansing_negative_value', methods = ['POST'])
    def cleansing_negative_value():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'cleansing_negative_value')

    # 28. 소수점 처리
    @bp_preprocessing.route('/round_value', methods = ['POST'])
    def round_value():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'round_value')

    # 29. 정렬
    @bp_preprocessing.route('/sort_col', methods = ['POST'])
    def sort_col():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'sort_col')

    # 30. 그룹별 집계
    @bp_preprocessing.route('/group_by', methods = ['POST'])
    def group_by():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'group_by')

    # 31. 행/열 전환
    @bp_preprocessing.route('/transpose_df', methods = ['POST'])
    def transpose_df():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'transpose_df')

    # 32. 필터
    @bp_preprocessing.route('/show_filter', methods = ['POST'])
    def show_filter():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'show_filter')

    # 33. 추출 - 찾아서 삭제
    @bp_preprocessing.route('/col_prop/string/remove_by_input_value', methods = ['POST'])
    def remove_by_input_value():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'remove_by_input_value')

    # 34. 컬럼 병합
    @bp_preprocessing.route('/concat_col', methods = ['POST'])
    def concat_col():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'concat_col')

    # 35. 이상치처리 - iqr
    @bp_preprocessing.route('/outlier_iqr', methods = ['POST'])
    def outlier_iqr():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'outlier_iqr')

    # 36. 이상치처리 - input
    @bp_preprocessing.route('/outlier_inpt', methods = ['POST'])
    def outlier_inpt():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'outlier_inpt')

    # 37. 이상치처리 - 빈도
    @bp_preprocessing.route('/outlier_value_counts', methods = ['POST'])
    def outlier_value_counts():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'outlier_value_counts')

    # 37. 이상치처리 - 밀도
    @bp_preprocessing.route('/outlier_clustering', methods = ['POST'])
    def outlier_clustering():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'outlier_clustering')

    # 38. 테이블 편집 - 비어있는 모든 행 삭제
    @bp_preprocessing.route('/missing_value_df', methods = ['POST'])
    def missing_value_df():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'missing_value_df')

    # 39. 테이블 편집 - 중복된 모든행 삭제
    @bp_preprocessing.route('/duplicated_df', methods = ['POST'])
    def duplicated_df():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'duplicated_df')

    # 41. 컬럼 생성
    @bp_preprocessing.route('/create_new_column', methods = ['POST'])
    def create_new_column():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'create_new_column')

    # 42. 컬럼 복사
    @bp_preprocessing.route('/copy_column', methods = ['POST'])
    def copy_column():
        payload = request.get_json(force = True)
        return ps.preprocessing(payload = payload, job_id = 'copy_column')

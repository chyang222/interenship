import traceback

from .custom_exception import CustomUserError


#
def error_handle(app):
    '''
    Error handler

    - 에러처리

    Args :
        app : __init__.py 에서 파라미터로 app 전달
    Return :
        json : error_response() 함수로 에러 메시지 전달해서 받고 return
    '''

    def error_response(user_error_msg, dev_error_msg, error_code):
        data = {
            'user_error_msg': user_error_msg,
            'dev_error_msg': dev_error_msg,
            'status_code': error_code
        }

        return data, error_code

    @app.errorhandler(Exception)
    def handle_error(e):
        traceback.print_exc()
        return error_response('서버 상에서 오류가 발생했습니다.', 'Internet Server Error', 500)

    @app.errorhandler(AttributeError)
    def handle_error(e):
        traceback.print_exc()
        return error_response('서버 상에서 오류가 발생했습니다.', 'Internet Server Error', 500)

    @app.errorhandler(TypeError)
    def handle_type_error(e):
        traceback.print_exc()
        return error_response('Data Type Error', str(e), 500)

    @app.errorhandler(ValueError)
    def handle_value_error(e):
        traceback.print_exc()
        return error_response('데이터에 잘못된 값이 입력되었습니다.', 'Data Value Error', 500)

    # Custom Excpetion Setting
    @app.errorhandler(CustomUserError)
    def handle_error(e):
        traceback.print_exc()
        return error_response(e.error_message, e.dev_error_message, e.status_code)

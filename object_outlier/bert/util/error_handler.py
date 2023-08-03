import traceback

def error_handler(app):
    @app.errorhandler(400)
    def bad_request(e):
        print(traceback.format_exc())
        return {'code' : 400, 'name' : 'Bad Request', 'message' : 'The browser (or proxy) sent a request that this server could not understand.'}, 400
    
    @app.errorhandler(404)
    def page_not_found(e):
        print(traceback.format_exc())
        return {'code' : 404, 'name' : 'Not Found', 'message' : 'The requested URL was not found on the server. If you entered the URL manually please check your spelling and try again.'}, 404
    
    @app.errorhandler(Exception)
    def exception(e):
        print(traceback.format_exc())
        return {'code' : 502, 'name' : 'Exception', 'message' : str(e)}, 502
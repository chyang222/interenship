from flask import request
from flask_restx import Namespace, Resource

from middleware.middleware import before_request as br, after_requset as ar
from service.service import Service

def middleware(app):
    @app.before_request
    def before_request():
        br(request)
        
    @app.after_request
    def after_request(response):
        ar(response)
        return response

model = Namespace('model', 'Bert Model API')

@model.route('/train/db')
class TrainDB(Resource):
    def __init__(self, app):
        self.app = app

    def post(self):
        return Service.train_db(self.app, request.get_json(force = True)), 202

@model.route('/train/file')
class TrainFile(Resource):
    def __init__(self, app):
        self.app = app

    def post(self):
        return Service.train_file(self.app, request.files['train'], request.form), 202

@model.route('/inference')
class Inference(Resource):
    def __init__(self, app):
        self.app = app
    
    def post(self):
        return Service.inference(self.app, request.get_json(force = True)['data']), 202

outlier = Namespace('outlier', 'Bert Model API')

# 192.168.2.204:5050/outlier/object
# Restful API
# pip install gensim
# pip install googletrans
# Postman

@outlier.route('/object')
class Object(Resource):
    def __init__(self, app):
        self.app = app
    
    def post(self):
        data = request.get_json(force=True)
        return Service.object_outlier(self.app, data['data'], data['column'], data['sensitivity']), 202
    
    #json에 이상치 찾을 칼럼명과 데이터를 함께해서 받을 예정 
    #그리고 input이라는 값에 이상치를 대체
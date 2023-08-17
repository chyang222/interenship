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
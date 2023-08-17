from flask import Flask
from flask_restx import Api

from controller.endpoints import middleware, model
from util.error_handler import error_handler
import config as cfg

app = Flask(__name__)
api = Api(
    app = app,
    version = 0.1,
    title = 'Machine Learning Model API Server',
    description = '전처리툴 Machine Learning 모델 API 서버',
    contact = 'whitedot@datanuri.net',
    license = '(주)데이터누리'
)

# 미들웨어
middleware(app)

# Namespace
api.add_namespace(model, '/model')

# Error Handler
error_handler(app)

if __name__ == "__main__":
    app.run(host = cfg.host, port = cfg.port, debug = True)

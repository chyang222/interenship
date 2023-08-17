import json

from flask import request, jsonify, current_app, Response, render_template, g, session, Blueprint, redirect
from kafka import KafkaConsumer
import msgpack
import pandas as pd
import logging
import os
import pika


# from preprocessing_endpoints import create_preprocessing_endpoints
# from profiling_endpoints import create_profiling_endpoint
# import preprocessing_endpoints
# import profiling_endpoints

def create_endpoints(app, preprocessing, pre_end, profiling, pro_end):
    # blueprint
    bp_preprocessing = Blueprint('preprocessing', __name__, url_prefix='/preprocessing')
    bp_profiling = Blueprint('profiling', __name__, url_prefix='/profiling')

    pre_end.create_preprocessing_endpoints(app, preprocessing_service=preprocessing, bp_preprocessing=bp_preprocessing)
    pro_end.create_profiling_endpoint(app, profiling_service=profiling, bp_profiling=bp_profiling)

    # (처음) 불러오기
    @app.route('/test', methods=['POST'])
    def test():
        return "테스트 입니다."

    @app.route('/set_data_options', methods=['POST'])
    def set_data_options():
        # update
        payload = request.get_json(force=True)
        return preprocessing.set_data_options(payload)

    # create_preprocessing_endpoints(preprocessing_service, bp_preprocessing)
    # create_profiling_endpoint(profiling_service, bp_profiling)

    app.register_blueprint(bp_preprocessing)
    app.register_blueprint(bp_profiling)

    ##################################################################
    ##################################################################
    ##################################################################
    # RabbitMQ 추출 작업 테스트

    ##################################################################
    ##################################################################
    ##################################################################
    # RabbitMQ RPC 패턴
    #
    # HOST_NAME = 'localhost'
    # QUEUE_NAME = 'export_TEST'
    #
    # # RabbitMQ connect and get channel
    # connection = pika.BlockingConnection(
    #     pika.ConnectionParameters(host=HOST_NAME))
    # channel = connection.channel()
    #
    # # declare queue name rpc_queue
    # channel.queue_declare(queue=QUEUE_NAME)
    #
    # # preprocessing and response to client
    # def on_request(ch, method, props, body):
    #     payload = json.loads(body)
    #
    #     print(type(payload))
    #     print(payload)
    #     response = ps.preprocessing(payload=payload, job_id=payload['job_id'])
    #     # response to props.reply_to, props.correlation_id
    #     ch.basic_publish(exchange='',
    #                      routing_key=props.reply_to,
    #                      properties=pika.BasicProperties(correlation_id= \
    #                                                          props.correlation_id),
    #                      body=json.dumps(response, ensure_ascii=False))
    #     ch.basic_ack(delivery_tag=method.delivery_tag)
    #
    # channel.basic_qos(prefetch_count=1)
    #
    # # set callback function == on_request
    # channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_request)
    #
    # # start consuming
    # print(" [x] Awaiting RPC requests")
    # channel.start_consuming()

    ###################################################################
    ###################################################################
    ###################################################################
    # # Kafka setting
    # # consume earliest available messages, don't commit offsets
    # KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)
    #
    # # consume json messages
    # KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    #
    # # consume msgpack
    # KafkaConsumer(value_deserializer=msgpack.unpackb)
    #
    # # StopIteration if no message after 1sec
    # KafkaConsumer(consumer_timeout_ms=1000)
    # def forgiving_json_deserializer(v):
    #     if v is None:
    #         try:
    #             return json.loads(v.decode('utf-8'))
    #         except json.decoder.JSONDecodeError:
    #             app.logger.inlog.exception('Unable to decode: %s', v)
    #             return None
    # # # Kafka Test
    # consumer = KafkaConsumer('test',
    #                      bootstrap_servers=['localhost:9092'],
    #                      auto_offset_reset='latest',
    #                      value_deserializer=forgiving_json_deserializer)
    #
    # for message in consumer:
    #     print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                          message.offset, message.key,
    #                                          message.value))
    #     print(type(message.value))
    #     df = pd.DataFrame(message.value)
    #     print(df.head)
    #     print(message)

    ###################################################################
    ###################################################################
    ###################################################################

from config import engine

from datetime import datetime

class ApiHistoryDao:
    def insert_api_history(api_history):
        sql = "INSERT INTO api_history(id, is_del, group_id, department_id, user_id, ip, method, path, params) VALUES ("
        sql += "'{}',".format(api_history['id'])
        sql += " False,"
        sql += " '{}',".format(api_history['group_id'])
        sql += " '{}',".format(api_history['department_id'])
        sql += " '{}',".format(api_history['user_id'])
        sql += " '{}',".format(api_history['ip'])
        sql += " '{}',".format(api_history['method'])
        sql += " '{}',".format(api_history['path'])
        if 'params' in api_history.keys():
            if '%' in api_history['params']:
                api_history['params'] = api_history['params'].replace('%', '%%')
            sql += " '{}') ;".format(api_history['params'])
        else:
            sql += " NULL) ;"

        # print(sql)
        
        engine.execute(sql)

    def update_api_history(api_history):
        sql = "UPDATE api_history"
        sql += " SET response = {},".format(api_history['response'])
        sql += " updated = '{}'".format(datetime.now())
        sql += " WHERE id = '{}' ;".format(api_history['id'])

        # print(sql)
        
        engine.execute(sql)
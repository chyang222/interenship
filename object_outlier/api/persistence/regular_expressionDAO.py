import json


class RegularExpressionDAO:

    def __init__(self, db, app):
        self.db = db
        # self.app = app
        self.logger = app.logger

    def get_all_from_regular_expression(self):
        sql = "select * from regular_expression"

        result_set = list()
        result = self.db.execute(sql)
        for row in result:
            result_set.append(dict(row))

        return result_set
from datetime import datetime

class FileDao:

    def __init__(self, db, app):
        self.db = db
        self.app = app
        self.logger = app.logger

    def insert_file_info(self, file):
        # sql = "INSERT INTO file(id, project_id, file_id, \"version\", format, encoding, extra, is_del) VALUES ("
        # sql = "INSERT INTO file(id, is_del, project_id, file_id, \"version\", format, encoding, extra) VALUES ("        
        sql = "INSERT INTO file(id, is_del, group_id, department_id, user_id, project_id, file_id, \"version\", format, encoding, extra)"        
        sql += " VALUES ('{}',".format(file['id'])
        sql += " False,"
        sql += " '{}',".format(file['group_id'])
        sql += " '{}',".format(file['department_id'])
        sql += " '{}',".format(file['user_id'])
        sql += " '{}',".format(file['project_id'])
        sql += " '{}',".format(file['file_id'])
        sql += " '{}',".format(round(file['version'], 2))
        sql += " '{}',".format(file['format'])
        if file['format'] != 'database table':
            sql += " '{}',".format(file['encoding_info'])
        else:
            sql += " NULL,"
        if 'extra_options' in file.keys():
            sql += " '{}') ;".format(file['extra_options'])
        else:
            sql += " NULL) ;"

        # sql = "INSERT INTO file(id, project_id, file_id, \"version\", format, encoding, extra) VALUES ("
        # sql += "'{}',".format(file['id'])
        # sql += " '{}',".format(file['project_id'])
        # sql += " '{}',".format(file['file_id'])
        # sql += " '{}',".format(round(file['version'], 2))
        # sql += " '{}',".format(file['format'])
        # sql += " '{}',".format(file['encoding'])
        # sql += " '{}',".format(file['extra'])
        # sql += " False) ;"

        self.app.logger.info(sql)
        
        self.db.execute(sql)

    def delete_file_info(self, project_id, file_id, version):
        sql = "UPDATE file"
        sql += " SET is_del = True,"
        sql += " updated = '{}'".format(datetime.now())
        sql += " WHERE is_del = False"
        sql += " AND project_id = '{}'".format(project_id)
        sql += " AND file_id = '{}'".format(file_id)
        sql += " AND \"version\" = {} ;".format(round(version, 2))
    # def delete_file_info(self, id):
    #     sql = "UPDATE file SET is_del = True"
    #     sql += " WHERE is_del = False"
    #     sql += " AND id = '{}' ;".format(id)
        
        self.app.logger.info(sql)
                
        self.db.execute(sql)

    def select_file_list(self, project_id, params):
        sql = "SELECT * FROM file"
        sql += " WHERE project_id = '{}'".format(project_id)
        sql += " AND is_del = False"
        
        for idx, (key, val) in enumerate(params.items()):                
            if 'before' in key:
                sql += " AND {} >= '{} 00:00:00'".format(key.split('_')[0], val)
            elif 'after' in key:
                sql += " AND {} <= '{} 23:59:59'".format(key.split('_')[0], val)
            elif 'id' in key:
                sql += " AND {} like '%%{}%%'".format(key, val)
        sql += " ;"
        
        self.app.logger.info(sql)
        
        result = self.db.execute(sql)
        result_set = list()
        for row in result:
            result_set.append(dict(row))

        return result_set

    def get_file_info(self, project_id, file_id, version):
        sql = "SELECT * FROM file"
        sql += " WHERE project_id = '{}'".format(project_id)
        sql += " AND file_id = '{}'".format(file_id)
        sql += " AND \"version\" = '{}' ;".format(round(version, 2))
    # def get_file_info(self, id):
    #     sql = "SELECT * FROM file"
    #     sql += " WHERE id = '{}' ;".format(id)
        
        self.app.logger.info(sql)
        
        result = self.db.execute(sql)
        result_set = list()
        for row in result:
            result_set.append(dict(row))

        return result_set

    def update_file_info(self, file):
        sql = "UPEATE file"
        sql += " SET format = '{}',".format(file['format'])
        sql += " encoding = '{}',".format(file['encoding_info'])
        sql += " extra = '{}',".format(file['extra_options'])
        sql += " updated = '{}'".format(datetime.now())
        # sql += " WHERE id = '{}' ;".format(file['id'])
        sql += " WHERE project_id = '{}' ;".format(file['project_id'])
        sql += " AND file_id = '{}'".format(file['file_id'])
        sql += " AND \"version\" = '{}' ;".format(round(file['version'], 2))
        
        self.app.logger.info(sql)
        
        self.db.execute(sql)
        # self.app.logger.info('ERROR update_file_info FAILED!!!')
    
    def rename_file_info(self, project_id, file_id, version, rename_id):
        sql = "UPDATE file"
        sql += " SET file_id = '{}',".format(rename_id)
        sql += " updated = '{}'".format(datetime.now())
        sql += " WHERE project_id = '{}'".format(project_id)
        sql += " AND file_id = '{}'".format(file_id)
        sql += " AND \"version\" = {} ;".format((round(version, 2)))
    # def rename_file_info(self, id, rename_id):
        # sql = "UPDATE file SET file_id = '{}'".format(rename_id)
        # sql += " WHERE id = '{}' ;".format(id)
        
        self.app.logger.info(sql)
        
        self.db.execute(sql)
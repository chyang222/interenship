import json
from datetime import datetime

class JobHistoryDao:

    def __init__(self, db, app):
        self.db = db
        self.app = app
        self.logger = app.logger

    def insert_job_history(self, job_history):
        sql = "INSERT INTO job_history(id, is_del, group_id, department_id, user_id, project_id, file_id, \"version\", job_id, content, active) VALUES ("
        sql += "'{}',".format(job_history['id'])
        sql += " False,"
        sql += "'{}',".format(job_history['group_id'])
        sql += "'{}',".format(job_history['department_id'])
        sql += "'{}',".format(job_history['user_id'])
        sql += "'{}',".format(job_history['project_id'])
        sql += "'{}',".format(job_history['file_id'])
        sql += " '{}',".format(round(job_history['version'], 2))
        sql += " '{}',".format(job_history['job_id'])
        if 'content' in job_history.keys():
            sql += " '{}',".format(job_history['content'])
        else:
            sql += " NULL,"
        sql += " True) ;"
        # sql += " '{}') ;".format(job_history['project_id'])
        # sql = "INSERT INTO preparation_job_history(file_id, job_id, version, job_request_user_id, content, active, is_del, project_id) VALUES ("
        # sql += "'{}',".format(job_history['file_id'])
        # sql += " '{}',".format(job_history['job_id'])
        # sql += " '{}',".format(round(job_history['version'], 2))
        # sql += " '{}',".format(job_history['job_request_user_id'])
        # sql += " '{}',".format(job_history['content'])
        # sql += " True,"
        # sql += " False, "
        # sql += " '{}') ;".format(job_history['project_id'])

        self.app.logger.info(sql)
        
        self.db.execute(sql)

    def init_dataset(self):
        sql = 'DELETE FROM dataset j'
        
        self.app.logger.info(sql)
        
        self.db.execute(sql)

    # dataset 조회 전체
    # result_set return 타입 list[dict]
    def select_dataset(self, file_id=None):
        if file_id is None:
            sql = "SELECT * FROM dataset"
        else:
            sql = "SELECT * FROM dataset"
            sql += " WHERE name = '{}'".format(file_id)
            sql += " ORDER BY seq DSC ;"
        
        result_set = list()
        
        self.app.logger.info(sql)
        
        result = self.db.execute(sql)
        for row in result:
            result_set.append(dict(row))
        
        return result_set

    # return type list[dict]
    def select_job_history_by_file_id_and_version(self, project_id, file_id, version):
        sql = "SELECT * FROM job_history"
        sql += " WHERE project_id = '{}'".format(project_id)
        sql += " AND file_id = '{}'".format(file_id)
        sql += " AND \"version\" = {}".format(round(version, 2))
        sql += " AND active = True"
        sql += " AND is_del = False"
        sql += " ORDER BY created ASC ;"
    # def select_job_history_by_file_id_and_version(self, project_id, file_id, version):
    #     sql = "SELECT * FROM preparation_job_history"
    #     sql += " WHERE project_id = '{}'".format(project_id)
    #     sql += " AND file_id = '{}'".format(file_id)
    #     sql += " AND \"version\" = {}".format(round(version, 2))
    #     sql += " AND active = True"
    #     sql += " AND is_del = False"
    #     sql += " ORDER BY seq ASC ;"
        
        self.app.logger.info(sql)
        
        result = self.db.execute(sql)
        result_set = list()
        for row in result:
            result_set.append(dict(row))

        return result_set
        
    # return type list[dict]
    def select_redo_job_history(self, job_id_list):
        sql = "SELECT * FROM job_history"
        sql += " WHERE id in {}".format(job_id_list)
        sql += " ORDER BY created ASC ;"

        self.app.logger.info(sql)
        
        result = self.db.execute(sql)
        result_set = list()
        for row in result:
            result_set.append(dict(row))

        return result_set

    def insert_work_step(self, work_step):
        sql = "INSERT INTO work_steps (id, is_del, group_id, department_id, user_id, project_id, file_id, version, name, description) VALUES ("
        sql += "'{}',".format(work_step['id'])
        sql += " False,"
        sql += "'{}',".format(work_step['group_id'])
        sql += "'{}',".format(work_step['department_id'])
        sql += "'{}',".format(work_step['user_id'])
        sql += " '{}',".format(work_step['project_id'])
        sql += " '{}',".format(work_step['file_id'])
        sql += " '{}',".format(round(work_step['version'], 2))
        sql += " '{}',".format(work_step['work_step_name'])
        sql += " '{}') ;".format(work_step['description'])
        
        self.app.logger.info(sql)
        
        self.db.execute(sql)

    # 작업 규칙 목록 조회
    def select_work_step(self, params):
        sql = "SELECT * FROM work_steps"
        sql += " WHERE is_del = False"
        
        for idx, (key, val) in enumerate(params.items()):                
            if 'before' in key:
                sql += " AND {} >= '{} 00:00:00'".format(key.split('_')[0], val)
            elif 'after' in key:
                sql += " AND {} <= '{} 23:59:59'".format(key.split('_')[0], val)
            elif 'id' in key or 'name' in key:
                sql += " AND {} like '%%{}%%'".format(key, val)
        sql += " ;"
        
        self.app.logger.info(sql)
        
        result = self.db.execute(sql)
        result_set = list()
        for row in result:
            result_set.append(dict(row))

        return result_set
    
    # 작업 규칙 목록 삭제
    def delete_work_step(self, work_step_id):
        sql = "UPDATE work_steps"
        sql += " SET is_del = True,"
        sql += " updated = '{}'".format(datetime.now())
        sql += " WHERE id = '{}'".format(work_step_id)
        sql += " AND is_del = False ;"
        
        self.app.logger.info(sql)
        
        self.db.execute(sql)

    def insert_work_step_details(self, work_step_details):
        sql = "INSERT INTO work_step_details (id, is_del, group_id, department_id, user_id, work_step_id, job_id, content) VALUES ("
        sql += "'{}',".format(work_step_details['id'])
        sql += " False,"
        sql += " '{}',".format(work_step_details['group_id'])
        sql += " '{}',".format(work_step_details['department_id'])
        sql += " '{}',".format(work_step_details['user_id'])
        sql += " '{}',".format(work_step_details['work_step_id'])
        sql += " '{}',".format(work_step_details['job_id'])
        sql += " '{}') ;".format(work_step_details['content'])
        
        self.app.logger.info(sql)
        
        self.db.execute(sql)

    def select_work_step_details(self, work_step_id):
        sql = "SELECT * FROM work_step_details"
        sql += " WHERE work_step_id = '{}'".format(work_step_id)
        sql += " AND is_del = False ;"
        
        self.app.logger.info(sql)
        
        result = self.db.execute(sql)
        result_set = list()
        for row in result:
            result_set.append(dict(row))

        return result_set
    
    # 작업 규칙 목록 삭제
    def delete_work_step_details(self, work_step_id):
        sql = "UPDATE work_step_details"
        sql += " SET is_del = True,"
        sql += " updated = '{}'".format(datetime.now())
        sql += " WHERE work_step_id = '{}'".format(work_step_id)
        sql += " AND is_del = False ;"
        
        self.app.logger.info(sql)
        
        self.db.execute(sql)

    def select_work_step_by_file_id_and_version_and_work_step_name(self, file_id, version, work_step_name):
        sql = "SELECT contents FROM work_steps"
        sql += " WHERE file_id='{}'".format(file_id)
        sql += " AND version = {}".format(round(version, 2))
        sql += " AND work_step_name ='{}'".format(work_step_name)
        
        self.app.logger.info(sql)
        
        self.db.execute(sql)

    # return type list[dict]
    def select_job_history_by_file_id_and_version_and_work_step_name(self, file_id, version, work_step_name):
        sql = "SELECT job_id, content FROM job_history"
        sql += " WHERE file_id = '%s'".format(file_id)
        sql += " AND \"version\" = {}".format(round(version, 2))
        sql += " AND seq = any(("
        sql += "SELECT contents FROM work_steps"
        sql += " WHERE work_step_name = '{}')::integer[])".format(work_step_name)
        sql += " ORDER BY create ASC"
    # def select_job_history_by_file_id_and_version_and_work_step_name(self, file_id, version, work_step_name):
    #     sql = "SELECT job_id, content FROM preparation_job_history"
    #     sql += " WHERE file_id = '%s'".format(file_id)
    #     sql += " AND \"version\" = {}".format(round(version, 2))
    #     sql += " AND seq = any(("
    #     sql += "SELECT contents FROM work_steps"
    #     sql += " WHERE work_step_name = '{}')::integer[])".format(work_step_name)
    #     sql += " ORDER BY seq ASC"

        self.app.logger.info(sql)
        
        result = self.db.execute(sql)
        result_set = list()
        for row in result:
            result_set.append(dict(row))

        return result_set

    def update_job_history_active(self, project_id, file_id, version, min_max, job_active):
        sql = "UPDATE job_history"
        sql += " SET active = {},".format(job_active)
        sql += " updated = '{}'".format(datetime.now())
        sql += " WHERE created = ("
        sql += "SELECT {}(created) from job_history jh".format(min_max)
        sql += " WHERE jh.project_id = '{}'".format(project_id)
        sql += " AND jh.file_id = '{}'".format(file_id)
        sql += " AND jh.\"version\" = {}".format(round(version, 2))
        sql += " AND jh.active != {}".format(job_active)
        sql += " AND jh.is_del = False)"
        sql += " AND file_id = '{}'".format(file_id)
        sql += " AND \"version\" = {} ;".format(round(version, 2))
    # def update_job_history_active(self, project_id, file_id, version, min_max, job_active):
    #     sql = "UPDATE preparation_job_history SET active = {}".format(job_active)
    #     sql += " WHERE create_date_time = ("
    #     sql += "SELECT {}(create_date_time) from preparation_job_history pjh".format(min_max)
    #     sql += " WHERE pjh.project_id = '{}'".format(project_id)
    #     sql += " AND pjh.file_id = '{}'".format(file_id)
    #     sql += " AND pjh.\"version\" = {}".format(round(version, 2))
    #     sql += " AND pjh.active != {}".format(job_active)
    #     sql += " AND pjh.is_del = False)"
    #     sql += " AND file_id = '{}'".format(file_id)
    #     sql += " AND \"version\" = {} ;".format(round(version, 2))
        
        self.app.logger.info(sql)
        
        self.db.execute(sql)
    
    def init_job_history(self, project_id, file_id, version):
        sql = "UPDATE job_history"
        sql += " SET is_del = True,"
        sql += " updated = '{}'".format(datetime.now())
        sql += " WHERE is_del = False"
        sql += " AND project_id = '{}'".format(project_id)
        sql += " AND file_id = '{}'".format(file_id)
        sql += " AND \"version\" = {} ;".format(round(version, 2))
        
        self.app.logger.info(sql)
        
        self.db.execute(sql)
    
    def delete_inactive_job_history(self, project_id, file_id, version):
        sql = "UPDATE job_history"
        sql += " SET is_del = True,"
        sql += " updated = '{}'".format(datetime.now())
        sql += " WHERE active = False"
        sql += " AND is_del = False"
        sql += " AND project_id = '{}'".format(project_id)
        sql += " AND file_id = '{}'".format(file_id)
        sql += " AND \"version\" = {} ;".format(round(version, 2))
    # def delete_job_history(self, project_id, file_id, version):
    #     sql = "UPDATE preparation_job_history SET is_del = True"
    #     sql += " WHERE active = False"
    #     sql += " AND is_del = False"
    #     sql += " AND project_id = '{}'".format(project_id)
    #     sql += " AND file_id = '{}'".format(file_id)
    #     sql += " AND \"version\" = {} ;".format(round(version, 2))
        
        self.app.logger.info(sql)
                
        self.db.execute(sql)
    
    def rename_job_history(self, project_id, file_id, version, rename_id):
        sql = "UPDATE job_history"
        sql += " SET file_id = '{}',".format(rename_id)
        sql += " updated = '{}'".format(datetime.now())
        sql += " WHERE project_id = '{}'".format(project_id)
        sql += " AND file_id = '{}'".format(file_id)
        sql += " AND \"version\" = {} ;".format(round(version, 2))
    # def rename_job_history(self, project_id, file_id, version, rename_id):
    #     sql = "UPDATE preparation_job_history set file_id = '{}'".format(rename_id)
    #     sql += " WHERE project_id = '{}'".format(project_id)
    #     sql += " AND file_id = '{}'".format(file_id)
    #     sql += " AND \"version\" = {} ;".format(round(version, 2))
        
        self.app.logger.info(sql)
                
        self.db.execute(sql)
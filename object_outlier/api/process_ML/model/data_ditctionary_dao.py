from config import engine

from datetime import datetime

class DataDictionaryDao:    
    def select_dictionary_category():
        sql = "SELECT * FROM dictionary_category"
        sql += " WHERE is_del = False ;"
        
        result = engine.execute(sql)
        
        # print(sql)
        
        result_set = list()
        for row in result:
            result_set.append(dict(row))
            
        return result_set

    def select_dictionary_info():
        sql = "SELECT * FROM dictionary_info"
        sql += " WHERE id >= 11 ;"
        
        result = engine.execute(sql)
        
        result_set = list()
        for row in result:
            result_set.append(dict(row))
            
        return result_set
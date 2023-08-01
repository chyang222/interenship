import pandas as pd
import numpy as np
import psycopg2

df = pd.read_csv('/home/datanuri/yjjo/python_scripts/Report/데이터누리_주소판별여부.csv', encoding = 'utf-8-sig', engine='python') 



host = "192.168.2.102"
port = "5432"
user = "metis"
password = "password"
database = "metisdb" 


try:
    # PostgreSQL에 접속
    connection = psycopg2.connect(
        host=host, port=port, user=user, password=password, database=database
    )
    cursor = connection.cursor()

    print("PostgreSQL에 성공적으로 접속했습니다.")

    #------------------------------------------------------------------------------------------------------------
    
    query_distribution = """
    SELECT df.*, distribution.resource_id, distribution.title, distribution.byte_size, distribution.file_name
    FROM df
    LEFT JOIN distribution ON df.칼럼명 = distribution.id;
    """
    df_mapped_1 = pd.read_sql_query(query_distribution, connection)
    
    #------------------------------------------------------------------------------------------------------------
    
    query_resuorce = """
    SELECT df_mapped_1.*, resource.title AS resource_title
    FROM df_mapped_1
    LEFT JOIN resource ON df_mapped_1.resource_id = resource.id;
    """
    df_mapped_2 = pd.read_sql_query(query_resuorce, connection)
    
    #------------------------------------------------------------------------------------------------------------
    
    query_resuorce_categoty = """
    SELECT df_mapped_2.*, resource_category_map.node_id
    FROM df_mapped_2
    LEFT JOIN resource ON df_mapped_2.resource_id = resource_category_map.id;
    """
    
    df_mapped_3 = pd.read_sql_query(query_resuorce_categoty, connection)

    #------------------------------------------------------------------------------------------------------------
    
    query_taxonomy = """
    SELECT df_mapped_3.*, taxonomy.title AS taxonomy_title
    FROM df_mapped_3
    LEFT JOIN resource ON df_mapped_3.node_id = taxonomy.id;
    """
    
    df_mapped_final = pd.read_sql_query(query_taxonomy, connection)
    # 연결 종료
    cursor.close()
    connection.close()
    print("PostgreSQL 연결이 종료되었습니다.")

except (Exception, psycopg2.Error) as error:
    print("PostgreSQL에 접속 중 오류가 발생했습니다:", error)
    
    


df_mapped_final.to_csv("/home/datanuri/yjjo/python_scripts/Report/mapping/데이터누리_address_mapping.csv", encoding = 'utf-8-sig')
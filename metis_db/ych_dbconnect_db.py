import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine


def main(df):        
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

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        
        print("PostgreSQL에 성공적으로 접속했습니다.")

        #------------------------------------------------------------------------------------------------------------
        
        query_distribution = """
        SELECT id, resource_id, title, byte_size, file_name
        FROM distribution;
        """
        df_distribution = pd.read_sql_query(query_distribution, engine)
        merged_df = df.merge(df_distribution, left_on='uuid', right_on='id', how='left')
        merged_df.drop('id', axis=1, inplace=True)
        
        #------------------------------------------------------------------------------------------------------------
            
        query_resuorce = """
        SELECT id, title AS resource_title
        FROM resource
        """
        
        df_resource = pd.read_sql_query(query_resuorce, engine)
        merged_df_2 = merged_df.merge(df_resource, left_on='resource_id', right_on='id', how='left')
        merged_df_2.drop('id', axis=1, inplace=True)
        
        #------------------------------------------------------------------------------------------------------------
        
        query_resuorce_categoty = """
        SELECT resource_id, catalog_id, node_id
        FROM resource_category_map
        """
        
        df_resuorce_categoty = pd.read_sql_query(query_resuorce_categoty, engine)
        merged_df_3 = merged_df_2.merge(df_resuorce_categoty, left_on='resource_id', right_on='resource_id', how='left')
        
        #------------------------------------------------------------------------------------------------------------
        
        query_catalog = """
        SELECT catalog_id, catalog_name
        FROM catalog
        """
        
        df_catalog = pd.read_sql_query(query_catalog, engine)
        merged_df_4 = merged_df_3.merge(df_catalog, left_on='catalog_id', right_on='catalog_id', how='left')
        #------------------------------------------------------------------------------------------------------------

        query_taxonomy = """
        SELECT id, title AS taxonomy_title
        FROM taxonomy
        """
        df_taxonomy = pd.read_sql_query(query_taxonomy, engine)
        df_final = merged_df_4.merge(df_taxonomy, left_on='node_id', right_on='id', how='left')
        df_final.drop('id', axis=1, inplace=True)
        
        #------------------------------------------------------------------------------------------------------------
        df_final = df_final.dropna(subset=['taxonomy_title'])        
        df_final.to_csv("/home/datanuri/yjjo/python_scripts/Report/mapping/데이터누리_address_mapping.csv", encoding = 'utf-8-sig', index=False)    

    except (Exception, psycopg2.Error) as error:
        print("PostgreSQL에 접속 중 오류가 발생했습니다:", error)


if __name__=="__main__":
    df = pd.read_csv('/home/datanuri/yjjo/python_scripts/Report/데이터누리_주소판별여부.csv', encoding = 'utf-8-sig', engine='python') 
    df.drop('Unnamed: 0', axis=1, inplace=True)
    df['uuid'] = df['uuid'].str.replace('.csv', '')
    main(df)
    
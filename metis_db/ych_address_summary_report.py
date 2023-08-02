import pandas as pd
import numpy as np

def main(df):
    summ_fomat = pd.DataFrame(columns=('카탈로그','분류 체계','전체 건', '주소 포함 건수', '주소 포함 비율'))

    node = df.groupby(['node_id','catalog_id'])

    
    node_idx = node['주소 여부'].count().sort_values().index
    node_count = node['주소 여부'].count().sort_values().to_list()
    include_df = node.agg({'주소 여부':get_add_count})['주소 여부'].droplevel(axis=0,level=1)

    idx = 0
    for id in node_idx:
        title = df[df.node_id == id[0]].taxonomy_title.iloc[0]
        addr = include_df[include_df.index == id[0]].iloc[0]
        catalog = df[df.node_id == id[0]].catalog_name.iloc[0]
        in_df = [catalog, title, node_count[idx], addr, round((addr/node_count[idx]) * 100, 2)] 
        summ_fomat.loc[idx] = in_df
        idx += 1
        
    writer = pd.ExcelWriter('/home/datanuri/yjjo/python_scripts/Report/mapping/데이터누리_address_final.xlsx', engine = 'xlsxwriter')
    summ_fomat.to_excel(writer, index = False, sheet_name = '요약')
    df.to_excel(writer , index = False, sheet_name = '세부내역')
    writer.save()   


def get_add_count(series):
    res = len([ x for x in series if x[0]=='O'])
    return res


if __name__=="__main__":
    data = pd.read_csv('/home/datanuri/yjjo/python_scripts/Report/mapping/데이터누리_address_mapping.csv', encoding = 'utf-8-sig', engine='python') 
    df = data.copy()
    main(df)
    
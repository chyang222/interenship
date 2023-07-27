import pandas as pd
import numpy as np
import os
import datetime

filePath, fileName = os.path.split(__file__) 

for_mapping_dic_kor = {
        1 : '전기차 충전 이력',
        2 : '전기차 충전 요금 이력',
        3 : '전기차 운행이력',
        4 : '전기차 배터리 상태 이력',
        5 : '전기차 충전기 운영 알람 이력',
        6 : '전기차 충전기 대기 전력량',
        7 : '전기차 배터리 충전 공급 전압/전류',
        8 : '수소버스 운행이력',
        9 : '전기차 충전기 DR 운영 이력',
        10 : '전기버스 노선별 충전이력',
        11 : '전기버스 노선 운영지표',
        12 : '전기버스 노선 굴곡도 대비 에너지 사용량',
        13 : '개방충전소(택시) 사용전력',
        14 : '개방충전소(택시) 사용요금',
        15 : '개방충전소 승용 및 택시 이력 데이터',
        16 : '한전요금제별 충전요금 분석',
        17 : '충전사업자 기준 전기택시 정보 및 차량보유현황',
        18 : '지역별 충전 전력 소모량',
        19 : '기온/강수량과 충전전력사용량 데이터',
        20 : '기온/강수량과 충전전력요금 데이터'
    }

for_mapping_dic_kor = dict(map(reversed,for_mapping_dic_kor.items()))

for_last_summary = pd.DataFrame(columns=('상품번호','상품명','원천데이터','상품데이터'))
report_format = pd.DataFrame(columns=('상품번호','상품명','컬럼명','오류유형 및 정제', '처리수'))
log_file = open('/home/datanuri/3_ppk_2023/2_데이터_가공/logs/ych_proc_raw_20230717.log')

count = 0
for idx, one_line in enumerate(log_file):
    try:
        reading_one_log = one_line.strip().split('|')
        if len(reading_one_log) == 4:
            report_format.loc[idx, '상품번호'] = for_mapping_dic_kor[reading_one_log[0].split(':')[-1].strip()]
            report_format.loc[idx, '상품명'] = reading_one_log[0].split(':')[-1].strip()
            report_format.loc[idx, '컬럼명'] = reading_one_log[1]
            report_format.loc[idx, '오류유형 및 정제'] = reading_one_log[2]
            report_format.loc[idx, '처리수'] = reading_one_log[3]
        if len(reading_one_log) == 3:
            if reading_one_log[1] == '정제전데이터수':
                for_last_summary.loc[count, '상품번호'] = for_mapping_dic_kor[reading_one_log[0].split(':')[-1].strip()]
                for_last_summary.loc[count, '상품명'] = reading_one_log[0].split(':')[-1].strip()
                for_last_summary.loc[count, '원천데이터'] = int(reading_one_log[2])
                count += 1
            elif reading_one_log[1] == '정제후데이터수':
                for_last_summary.loc[count, '상품번호'] = for_mapping_dic_kor[reading_one_log[0].split(':')[-1].strip()]
                for_last_summary.loc[count, '상품명'] = reading_one_log[0].split(':')[-1].strip()
                for_last_summary.loc[count, '상품데이터'] = int(reading_one_log[2])
                count += 1
    except:
        # print('#### {} : {}'.format(idx, one_line))
        pass

report_format.sort_values(['상품번호', '컬럼명'], inplace = True)
report_format.reset_index(drop = True, inplace = True)

# print(report_format.head(50))

product_number = []
product_name = []
column_name = []
error_type = []
error_count = []
transfer_type = []
transfer_count = []
delete_type = []
delete_count = []

temp_column = ''

vin_df = pd.DataFrame(columns=('상품번호','상품명','컬럼명','오류유형 내용', '오류', '변환 내용', '변환', '삭제 내용', '삭제'))
report_format2 = pd.DataFrame(columns=('상품번호','상품명','컬럼명','오류유형 내용', '오류', '변환 내용', '변환', '삭제 내용', '삭제'))
for idx, one_line in enumerate(report_format['컬럼명']):

    
    if temp_column != one_line and idx != 0:
        dic = {'상품번호' : product_number,
                '상품명' : product_name,
                '컬럼명' : column_name,
                '오류유형 내용' : error_type,
                '오류' : error_count,
                '변환 내용' : transfer_type,
                '변환' : transfer_count,
                '삭제 내용' : delete_type,
                '삭제' : delete_count}
        
        res = pd.DataFrame.from_dict(dic, orient='index')
        res = res.transpose()
        vin_df = pd.concat([vin_df, res], axis = 0)


        # 초기화
        product_number = []
        product_name = []
        column_name = []
        error_type = []
        error_count = []
        transfer_type = []
        transfer_count = []
        delete_type = []
        delete_count = []
        report_format2 = pd.DataFrame(columns=('상품번호','상품명','컬럼명','오류유형 내용', '오류', '변환 내용', '변환', '삭제 내용', '삭제'))
    
    important_one = report_format.loc[idx,'오류유형 및 정제'].split(' ')[-1]

    if  important_one == '오류':
        product_number.append(report_format.loc[idx,'상품번호'])
        product_name.append(report_format.loc[idx,'상품명'])
        column_name.append(report_format.loc[idx,'컬럼명'])
        error_type.append(report_format.loc[idx,'오류유형 및 정제'])
        error_count.append(int(report_format.loc[idx,'처리수']))

    elif important_one == '변환':
        transfer_type.append(report_format.loc[idx,'오류유형 및 정제'])
        transfer_count.append(int(report_format.loc[idx,'처리수']))

    elif important_one == '삭제':
        delete_type.append(report_format.loc[idx,'오류유형 및 정제'])
        delete_count.append(int(report_format.loc[idx,'처리수']))
    
    else:
        pass

    temp_column = one_line
    if idx == len(report_format['컬럼명']) - 1:
        dic = {'상품번호' : product_number,
        '상품명' : product_name,
        '컬럼명' : column_name,
        '오류유형 내용' : error_type,
        '오류' : error_count,
        '변환 내용' : transfer_type,
        '변환' : transfer_count,
        '삭제 내용' : delete_type,
        '삭제' : delete_count}

        res = pd.DataFrame.from_dict(dic, orient='index')
        res = res.transpose()
        vin_df = pd.concat([vin_df, res], axis = 0)

vin_df = vin_df.groupby(['상품번호','상품명','컬럼명']).agg({
                            '오류유형 내용':'unique',
                            '오류': "sum",
                            '변환 내용':'unique',
                            '변환': "sum",
                            '삭제 내용':'unique',
                            '삭제': "sum"}).reset_index()

# 상품명 기준 정리해서, 최종 요약 내역 뽑아내기
summary = vin_df.groupby(['상품번호','상품명']).agg({'오류' : "sum",
                                       '변환' : "sum",
                                       '삭제' : "sum"})

for idx, i in enumerate(vin_df['오류유형 내용']):
    # i = str(i).replace("'",'')
    i = str(i).replace("[", '')
    i = str(i).replace("]", '')
    i = str(i).replace("None", '')
    vin_df.loc[idx,'오류유형 내용'] = i

for idx, i in enumerate(vin_df['오류']):
    if i == 0:
        vin_df.loc[idx,'오류'] = ''

for idx, i in enumerate(vin_df['변환 내용']):
    # i = str(i).replace("'",'')
    i = str(i).replace("[", '')
    i = str(i).replace("]", '')
    i = str(i).replace("None", '')
    vin_df.loc[idx,'변환 내용'] = i

for idx, i in enumerate(vin_df['변환']):
    if i == 0:
        vin_df.loc[idx,'변환'] = ''

for idx, i in enumerate(vin_df['삭제 내용']):
    # i = str(i).replace("'",'')
    i = str(i).replace("[", '')
    i = str(i).replace("]", '')
    i = str(i).replace("None", '')
    vin_df.loc[idx,'삭제 내용'] = i

for idx, i in enumerate(vin_df['삭제']):
    if i == 0:
        vin_df.loc[idx,'삭제'] = ''

add_summary = for_last_summary.groupby(['상품번호','상품명']).agg({'원천데이터': "sum", '상품데이터' : "sum"})

summary = pd.concat([summary,add_summary], axis = 1).reset_index()
# summary['삭제비율'] = summary['삭제']/summary['원천데이터'] * 100
summary['삭제비율'] = summary.apply(lambda x : round(x['삭제'] / x['원천데이터'] * 100,2),axis = 1)
summary = summary[['상품번호', '상품명', '원천데이터', '오류', '변환', '삭제','삭제비율', '상품데이터']]

summary = summary.fillna(0)

vin_df['삭제 내용'] = vin_df['삭제 내용'].fillna('-')
vin_df['삭제'] = vin_df['삭제'].fillna(0)

days = datetime.datetime.now().strftime('%Y%m%d')
writer = pd.ExcelWriter(f'/home/datanuri/3_ppk_2023/4_가공레포트/report/데이터누리_데이터정제가공 결과서_{days}.xlsx', engine = 'xlsxwriter')
summary.to_excel(writer, index = False, sheet_name = '요약')
vin_df.to_excel(writer , index = False, sheet_name = '세부내역')
writer.save()   
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd
import os
import logging
import requests
import json
from datetime import datetime

def extract_load_price_input(mysql_conn_id, execution_date, **context):

    none_id_list = [] 
    transform_id_list = []
    values_list = []

    # 1. 배추 
    url = 'http://www.kamis.or.kr/service/price/xml.do?action=periodProductList&p_productclscode=01&p_startday={}&p_endday={}&p_itemcategorycode=200&p_itemcode=211&p_kindcode=03&p_productrankcode=04&p_countrycode=1101&p_convert_kg_yn=n&p_cert_key=938c7121-0448-4cf5-acfe-e4f87400e335&p_cert_id=2926&p_returntype=json'.format(execution_date, execution_date) 
    response = requests.get(url).json()

    if len(response['data']) == 1:
        none_id_list.append(1)
    else:
        transform_id_list.append(1)
        values_list.append(( 1, int(response['data']['item'][0]['price'].replace(',', '')) ))

    # 2. 무
    url = 'http://www.kamis.or.kr/service/price/xml.do?action=periodProductList&p_productclscode=01&p_startday={}&p_endday={}&p_itemcategorycode=200&p_itemcode=231&p_kindcode=03&p_productrankcode=04&p_countrycode=1101&p_convert_kg_yn=n&p_cert_key=938c7121-0448-4cf5-acfe-e4f87400e335&p_cert_id=2926&p_returntype=json'.format(execution_date, execution_date) 
    response = requests.get(url).json()

    if len(response['data']) == 1:
        none_id_list.append(2)
    else:
        transform_id_list.append(2)
        values_list.append(( 2, int(response['data']['item'][0]['price'].replace(',', '')) ))

    # 3. 양파
    url = 'http://www.kamis.or.kr/service/price/xml.do?action=periodProductList&p_productclscode=01&p_startday={}&p_endday={}&p_itemcategorycode=200&p_itemcode=245&p_kindcode=00&p_productrankcode=04&p_countrycode=1101&p_convert_kg_yn=n&p_cert_key=938c7121-0448-4cf5-acfe-e4f87400e335&p_cert_id=2926&p_returntype=json'.format(execution_date, execution_date) 
    response = requests.get(url).json()

    if len(response['data']) == 1:
        none_id_list.append(3)
    else:
        transform_id_list.append(3)
        values_list.append(( 3, int(response['data']['item'][0]['price'].replace(',', '')) ))

    # 4. 쌀
    url = 'http://www.kamis.or.kr/service/price/xml.do?action=periodProductList&p_productclscode=01&p_startday={}&p_endday={}&p_itemcategorycode=100&p_itemcode=111&p_kindcode=01&p_productrankcode=04&p_countrycode=1101&p_convert_kg_yn=n&p_cert_key=938c7121-0448-4cf5-acfe-e4f87400e335&p_cert_id=2926&p_returntype=json'.format(execution_date, execution_date) 
    response = requests.get(url).json()

    if len(response['data']) == 1:
        none_id_list.append(4)
    else:
        transform_id_list.append(4)
        values_list.append(( 4, int(response['data']['item'][0]['price'].replace(',', '')) ))

    # 5. 감자
    url = 'http://www.kamis.or.kr/service/price/xml.do?action=periodProductList&p_productclscode=01&p_startday={}&p_endday={}&p_itemcategorycode=100&p_itemcode=152&p_kindcode=01&p_productrankcode=04&p_countrycode=1101&p_convert_kg_yn=n&p_cert_key=938c7121-0448-4cf5-acfe-e4f87400e335&p_cert_id=2926&p_returntype=json'.format(execution_date, execution_date) 
    response = requests.get(url).json()

    if len(response['data']) == 1:
        none_id_list.append(5)
    else:
        transform_id_list.append(5)
        values_list.append(( 5, int(response['data']['item'][0]['price'].replace(',', '')) ))

    if not values_list:
        context['ti'].xcom_push(key='none_id_list', value=none_id_list)
        return 'transform_price_output'
    else:
        sql_string = ''
        table_name = 'PriceInput'
        produced = [ 2017507, 1172345, 1576752, 5211037, 549878 ]

        for i, value in enumerate(values_list):
            value = (execution_date,) + value + (produced[i],)
            sql_string = sql_string + str(value) + ','
        
        sql_string = "INSERT INTO " + table_name + " values " + sql_string[:-1]
        
        # mysql = MySqlHook(mysql_conn_id=mysql_conn_id)
        # conn = mysql.get_conn()
        # cur = conn.cursor()
        # cur.execute(sql_string)
        # conn.commit()

        context['ti'].xcom_push(key='none_id_list', value=none_id_list)
        context['ti'].xcom_push(key='transform_id_list', value=transform_id_list)
        return 'extract_load_other_input'

def extract_load_other_input(mysql_conn_id, execution_date, **context):

    # dtparam = execution_date.replace('-', '') => 추후 이걸로 변경 예정, 시간 관련 때문에 지금은 전날것으로 진행
    dtparam = '2022-11-10'.replace('-', '')

    url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'
    params = { 'serviceKey' : 'tFJ4Yv6uk6Ln/rLoQmzsa+yxjSDOfbEoMowILA/o9GeEyd1nFUASVIKlFxIO91N0Ix2DQXGYNbai7XHf2sCpMw==',
         'pageNo' : '1',
         'numOfRows' : '10',
         'dataType' : 'json',
         'dataCd' : 'ASOS',
         'dateCd' : 'DAY',
         'startDt' : dtparam,
         'endDt' : dtparam,
         'stnIds' : '108' }

    response = requests.get(url, params=params).json()
    response = response['response']['body']['items']['item'][0]

    if response['sumRn'] == '':
        response['sumRn'] = '0.0'
    values = ( execution_date, float(response['sumRn']), float(response['avgWs']), 2.5, 8.7 ) # 강수량, 풍속, 소비자 물가 상승률, 농산물 물가 상승률

    sql_string = ''
    table_name = 'OtherInput'

    sql_string = "INSERT INTO " + table_name + " values " + str(values)
        
    # mysql = MySqlHook(mysql_conn_id=mysql_conn_id)
    # conn = mysql.get_conn()
    # cur = conn.cursor()
    # cur.execute(sql_string)
    # conn.commit()

def transform_load_price_output(mysql_conn_id, execution_date, **context):

    transform_id_list = context['ti'].xcom_pull(key='transform_id_list')
    
    for id in transform_id_list:

        mysql = MySqlHook(mysql_conn_id=mysql_conn_id)
        conn = mysql.get_conn()
        cur = conn.cursor()
        sql_string = 'select * from PriceInput where id={} order by date'.format(id)
        cur.execute(sql_string)
        rows = cur.fetchall()

        columnNames = [column[0] for column in cur.description]
        df = pd.DataFrame(rows, columns=columnNames)

        print(df)

def transform_price_output(mysql_conn_id, execution_date, **context):

    print('hihi')
    
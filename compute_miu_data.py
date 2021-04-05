#coding=utf-8
import requests
import re
import pymysql
import pandas as pd
import logging
import math
import datetime
from multiprocessing import Pool
import json
pd.set_option('display.max_rows',1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth',1000)

logging.basicConfig(level=logging.DEBUG,filename='compute_miu_data.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

def db_to_df(db,h_table,id,day):
    cursor = db.cursor()
    print('day:',day)
    sql = "select data from stock_miu_trade{} where stock_id = '{}' and trade_date like '{}%' ".format(h_table,id,day)
    cursor.execute(sql)
    data = cursor.fetchall()[0][0]
    cursor.close()
    #print('db_data:',data[0:10])
    #data_json = data
    data_json = json.loads(data)
    df = pd.DataFrame.from_dict(data_json, orient='columns')
    #print('df1:',df)
    df['tm'] = ''
    for i in range(len(df)):
        df.loc[i,'tm'] = str(df.loc[i,'t'])[:-2]
    #print('df2:', df)
    df = df.groupby(['tm'])['v'].sum()
    df = df.reset_index()
    #print('df3:', df)
    return df
def last_day(db,h_table,id):
    cursor = db.cursor()
    sql = "SELECT distinct(trade_date) FROM stockdb.stock_miu_trade{} where stock_id = '{}' order by trade_date DESC limit 20".format(h_table, id)
    cursor.execute(sql)
    days = cursor.fetchall()
    print('days:',type(days[0][0]))
    cursor.close()
    return days,len(days)
def compute_junxian(db, h_table, id):
    p_v_json = []
    days,len_days=last_day(db, h_table, id)
    df_sum = {'tm':[],'v':[],'time':[]}
    df_sum = pd.DataFrame(df_sum)
    for day in days:
        #print('day1:',day[0].strftime("%Y-%m-%d"))
        day = day[0]#.strftime("%Y-%m-%d")
        if day == None:
            continue
        df = db_to_df(db, h_table, id, day)
        df_sum.append(df)
    df_sum = df_sum.groupby(['tm'])['v'].sum()
    df_sum = df.reset_index()
    df_sum['p_v'] = df_sum['v'] / len_days
    for i in range(len(df_sum)):
        #time_miu = datetime.datetime.strptime((df_sum.loc[i,'tm'] + '00'), "%H:%M:%S")
        df_sum.loc[i, 'time'] = df_sum.loc[i,'tm'] + '00'
        p_v_json.append({'time':df_sum.loc[i, 'time'] , 'pv':df_sum.loc[i,'p_v']})
    df_sum.to_csv('{}_df_sum.csv'.format(id))
    print('df_sum',df_sum,p_v_json)
    return df_sum,p_v_json
def save_json(db,p_v_json,trade_code,h_table):
    json_str = json.dumps(p_v_json)
    cursor = db.cursor()
    sql = "insert into stock_miu_trade{0}(trade_code,20_miu_json) " \
          "values('{1}','{2}') " \
            "ON DUPLICATE KEY UPDATE trade_code = '{1}',20_miu_json='{2}' " \
            .format(h_table,trade_code,json_str)
    cursor.execute(sql)
    db.commit()
    cursor.close()
def compute_pianli():
    pass
def main():
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    h_table = '2'
    id = '601975'
    date = '2020-11-19'
    trade_code = re.sub('-','',date)+id
    print('trade_code:',trade_code)
    df_sum,p_v_json = compute_junxian(db, h_table, id)
    save_json(db, p_v_json, trade_code,h_table)
    #db_to_df(db,h_table,id,day)
    #last_day(db, h_table, id)
if __name__ == '__main__':
    main()
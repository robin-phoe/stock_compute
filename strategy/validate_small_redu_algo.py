#通过热门回头捕捉反弹
#短点：单涨停后小回头，前几日有单涨停加分，10日均线低于30日均线加分。【单涨停】：收盘是10日内最大值，10日increase<= 20%。
#大热门：30日内大热门，大回头

import mpl_finance
# import tushare as ts
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pymysql
from matplotlib import ticker
from matplotlib.pylab import date2num
import numpy as np
import datetime
import logging
import re
from multiprocessing import Pool
import json
import openpyxl

logging.basicConfig(level=logging.DEBUG, filename='comp_redu_210120.py.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
def get_df_from_db(sql, db):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    # df = df.set_index(keys=['trade_date'])
    df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
    df.reset_index(inplace=True)
    # df = df.dropna(axis=0, how='any')
    # df.reset_index(inplace=True)
    # df['trade_date2'] = df['trade_date'].copy()
    # print('trade_date2:',type(df['trade_date2'][0]))
    # df['trade_date2'] = pd.to_datetime(df['trade_date2']).map(date2num)
    # df['dates'] = np.arange(0, len(df))
    # df['avg_10'] = df['close_price'].rolling(10).mean()
    df['avg_5'] = df['close_price'].rolling(5).mean()
    cursor.close()
    # print(df)
    # df['trade_date'] = date2num(df['trade_date'])
    # print('df:', df[['avg_10', 'close_price']])
    return df
def save(db,trade_code,trade_date,stock_id,stock_name,verify_flag,avg_5_flag,increase=0,second_zhangting=0):
    cursor = db.cursor()
    try:
        sql = "insert into verify_result(trade_code,trade_date,stock_id,stock_name,avg_5_flag,increase,verify_flag,second_zhangting) \
            values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}') " \
              "ON DUPLICATE KEY UPDATE trade_code='{0}',trade_date='{1}',stock_id='{2}',stock_name='{3}'," \
              "avg_5_flag='{4}',increase = '{5}' ,verify_flag = '{6}',second_zhangting = '{7}' \
            ".format(trade_code,trade_date,stock_id,stock_name,avg_5_flag,increase,verify_flag,second_zhangting)
        print('sql:', sql)
        cursor.execute(sql)
        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{},name:{},date:{}'.format(id, stock_name,trade_date))
    except Exception as err:
        db.rollback()
        print('存储失败:', err)
        logging.error('存储失败:id:{},name:{},date:{}\n{}'.format(id, stock_name,trade_date, err))
    cursor.close()
#单涨停回头验证
def make_data(df,db):
    for i in range(11,len(df)-5):
        if df.loc[i,'increase'] >= 9.75 / 100 and df['increase'][i-10:i].sum() <= 17 / 100:
            avg_5_flag = 0
            increase_list = []
            for j in range(i+1,i+4):
                if df.loc[j,'increase'] >= 9.75 / 100:
                    if j ==i+1:
                        save(db, df.loc[i, 'trade_code'], df.loc[i, 'trade_date'], df.loc[i, 'stock_id'],
                             df.loc[i, 'stock_name'], 0, avg_5_flag, df.loc[j,'increase'],second_zhangting= 1)
                        break
                    else:
                        save(db,df.loc[i,'trade_code'],df.loc[i,'trade_date'],df.loc[i,'stock_id'],df.loc[i,'stock_name'],
                                                        1,avg_5_flag,df.loc[j,'increase'],second_zhangting=(j-i))
                        break
                elif min(df.loc[j,'high_price'],df.loc[j,'low_price']) <= df.loc[j,'avg_5']:
                    avg_5_flag = 1
                    increase_list.append(df.loc[j, 'increase'])
                else:
                    increase_list.append(df.loc[j,'increase'])
            else:
                save(db, df.loc[i, 'trade_code'], df.loc[i, 'trade_date'], df.loc[i, 'stock_id'],
                     df.loc[i, 'stock_name'], 0,avg_5_flag,increase=max(increase_list))
        else:
            continue

def main(h_table):
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db.cursor()
    sql = "select distinct stock_id from stock_history_trade{0}".format(h_table)
    cursor.execute(sql)  # 执行SQL语句
    id_list = cursor.fetchall()
    # id_list = [(600126,),] #杭钢股份
    cursor.close()
    for id in id_list:
        id = id[0]
        sql = "select trade_code,stock_id,stock_name,increase,trade_date,close_price,high_price,low_price  " \
              "from stock_history_trade{0} where stock_id = '{1}'".format(h_table,id)
        df = get_df_from_db(sql, db)
        make_data(df, db)

if __name__ == '__main__':
    h_table = '1'
    main(h_table)
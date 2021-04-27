import pandas as pd
import pymysql
import datetime
import logging
import re
from multiprocessing import Pool
import json
import copy
import numpy as np
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config

#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)

logging.basicConfig(level=logging.DEBUG, filename='../log/single_limit_up.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
def get_df_from_db(sql, db):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    cursor.close()
    return df
def save(db,df,date):
    cursor = db.cursor()
    #清理当日原数据
    sql = "delete from limit_up_single where trade_date = '{}'".format(date)
    try:
        cursor.execute(sql)
        db.commit()
        print('date:{} 清理成功。'.format(date))
        logging.info('date:{} 清理成功。'.format(date))
    except Exception as err:
        print('date:{} 清理失败:{}'.format(date,err))
        logging.info('date:{} 清理失败:{}'.format(date,err))
    #df转列表
    data_list = df.apply(lambda row: tuple(row), axis=1).values.tolist()
    try:
        sql = "insert into  limit_up_single(trade_code,stock_id,stock_name,trade_date,grade,monitor) \
            values(%s,%s,%s,%s,%s,%s) "
        # print('sql:', sql)
        cursor.executemany(sql, data_list)
        db.commit()
        print('存储完成')
        logging.info('存储完成')
    except Exception as err:
        db.rollback()
        print('存储失败:', err)
        logging.error('存储失败:{}'.format(err))
    cursor.close()
def core(db,start_t,date):
    time_start = datetime.datetime.now()
    sql = "select trade_code,stock_id,stock_name,trade_date,close_price,increase,turnover_rate " \
          "from stock_trade_data " \
          "where stock_id not like '688%' and trade_date >= '{0}' and trade_date <= '{1}' " \
          "".format(start_t,date)  # and stock_id in ('002940','000812')
    #test
    # sql = "select trade_code,stock_id,stock_name,trade_date,close_price,increase,turnover_rate " \
    #       "from stock_trade_data " \
    #       "where stock_id in ('002940','002168') and trade_date >= '{0}' and trade_date <= '{1}' " \
    #       "".format(start_t,date)  # and stock_id in ('002940','000812')
    df = get_df_from_db(sql, db)
    time_end = datetime.datetime.now()
    print('delta_time:',time_end - time_start)
    #标记limit_up 数据行
    df['limit_up'] = df['increase'].apply(lambda x: 1 if x >= 9.75 else 0)
    #设置时间为索引
    df = df.set_index(keys=['trade_date'])
    #日期升序排序
    df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last', inplace=True)
    #计算涨停后平缓或者下降趋势，小于等于涨停收盘*102.5% AND 大于 （）：
    print('df columns1:', df.columns)
    df['grade'] = 0
    #shift increase
    df['shift1'] = df.groupby(['stock_id'])['increase'].shift(-1)
    df['shift1'].fillna(-1000,inplace=True)
    df['shift2'] = df.groupby(['stock_id'])['increase'].shift(-2)
    df['shift2'].fillna(-1000, inplace=True)
    df['shift3'] = df.groupby(['stock_id'])['increase'].shift(-3)
    df['shift3'].fillna(-1000, inplace=True)
    df['shift4'] = df.groupby(['stock_id'])['increase'].shift(-4)
    df['shift4'].fillna(-1000, inplace=True)
    print('df columns2:', df.columns)
    def com_shift(row):
        day_count=0
        if row['limit_up'] != 1:
            return row
        if row['shift4'] != -1000:
            day_count = 4
        elif row['shift3'] != -1000:
            day_count = 3
        elif row['shift2'] != -1000:
            day_count = 2
        elif row['shift1'] != -1000:
            day_count = 1
        else:
            return row
        sum_increase = 0
        for i in range(1,day_count+1):
            if row['shift{}'.format(str(i))] > 2.5:
                return row
            sum_increase += row['shift{}'.format(str(i))]
        if sum_increase >= 5 :
            return row
        if day_count <= 3 and -5 < sum_increase < 2:
             row['grade'] = 20000
        else:
             row['grade'] = 10000
        return row
    df = df.apply(com_shift,axis=1)
    #rolling 5日统计
    limit_up_5 = df.groupby(['stock_id'])['limit_up','grade'].rolling(5).sum()
    print('limit_up_5:', limit_up_5)
    limit_up_20 = df.groupby(['stock_id'])['limit_up'].rolling(20).sum()
    # print('limit_up_20:', limit_up_20)
    df = pd.merge(df, limit_up_5, how='left', on=['stock_id', 'trade_date'])
    print('df columns:',df.columns)
    df.rename(columns={'limit_up_x': 'limit_up', 'limit_up_y': 'limit_up_5','grade_y':'grade'}, inplace=True)
    df = pd.merge(df, limit_up_20, how='left', on=['stock_id', 'trade_date'])
    df.rename(columns={'limit_up_x': 'limit_up', 'limit_up_y': 'limit_up_20'}, inplace=True)

    df.fillna(0,inplace=True)
    #删除不是今日的数据行
    df.reset_index(inplace=True)
    df.drop(df[df.trade_date < date].index, inplace=True)
    #删除limit_up_5 <1数据行
    df.drop(df[df.limit_up_5 < 1].index, inplace=True)
    #删除limit_up_20 >3数据行
    df.drop(df[df.limit_up_20 > 3].index, inplace=True)
    #删除grade = 0数据行
    df.drop(df[df.grade < 1000].index, inplace=True)
    df['monitor'] = 1
    df['trade_date'] = df['trade_date'].astype('str',)
    print('df_column:',df.columns)
    df = df[['trade_code','stock_id','stock_name','trade_date','grade','monitor']]
    print('df:',df)
    save(db, df, date)
def main(date):
    db_config = read_config('db_config')
    db = pymysql.connect(host=db_config["host"], user=db_config["user"],
                         password=db_config["password"], database=db_config["database"])
    if date == None:
        date = datetime.datetime.now().strftime('%Y-%m-%d')
    date_time = datetime.datetime.strptime(date, '%Y-%m-%d')
    start_t = (date_time - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    core(db, start_t, date)
if __name__ == '__main__':
    date = '2020-12-31'
    time1 = datetime.datetime.now()
    main(date)
    print('time_delta:',datetime.datetime.now() - time1)
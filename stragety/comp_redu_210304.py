# coding:utf-8
# import tushare as ts
import pandas as pd
import pymysql
import numpy as np
import datetime
import logging
import re
from multiprocessing import Pool
import json
#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
logging.basicConfig(level=logging.DEBUG, filename='../comp_redu_210304.log', filemode='w',
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
    # df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
    # df.reset_index(inplace=True)
    cursor.close()
    # #print(df)
    # df['trade_date'] = date2num(df['trade_date'])
    # #print('df:', df[['avg_10', 'close_price']])
    return df
#计算龙虎榜热度
def com_redu1(db,date,delta = 30):
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    start_date = (end_date - datetime.timedelta(days=delta)).strftime("%Y-%m-%d")
    sql = "select L.stock_code,count(L.jmrate)*10000 as longhu_count from longhu_info L " \
          "where  " \
          "trade_date >= '{0}' and trade_date <= '{1}'and reson not like '退市%' " \
          "group by stock_code".format(start_date,date)
    longhu_df = get_df_from_db(sql, db)
    return longhu_df
#计算涨停热度
def com_redu2(db,date,delta = 30):
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    start_date = (end_date - datetime.timedelta(days=delta)).strftime("%Y-%m-%d")
    sql = "select stock_id,stock_name,trade_date,close_price,increase from stock_trade_data " \
          "where trade_date >= '{0}' and trade_date <= '{1}' and stock_id not like '688%'".format(start_date,date)
    zhangting_df = get_df_from_db(sql, db)
    zhangting_df['increase_flag'] = zhangting_df['increase'].apply(lambda x: 10000 if x >= 9.75 else 0)
    zhangting_count_df = zhangting_df.groupby('stock_id',as_index=False)['increase_flag'].sum()
    # print('zhangting_count_df:',zhangting_count_df)
    # 求涨停与龙虎热度和
    longhu_df = com_redu1(db, date, delta=30)
    # #print('longhu_df:', longhu_df)
    longhu_df = longhu_df.rename(columns={'stock_code':'stock_id'})
    # print('longhu_df:', longhu_df)
    redu_df = pd.merge(zhangting_count_df,longhu_df,how='outer',on='stock_id')
    # print('redu_df:',redu_df)
    redu_df.fillna(0, inplace=True)
    redu_df['redu'] = redu_df['increase_flag'] + redu_df['longhu_count']
    redu_df = redu_df.drop(redu_df[redu_df.redu < 10000].index)
    print('redu_df_len:',len(redu_df))
    #求5日均值
    start_date = (end_date - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
    sql = "select stock_id,trade_date,close_price,increase from stock_trade_data " \
          "where trade_date >= '{0}' and trade_date <= '{1}' and stock_id not like '688%'".format(start_date,date)
    zhangting_avg_df = get_df_from_db(sql, db)
    zhangting_avg_df.set_index(['trade_date'],inplace=True)
    zhangting_avg_df = zhangting_avg_df.groupby('stock_id')['close_price'].rolling(5).mean()
    # zhangting_avg_df.rename(index={'close_price': 'avg_5'}, inplace=True)
    # zhangting_avg_df.reset_index(inplace=True)
    zhangting_df = pd.merge(zhangting_df,zhangting_avg_df,how='left',on=['stock_id','trade_date'])
    zhangting_df.rename(columns={'close_price_x': 'close_price','close_price_y': 'avg_5'}, inplace=True)
    # #print('zhangting_df:',zhangting_df)
    zhangting_df = pd.merge(zhangting_df, redu_df, how='left', on='stock_id')
    # #print('zhangting_df:',zhangting_df.head())
    zhangting_df = zhangting_df.fillna(0)
    #print('len1:',len(zhangting_df))
    # 删除非本日数据行
    zhangting_df = zhangting_df.drop(zhangting_df[zhangting_df.trade_date < date].index)
    print('len3:', len(zhangting_df))
    # print('zhangting_df:',zhangting_df['redu'])
    # 删除热度小于10000的数据行
    zhangting_df = zhangting_df.drop(zhangting_df[zhangting_df.redu < 10000].index)
    print('len2:', len(zhangting_df))
    zhangting_df['low_flag'] = zhangting_df['avg_5'] - zhangting_df['close_price']
    return zhangting_df
#判断当日下穿5日均线是10日内极大值之后首次下穿
def com_redu_init(redu,df):
    if redu<10000:
        return 0
    max_value = df['close_price'][len(df)-11 : len(df) - 1].max()
    #print('max_value:',max_value)
    index = df.query("close_price == {}".format(max_value)).index[-1]
    for i in range(index,len(df)):
        if df.loc[i,'close_price'] <=  df.loc[i,'avg_5']:
            return 0
    return 1
#判断前溯极大值是10日内最大值
def find_max(df):
    len_df = len(df)
    # increase<=-7 跌幅未稳定
    if df.loc[len_df-1,'increase'] <= -7:
        return False
    val = 0
    for i in range(len_df-1,len_df-11,-1):
        val = df.loc[i,'close_price']
        #print('val:',val,df.loc[i-1,'close_price'])
        if val > df.loc[i-1,'close_price']:
            break
    #print('max_val:',df['close_price'][len_df-11:len_df-1].max(),val)
    if df['close_price'][len_df-11:len_df-1].max() > val:
        return False
    else:
        return True
def main(date):
    init_time = datetime.datetime.now()
    #print('init_time:',init_time)
    if date == None:
        date = datetime.datetime.now().strftime('%Y-%m-%d')
    date_time = datetime.datetime.strptime(date,'%Y-%m-%d')
    start_t = (date_time - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    db = pymysql.connect(host="localhost", user="root", password="Zzl08382020", database="stockdb")
    cursor = db.cursor()
    zhangting_df = com_redu2(db,date,delta = 30)
    #print('len4:', len(zhangting_df))
    zhangting_df = zhangting_df.reset_index()
    redu_init = 0
    for i in range(len(zhangting_df)):
        #print('time_flag2:', datetime.datetime.now() - init_time)
        # #print('zhangting_df:',zhangting_df)
        ids = zhangting_df.loc[i,'stock_id']
        stock_name = zhangting_df.loc[i,'stock_name']
        redu_grade = zhangting_df.loc[i,'redu']
        avg_5 = zhangting_df.loc[i,'avg_5']
        redu_5 = 0
        trade_code = re.sub('-', '', date[0:10]) + ids
        if zhangting_df.loc[i,'low_flag'] >= 0:
            sql = "SELECT stock_id,trade_date,open_price,close_price,high_price,low_price,increase  FROM stock_trade_data \
                where trade_date >= '{0}' and trade_date <= '{1}' and  stock_id  = '{2}'".format( start_t, date,
                                                                                                 ids)
            df = get_df_from_db(sql, db)
            #print('df:', df['trade_date'])
            df['avg_5'] = df['close_price'].rolling(5).mean()
            if len(df) < 20:
                continue
            avg_5 = df.loc[len(df) - 1, 'avg_5']
            #连续三日低于5日均线：
            if df.loc[len(df) - 3, 'avg_5'] > df.loc[len(df) - 3, 'close_price'] and df.loc[len(df) - 2, 'avg_5'] > df.loc[len(df) - 2, 'close_price'] :
                redu_5 = redu_grade /10000
            if redu_5 == 0 and df.loc[len(df) - 1, 'avg_5'] >= df.loc[len(df) - 1,'close_price']:
                if find_max(df):#return bool
                    redu_5 = redu_grade
            #print('redu_grade:',redu_grade,redu_5)
            redu_init = com_redu_init(redu_grade, df)
        #print('time_flag4:', datetime.datetime.now() - init_time)
        h_table = ids[-1]
        sql = "insert into com_redu_test(trade_code,trade_date,stock_id,stock_name,redu,redu_5,avg_5,h_table,redu_init) \
            values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}') " \
              "ON DUPLICATE KEY UPDATE trade_code='{0}',trade_date='{1}',stock_id='{2}',stock_name='{3}'," \
              "redu ='{4}',redu_5 ='{5}',avg_5='{6}',h_table = '{7}',redu_init = '{8}'\
            ".format(trade_code, date, ids, stock_name, redu_grade, redu_5, float(avg_5), h_table, redu_init)
        #print('sql:', sql)
        cursor.execute(sql)
        #print('time_flag5:', datetime.datetime.now() - init_time)
    try:
        db.commit()
        print('存储完成')
        logging.info('存储完成')
    except Exception as err:
        db.rollback()
        print('存储失败:', err)
        logging.error('存储失败:{}'.format(err))
    cursor.close()
def run(date):
    p = Pool(8)
    for i in range(1, 11):
        p.apply_async(main, args=(str(i),date,))
    #    p.apply_async(main, args=('1',date,))
    #print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    #print('All subprocesses done.')
def history_com(start_date,end_date):
    db_h = pymysql.connect(host="localhost", user="root", password="Zzl08382020", database="stockdb")
    cursor = db_h.cursor()
    sql = "select distinct(trade_date) from stock_history_trade1 where trade_date >= '{0}' and trade_date <= '{1}'".format(start_date,end_date)
    cursor.execute(sql)
    date_list = cursor.fetchall()
    #print('date_list:',date_list)
    for date_t in date_list:
            date = date_t[0].strftime('%Y-%m-%d')
            #print(date)
    main(date)
def run_h(start_date,end_date):
    p = Pool(8)
    for i in range(1, 11):
        p.apply_async(history_com, args=(str(i),start_date,end_date))
    #    p.apply_async(main, args=('1',date,))
    #print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    #print('All subprocesses done.')
def test_apply():
    db = pymysql.connect(host="localhost", user="root", password="Zzl08382020", database="stockdb")
    cursor = db.cursor()
    sql = "select stock_id,trade_date,open_price,close_price,high_price,low_price,increase from stock_history_trade1 where trade_date >= '2021-01-01' and trade_date <= '2021-03-04'".format()
    init_time = datetime.datetime.now()
    #print('init_time:',init_time)
    df = get_df_from_db(sql, db)
    #print('len:',len(df))
    #print('flag_time1:', datetime.datetime.now() - init_time)
    #test 1
    # for i in range(len(df)):
    #     if df.loc[i,'increase'] >= 9.75:
    #         df.loc[i, 'increase'] = 1
    #     else:
    #         df.loc[i, 'increase'] = 0
    #test2
    a=9.75
    df['increase'] = df['increase'].apply(lambda x: 1 if x >= a else 0)
    #print('flag_time:', datetime.datetime.now() - init_time)
    df = df.groupby('stock_id')['increase'].sum()
    #print('end_time:', datetime.datetime.now() - init_time)
    #print('df:',df)
if __name__ == '__main__':
    date =None#'2021-02-01' #'2021-01-20'
    # run(date)

    # h_tab = '2'
    main( date)

    # history_com(start_date='2020-01-01', end_date='2021-01-19')
    # run_h(start_date='2020-07-31', end_date='2021-02-22')

    # test_apply()
    # main(h_tab = '9', date = date)
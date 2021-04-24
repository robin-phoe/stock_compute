# coding:utf-8
#这是热门小波形的B方案，筛选出热门的个股，监控波动中的小回调再拉升的空间
# 思路是通过比较广的入门条件（热门），最后通过分数和启动条件来把控。（实践中来进一步缩小精确条件筛选）
#热门条件：进20日换手日均大于3%
#分数因素：
#一、5日线：当时值小于5日线为 +分数（固定分数10000）
#二、换手率
#三、左侧走势 60日均线 近20日差为正，且20均线20日差为正（固定分数10000），正：>=0.97
#四、换手率
#五、振荡波形--偏离度 当前价与20日均线偏差值（分数为二次拱形函数）
#*
#目前不包含688

# import tushare as ts
import pandas as pd
import pymysql
import datetime
import logging
import re
from multiprocessing import Pool
import json
import copy
import numpy as np
#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)

logging.basicConfig(level=logging.DEBUG, filename='remen_xiaoboxin_B.log', filemode='w',
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
    # print('df:',df)
    # df['trade_date'] = date2num(df['trade_date'])
    # print('df:', df[['avg_10', 'close_price']])
    return df
def core(df,date):
    df = df.set_index(keys=['trade_date'])

    # df.reset_index(inplace=True)
    # print('df:',df.index)
    df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last', inplace=True)
    # print('df:', df)
    #求5日数据
    df_avg_5 = df.groupby(['stock_id'])['close_price'].rolling(5).mean()
    # print('df_avg_5:',df_avg_5)
    #求下底均线（20日）
    df_avg_low = df.groupby(['stock_id'])['close_price'].rolling(20).mean()
    # print('df_avg_low',df_avg_low)
    #merge 下底均线 & 计算下底线最大偏离度
    df = pd.merge(df, df_avg_low, how='left', on=['stock_id','trade_date'])
    df.rename(columns={'close_price_x':'close_price','close_price_y': 'avg_low'}, inplace=True)
    # df_low.reset_index(inplace=True)
    df['bais'] = abs(df['close_price'] / df['avg_low'] -1)
    df_bais = df.groupby(['stock_id'])['bais'].rolling(20).max()
    df = pd.merge(df, df_bais, how='left', on=['stock_id','trade_date'])
    df.rename(columns={'bais_y': 'bais'}, inplace=True)
    # print('df bais:',df)
    #截取近20日数据
    df.sort_values(axis=0, ascending=False, by='trade_date', na_position='last', inplace=True)
    df = df.groupby('stock_id', as_index=False).head(20)
    #求20日换手率日均值
    df_turnover_20 = df.groupby(['stock_id'], as_index=True)['turnover_rate'].mean()
    # print('df_turnover_20:', df_turnover_20)
    #merge 5日均线
    # print('df.head()1:', df.head())
    df = pd.merge(df, df_avg_5, how='left', on=['stock_id','trade_date'])
    df.rename(columns={'close_price_x':'close_price','close_price_y': 'avg_5'}, inplace=True)

    #删除不是今日的数据行
    # print('df1:', df)
    df.reset_index(inplace=True)
    df.drop(df[df.trade_date < date].index, inplace=True)
    #merge 20日换手
    # print('df.head()2:', df.head())
    df = pd.merge(df, df_turnover_20, how='left', on='stock_id')
    df.rename(columns={'turnover_rate_x':'turnover_rate','turnover_rate_y': 'turnover_20'}, inplace=True)
    print('df:',df.head(20))
    #删除日均换手小于3%
    df.drop(df[df.turnover_20 < 3].index, inplace=True)
    #计算5日均线的参照分数
    df['avg_5_flag'] = (df['avg_5']/df['close_price'] - 0.75) * 10000
    # 计算偏离度分数
    df['avg_low_flag'] =-(df['bais']*100 - 15) * 1000
    #计算分数
    df['grade'] = df['avg_5_flag'] + df['avg_low_flag']
    df.drop(df[np.isnan(df['grade'])].index, inplace=True)
    df.sort_values(axis=0, ascending=False, by='grade', na_position='last', inplace=True)
    df.reset_index(inplace=True)
    # print('df:', df[['stock_id','stock_name','trade_date','grade','avg_5_flag','avg_low_flag','bais']])
    return df
def save(db,df):
    cursor = db.cursor()
    for i in range(len(df)):
        trade_date = str(df.loc[i,'trade_date'])
        stock_id = df.loc[i,'stock_id']
        trade_code = re.sub('-','',trade_date[0:10]) + stock_id
        stock_name = df.loc[i,'stock_name']
        grade = df.loc[i,'grade']
        sql = "insert into remen_xiaoboxin(trade_code,trade_date,stock_id,stock_name,grade) \
            values('{0}','{1}','{2}','{3}','{4}') " \
              "ON DUPLICATE KEY UPDATE trade_code='{0}',trade_date='{1}',stock_id='{2}',stock_name='{3}'," \
              "grade ='{4}' \
            ".format(trade_code,trade_date,stock_id,stock_name,grade)
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
def main(date):
    if date == None:
        date = datetime.datetime.now().strftime('%Y-%m-%d')
    date_time = datetime.datetime.strptime(date, '%Y-%m-%d')
    start_t = (date_time - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    # day_delta = 40
    db = pymysql.connect(host="192.168.1.6", user="user1", password="Zzl08382020", database="stockdb")
    # cursor = db.cursor()
    #test 作为单个账号历史数据测试
    # sql = "select stock_id,stock_name,trade_date,close_price,increase from stock_history_trade{0} " \
    #       "where trade_date <= '{1}' and stock_id not like '688%' " \
    #       "and stock_id = '002407' order by trade_date DESC limit {2} ".format(h_tab,date,day_delta)
    # sql = "select stock_id,stock_name,trade_date,close_price,increase,turnover_rate from stock_history_trade{0} " \
    #       "where trade_date >= '{1}' and trade_date <= '{2}' and stock_id not like '688%' ".format(h_tab,start_t,date)#and stock_id in ('002940','000812')
    # sql = "select stock_id,stock_name,trade_date,close_price,increase,turnover_rate from stock_trade_data " \
    #       "where trade_date >= '{0}' and trade_date <= '{1}' and stock_id not like '688%' ".format(start_t,date)#and stock_id in ('002940','000812')
    sql = "select stock_id,stock_name,trade_date,close_price,increase,turnover_rate from stock_trade_data " \
          "where trade_date >= '{0}' and trade_date <= '{1}' and stock_id not like '688%' ".format(start_t,date)#and stock_id in ('002940','000812')
    time_start = datetime.datetime.now()
    df = get_df_from_db(sql, db)
    time_end = datetime.datetime.now()
    print('df_len:',len(df))
    print('time_delta:',time_end - time_start )
    df = core(df, date)
    save(db, df)
def run(date):
    p = Pool(8)
    for i in range(0, 10):
        p.apply_async(main, args=(date,str(i),))
    #    p.apply_async(main, args=('1',date,))
    #print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    #print('All subprocesses done.')
if __name__ == '__main__':
    date =None#'2021-02-01' #'2021-01-20'
    main(date)
    # run(date)
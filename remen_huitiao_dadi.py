# coding:utf-8
#目标是寻找热门大涨回调到支撑位的回升
#1、过去30日热度和大于3，（涨停计1，龙虎榜计1）#暂时不考虑，可能可increase 作用重复
#2、当前价格低于10线
#3、30日内10日均线正向increase 和>=12%
#4、30日内10日均线负向increase 和>=8%
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
import copy

#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)

logging.basicConfig(level=logging.DEBUG, filename='remen_huitiao_dadi.log', filemode='w',
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
    # df.reset_index(inplace=True)
    cursor.close()
    # print('df:',df)
    # df['trade_date'] = date2num(df['trade_date'])
    # print('df:', df[['avg_10', 'close_price']])
    return df
def core(df,date):
    df = df.set_index(keys=['trade_date'])
    # print('df_init:',df)
    #计算10日均值
    df_group = df.groupby(['stock_id'])['close_price'].rolling(10).mean()
    df = pd.merge(df, df_group, how='left', on=['stock_id','trade_date'])
    df.rename(columns={'close_price_x':'close_price','close_price_y': 'avg_10'}, inplace=True)
    # print('df_group_avg:', df_group)
    #计算30日均值
    df_group = df.groupby(['stock_id'])['close_price'].rolling(30).mean()
    df = pd.merge(df, df_group, how='left', on=['stock_id','trade_date'])
    df.rename(columns={'close_price_x':'close_price','close_price_y': 'avg_30'}, inplace=True)
    # 求30日极值比值
    df_group = df.groupby(['stock_id'])['avg_10'].rolling(30).max() / df.groupby(['stock_id'])['avg_10'].rolling(10).min() - 1
    df = pd.merge(df, df_group, how='left', on=['stock_id', 'trade_date'])
    df.rename(columns={'avg_10_x': 'avg_10', 'avg_10_y': 'avg10_max_min_delta'}, inplace=True)
    # print('df_avg:', df.avg_10)

    # df_group['close_price'].sum()
    # df_group['avg_10'] = df_group['close_price'].rolling(10).mean()

    # df_group = copy.deepcopy(df)
    # df_group['rank'] = df_group['trade_date'].groupby(df_group['stock_id']).rank(method='min',ascending=False)#.rolling(10).mean()
    # #删除30日前数据
    # df_group.drop(df_group[df_group.rank > 30].index, inplace=True)
    # df_group['avg_30'] = df_group['close_price'].mean()
    # df_group['avg10_max_min_delta'] = df_group['avg_10'].rolling(30).max() - df_group['avg_10'].rolling(30).min()
    # #删除10日前数据
    # df_group.drop(df_group[df_group.rank > 10].index, inplace=True)
    # df_group['avg_10'] = df_group['close_price'].mean()


    # print('df_group:',df_group.head())
    # print('df:', df.head())
    df.fillna(0,inplace= True)
    df['lastest_10_delta'] = df['close_price'] - df['avg_10']
    df['lastest_30_delta'] = df['close_price'] - df['avg_30']
    df['increase_10'] = df.groupby(['stock_id'])['avg_10'].pct_change()
    # print("df['increase_10']:",df['increase_10'])
    df['increase_10'][np.isinf(df['increase_10'])] = 0
    # print('increase_10:',df['increase_10'])
    df['increase_10_minus'] = df['increase_10'].apply(lambda x: x if x <=0 else 0)
    df['increase_10_postive'] = df['increase_10'].apply(lambda x: x if x >= 0 else 0)
    print('increase_10_postive:',df['increase_10_postive'])
    # # print('increase_10_minus:', df['increase_10_minus'])
    print('df1:', df)
    df_inc_sum = df.groupby(['stock_id'])['increase_10_postive','increase_10_minus'].rolling(30).sum()
    print('df_inc_sum:',df_inc_sum)
    #删除列防止合并后重复
    del df['increase_10_postive']
    del df['increase_10_minus']
    df = pd.merge(df, df_inc_sum, how='left', on=['stock_id', 'trade_date'])
    df = df.reset_index()
    print('df:', df.head(200))
    #计算10日均线涨跌的bool值（涨1，跌0）
    df['increase_10_up_bool'] = df['increase_10'].apply(lambda x: 1 if x >= 0 else 0)
    print('increase_10_up_bool:',df['increase_10_up_bool'])
    def fun(y):
        # y[0:-30] = 0
        z = y.apply(lambda x: 1 if x == 0 else 1)
        print('fun_y:', y)
        x = z * (y.groupby((y != y.shift()).cumsum()).cumcount() + 1).max()
        print('fun:',x)
        return x
    df['increase_count_day'] = df.groupby('stock_id')['increase_10_up_bool'].apply(fun)
    # test 作为单个账号历史数据测试
    # df_inc_sum = df[['stock_id','increase_10_minus','increase_10_postive']]
    # df_inc_sum = df_inc_sum.groupby('stock_id')['increase_10_minus','increase_10_postive'].rolling(30).sum()
    # # df_inc_sum['increase_10_postive'] = df_inc_sum['increase_10_postive'].rolling(30).sum()
    # print('df_inc_sum:',df_inc_sum)


    print('date_df:',df)
    #删除不是今日的数据行
    df.drop(df[df.trade_date < date].index, inplace=True)
    # print('date_df1:', df)
    #合并df与df_inc_sum

    print('df1:',df[['stock_id','stock_name','trade_date','lastest_10_delta','increase_10_postive','increase_10_minus','avg10_max_min_delta','increase_count_day']])
    logging.info('df1:{}'.format(df))
    #删除最后日期大于10均值行
    df.drop(df[df.lastest_10_delta > 0].index, inplace=True)
    #删除最后日期大于30均值行
    df.drop(df[df.lastest_30_delta > 0].index, inplace=True)
    #删除10日均线极大值极小值差<12%
    df.drop(df[df.avg10_max_min_delta < 0.12].index,inplace=True)
    #删除10日均线正向increase和<12%
    df.drop(df[df.increase_10_postive < 0.12].index,inplace=True)
    # 删除10日均线负向increase和<8%
    df.drop(df[df.increase_10_minus > -0.08].index, inplace=True)
    # 删除10日均线连续增长天数少于8天
    df.drop(df[df.increase_count_day < 12].index, inplace=True)
    print('df2:',df[['stock_id','stock_name','trade_date','lastest_10_delta','increase_10_postive','increase_10_minus','avg10_max_min_delta','increase_count_day']])
    return df
def main(date,h_tab):
    if date == None:
        date = datetime.datetime.now().strftime('%Y-%m-%d')
    date_time = datetime.datetime.strptime(date, '%Y-%m-%d')
    start_t = (date_time - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    # day_delta = 40
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    # cursor = db.cursor()
    #test 作为单个账号历史数据测试
    # sql = "select stock_id,stock_name,trade_date,close_price,increase from stock_history_trade{0} " \
    #       "where trade_date <= '{1}' and stock_id not like '688%' " \
    #       "and stock_id = '002407' order by trade_date DESC limit {2} ".format(h_tab,date,day_delta)
    sql = "select stock_id,stock_name,trade_date,close_price,increase from stock_history_trade{0} " \
          "where trade_date >= '{1}' and trade_date <= '{2}' and stock_id not like '688%' ".format(h_tab,start_t,date)#and stock_id in ('002940','000812')
    df = get_df_from_db(sql, db)
    # print('df:',df)
    df = core(df, date)
if __name__ == '__main__':
    date ='2021-03-11'#'2021-02-01' #'2021-01-20'
    main(date, h_tab = '1')
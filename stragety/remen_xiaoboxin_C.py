# coding:utf-8
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
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config
import pub_uti


#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)

logging.basicConfig(level=logging.DEBUG, filename='../log/remen_xiaoboxin_B.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
class stock:
    def __init__(self,id,name,ponit_json,close_price):
        pass

'''
计算最后日情况
一期：计算最后低点后3%以内情况
二期：增加高点3个点一下回调情况（前期高热）
三期：增加点后多日形态筛选
四期：分离尾盘入场类（圆滑跌后收稳）
'''
class core:
    def __init__(self,date = None):
        self.df = self.select_df()
        if date == None:
            sql = "select max(date) from stock_trade_data"
            self.date = pub_uti.select_from_db(sql=sql)[0][0]
    def select_df(self):
        sql = "SELECT * FROM boxin_data B " \
              " LEFT JOIN " \
              " (select stock_id,stock_name,close_price from stock_trade_data where trade_date = '{0}') T " \
              " ON B.stock_id = T.stock_id ".format(self.date)
        self.df = pub_uti.creat_df(sql=sql)
        return self.df
    def run_apply(self):
        self.df['boxin_json'] = ''
        self.df['grade'] = ''
        self.df = self.df.apply(fun_main,asxi = 1)
    #apply函数汇总入口
    def fun_main(self,raw):
        point_json = raw['boxin_json'] = json.loads(raw['boxin_list'])
        if not self.jugement_last_point(point_json):
            raw['grade'] =
            return raw

    #判断最后点是否为低点
    def jugement_last_point(self,point_json):
        #取最后一组tuple
        point_tuple = point_json[0]
        first_point = point_tuple[0][0]
        second_point = point_tuple[1][0]
        if first_point > second_point:
            return True
        else:
            return False



'''
计算历史指定日期情况（用于验证）
'''
#判断凹谷左侧
#单日increase<3%
#累计下跌幅度>-6%
def estimate_modality(df):
    def com_inc(row):
        sum_depreciate = 0
        day_count = 0
        for i in range(0,16):
            field_name = 'increase'+str(i)
            # 当日increase
            if i == 0:
                field_name = 'increase'
            value = row[field_name]
            if value >= 3:
                break
            sum_depreciate += value
            day_count = i
        if day_count >= 3 and sum_depreciate < -6:
            row['modality_grade'] = 1
        else:
            row['modality_grade'] = 0
        return row
    df = df.apply(com_inc,axis=1)
    return df
#shift近15日increase
def deal_df_shift(df):
    for i in range(1,16):
        df['increase'+str(i)] = df.groupby(['stock_id'])['increase'].shift(i)
        df['increase'+str(i)].fillna(100,inplace=True)
    df['modality_grade'] = -1
    return df
def core(df,date):
    df = deal_df_shift(df)
    print('df col1:',df.columns)
    df = df.set_index(keys=['trade_date'])
    df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last', inplace=True)
    #求5日数据
    df_avg_5 = df.groupby(['stock_id'])['close_price'].rolling(5).mean()
    #求下底均线（20日）
    df_avg_low = df.groupby(['stock_id'])['close_price'].rolling(20).mean()
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
    #计算凹谷（左侧）特征
    print('df col2:', df.columns)
    df = estimate_modality(df)
    #删除凹谷特征不符合的行（0）
    # print('df_aogu:', df[['stock_id', 'stock_name', 'trade_date', 'modality_grade']])
    df.drop(df[df.modality_grade == 0].index, inplace=True)
    #计算5日均线的参照分数
    df['avg_5_flag'] = (df['avg_5']/df['close_price'] - 0.75) * 10000
    # 计算偏离度分数
    df['avg_low_flag'] =-(df['bais']*100 - 15) * 1000
    #计算分数
    df['grade'] = df['avg_5_flag'] + df['avg_low_flag']
    df.drop(df[np.isnan(df['grade'])].index, inplace=True)
    df.sort_values(axis=0, ascending=False, by='grade', na_position='last', inplace=True)
    df.reset_index(inplace=True)
    print('df result:', df[['stock_id','stock_name','trade_date','grade','avg_5_flag','avg_low_flag','bais']])
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
    db_config = read_config('db_config')
    print('db_config:',db_config)
    db = pymysql.connect(host=db_config["host"], user=db_config["user"], password=db_config["password"], database=db_config["database"])
    # db = pymysql.connect(host="192.168.1.6", user="user1", password="Zzl08382020", database="stockdb")
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
          "where stock_id not like '688%' and stock_id not like '300%' and trade_date >= '{0}' and trade_date <= '{1}' " \
          "and stock_name not like 'ST%' and stock_name not like '%ST%' ".format(start_t,date)#and stock_id in ('002940','000812')
    time_start = datetime.datetime.now()
    df = get_df_from_db(sql, db)
    time_end = datetime.datetime.now()
    print('df_len:',len(df))
    print('time_delta:',time_end - time_start )
    df = core(df, date)
    save(db, df)
#计算历史日期段结果
def history(start_date,end_date):
    db_config = read_config('db_config')
    db = pymysql.connect(host=db_config["host"], user=db_config["user"], password=db_config["password"], database=db_config["database"])
    cursor = db.cursor()
    sql = "select date_format(trade_date,'%Y-%m-%d') as trade_date from stock_trade_data where trade_date >= '{0}' and trade_date <= '{1}' ".format(start_date,end_date)
    cursor.execute(sql)  # 执行SQL语句
    date_tuple = cursor.fetchall()
    print('date_tuple:',date_tuple)
    cursor.close()
    p = Pool(4)
    for i in range(0, len(date_tuple)):
        date = date_tuple[i][0]
        print('date:',date)
        p.apply_async(main, args=(date,))
    #    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
if __name__ == '__main__':
    date =None#'2021-02-01' #'2021-01-20'
    main(date)
    # history('2020-01-01','2021-05-06')
# coding:utf-8
import pandas as pd
import pymysql
from matplotlib.pylab import date2num
import numpy as np
import datetime
import logging
import re
from multiprocessing import Pool
import json
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
import pub_uti_a
pd.set_option('display.max_columns', None)
logging.basicConfig(level=logging.DEBUG, filename='../log/comp_zhaung.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

class stock:
    def __init__(self,single_df):
        self.df = single_df
        self.clean_df()
        self.zhuang_grade = 0
        self.zhuang_long = 0
        self.max_avg_rate = 0
        self.lasheng_flag = 0
        self.piece = 45
        self.yidong = []
        self.zhuang_date = []
        self.lastest_target = '1971-01-01'
        self.start_day = None
        self.end_day =None
    def run(self):
        #验证数据长度
        if len(self.df) <= 100:
            print('少于100条记录')
            return 0
        self.clean_df()
    def clean_df(self):
        self.df.fillna(0, inplace=True)
        self.df['trade_date2'] = self.df['trade_date'].copy()
        self.df['trade_date2'] = pd.to_datetime(self.df['trade_date2']).map(date2num)
        self.df['dates'] = np.arange(0, len(self.df))
        self.df['arv_10'] = self.df['close_price'].rolling(10).mean()
        self.df['arv_5'] = self.df['close_price'].rolling(5).mean()
        self.df['increase_flag'] = 0
        self.df['increase_abs'] = 0
        self.df['piece_flag_sum'] = self.df.increase_flag.rolling(self.piece).sum()
        self.df['increase_abs_sum'] = self.df.increase_flag.rolling(self.piece).sum()
        self.start_day = self.end_day = self.df.loc[len(self.df) - 1, 'trade_date']
        for i in range(1, len(self.df) - 1):
            # 涨幅绝对值
            self.df.loc[i, 'increase_abs'] = abs(float(self.df.loc[i, 'increase']))
            # DB中历史老数据缺失increase
            self.df.loc[i, 'increase'] = (self.df.loc[i, 'close_price'] - self.df.loc[i - 1, 'close_price']) / self.df.loc[
                i - 1, 'close_price'] * 100
            if -2 <= float(self.df.loc[i, 'increase']) <= 2:
                self.df.loc[i, 'increase_flag'] = 1
        print('self.df:', self.df[['increase', 'increase_flag']])
    def
class stock_buffer:
    def __init__(self,num,start_t=None,end_t= None):
        self.num = str(num)
        self.start_t = start_t
        self.end_t = end_t
        self.id_set = set()
    def select_info(self):
        if start_t != None and end_t != None:
            sql = "SELECT stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price,increase  FROM stock_trade_data \
                    where trade_date >= '{0}' and trade_date <= '{1}' and stock_id like '%{2}'".format(start_t, end_t,self.num)
        else:
            sql = "SELECT stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price,increase  " \
                  "FROM stock_trade_data where stock_id like '%{0}' ".format(self.num)
        df = pub_uti_a.creat_df(sql, ascending=True)
        self.id_set = set(df['stock_id'].to_list())
    def init_stock(self, id):
        single_df = self.df.loc[self.trade_df.stock_id == id]
        single_df.reset_index(inplace=True)
        # if len(single_df) < 20 :
        #     return
        stock_name = single_df.loc[0, 'stock_name']
        self.stock_buffer[id] = stock_object = stock(id, self.date, single_df)
        res = stock_object.compute()
        if not res:
            print('本条退出。')
            return
if __name__ == '__main__':
    start_t = None#'2020-01-01'
    end_t = None#'2021-01-14'
    start_time = datetime.datetime.now()

    # run(start_t, end_t)
    # com_lastest_point()
    com_volume_signal()
    print('耗时:', datetime.datetime.now() - start_time)
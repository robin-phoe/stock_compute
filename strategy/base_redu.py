# coding:utf-8
# import tushare as ts
import pandas as pd
import pymysql
import datetime
import logging
import re
from multiprocessing import Pool
from itertools import chain
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

'''

'''

class stock_remdu:
    def __init__(self,date = None):
        self.stock_buffer = {}
        self.trade_df = ''
        self.longhu = ''
        self.df = ''
        self.date = date
        self.sql_range_day = 60
        self.sql_start_date = ''#'2021-06-10'
        self.save = ''
        #trade_data区间开始的时间
    def run(self):
        self.creat_time()
        self.select_trade_info()
        self.select_longhu_info()
        self.merge_df()
        # print(self.df)
        pass
    def creat_time(self):
        if self.date == None:
            sql = "select DATE_FORMAT(max(trade_date),'%Y-%m-%d') as last_date from stock_trade_data "
            self.date = pub_uti.select_from_db(sql=sql)[0][0]
        self.sql_start_date = (datetime.datetime.strptime(self.date,'%Y-%m-%d') -
                               datetime.timedelta(days= self.sql_range_day)).strftime('%Y-%m-%d')
    def select_trade_info(self):
        trade_sql = "select stock_id,stock_name,high_price,low_price,open_price,close_price,trade_date,wave_data,point_type,turnover_rate,increase " \
                    " FROM stock_trade_data " \
                    "where trade_date >= '{0}' and trade_date <= '{1}' " \
                    " AND stock_id not like '300%' AND stock_id not like '688%' " \
                    " AND stock_name not like 'ST%' AND stock_name not like '*ST%' ".format(self.sql_start_date,self.date)

        print('trade_sql:{}'.format(trade_sql))
        self.trade_df = pub_uti.creat_df(sql=trade_sql)
        self.trade_df.fillna('',inplace=True)
        #标价涨停
        self.trade_df['limit_flag'] = self.trade_df['increase'].apply(lambda x: 1 if x >=9.75 else 0)
        self.trade_df = self.trade_df.groupby(['stock_id','stock_name'],as_index= False)['limit_flag'].sum()
        # print(self.trade_df)
    def select_longhu_info(self):
        longhu_sql = "select trade_date,stock_id from longhu_info where trade_date >= '{0}' and trade_date <= '{1}'".format(self.sql_start_date,self.date)
        self.longhu = pub_uti.creat_df(sql=longhu_sql)
        self.longhu['count_longhu'] = 1
        self.longhu = self.longhu.groupby('stock_id',as_index= False)['count_longhu'].sum()
        print(self.longhu)
    def merge_df(self):
        self.df = pd.merge(self.trade_df,self.longhu,on='stock_id',how='left')
        file_name = './validate_report/redu_base.xlsx'
        writer = pd.ExcelWriter(file_name)
        self.df.to_excel(writer, encoding='utf_8_sig', index=False)
        writer.save()




'''
计算历史指定日期情况（用于验证）
'''
class save_result:
    def __init__(self):
        self.csv_name = './validate_report/redu_base.xlsx'
        self.df = None
        self.df = pd.read_excel(self.csv_name,dtype={'stock_id':str})
        print('df:',self.df.head(100))
    def save(self):
        pub_uti.df_to_mysql('base_redu',self.df)


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    date =None#'2021-01-20'
    # st_buff = stock_remdu()
    # st_buff.run()
    sr = save_result()
    sr.save()
    # history(start_date= '2021-01-01', end_date= '2021-07-02')
    # print(datetime.datetime.now() - start_time)
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


class stock:
    def __init__(self,id,date,single_df):
        self.id = id
        self.single_df = single_df #时间倒序
        self.date = date
        self.grade = 0 #20000+ 表示尾盘可以进入。10000+，
        self.low_standard = 1.03
        self.after_inc = 0
        self.before_inc = 0
        self.after_inc_abs = 0
        self.before_inc_abs = 0
        self.limit_up_flag = False
        # self.after_days = 10
        self.before_days = 10
        self.trade_code = re.sub('-', '', self.date) + self.id
        self.single_limit = 0
        self.after_day = 0
        self.last_price = 0
        self.stop = False
    def compute(self):
        if not self.com_inc():
            return
        self.count_limit_up()
        self.com_last_price()
        self.com_grade()

    def com_grade(self):
        #万：(20000:1个涨停，10000：2个连续涨停)
        #千：(2000:涨停第二日开收盘价低于前日涨停价格，1000：开收盘价格低于3%，else：0)
        #百：(abs(int(after_inc))*100)
        print('前斜率：{}{}'.format(self.before_inc,self.before_inc_abs))
        if self.before_inc > 5 or self.before_inc < -5 or self.before_inc_abs > 15:
            return
        if self.after_inc > 3:
            return
        self.grade = self.single_limit + self.after_day + abs(int(self.after_inc))*100 + self.last_price
    def com_inc(self):
        count = 0
        for i in range(len(self.single_df)):
            #判断在涨停后区间
            if not self.limit_up_flag:
                if self.single_df.loc[i,'flag'] == 1 :
                    # 涨停在最后一日，退出
                    if i == 0:
                        self.stop = True
                        return False
                    #比较后一日价格与涨停日收盘价
                    self.com_price(i)
                    self.limit_up_flag = True
                    continue
                #期间一日涨幅大于5，退出
                if self.single_df.loc[i,'increase'] >= 5:
                    self.stop = True
                    return False
                self.after_inc += self.single_df.loc[i,'increase']
                self.after_inc_abs += abs(self.single_df.loc[i, 'increase'])
            else:
                if count >= self.before_days:
                    break
                self.before_inc += self.single_df.loc[i,'increase']
                self.before_inc_abs += abs(self.single_df.loc[i, 'increase'])
                count += 1
        return True
    #计算十日内有几个涨停
    def count_limit_up(self):
        limit_up_df = self.single_df.head(10)
        flag_index_list = limit_up_df[limit_up_df.flag == 1].index.to_list()
        if len(flag_index_list) == 1:
            self.single_limit = 15000
        else:
            if abs(flag_index_list[0] - flag_index_list[1]) == 1:
                self.single_limit = 10000
            else:
                self.single_limit = 15000
    #涨停第二日开收盘价与前日涨停价格比照
    def com_price(self,i):
        limit_c_price = self.single_df.loc[i,'close_price']
        #第二日开收盘价都低于涨停日收盘价
        if self.single_df.loc[i-1,'close_price'] <= limit_c_price and self.single_df.loc[i-1,'open_price'] <= limit_c_price:
            self.after_day = 5000
        #第二日开收盘价都低于涨停日收盘价*1.03
        elif self.single_df.loc[i-1,'close_price'] <= limit_c_price*1.03 and self.single_df.loc[i-1,'open_price'] <= limit_c_price*1.03:
            self.after_day = 1000
    #判断是否企稳
    def com_last_price(self):
        #涨停后一日企稳不在范围内
        if self.single_df.loc[1,'flag'] == 1 :
            return
        if -1 <= self.single_df.loc[1,'increase'] <= 3:
            self.last_price = 10000

class stock_buffer:
    def __init__(self,date = None):
        self.stock_buffer = {}
        self.trade_df = ''
        self.date = date
        self.sql_range_day = 50
        self.sql_start_date = ''#'2021-06-10'
        self.id_set = set()
        self.save = ''
        #trade_data区间开始的时间
    def init_buffer(self):
        self.creat_time()
        self.clean_tab()
        self.select_info()
        self.save = pub_uti.save()
        for id in self.id_set:
            self.init_stock(id)
        self.save.commit()
    def creat_time(self):
        if self.date == None:
            sql = "select DATE_FORMAT(max(trade_date),'%Y-%m-%d') as last_date from stock_trade_data "
            self.date = pub_uti.select_from_db(sql=sql)[0][0]
        self.sql_start_date = (datetime.datetime.strptime(self.date,'%Y-%m-%d') -
                               datetime.timedelta(days= self.sql_range_day)).strftime('%Y-%m-%d')
    def clean_tab(self):
        sql = "delete from limit_up_single where trade_date = '{}'".format(self.date)
        pub_uti.commit_to_db(sql)
    def select_info(self):
        trade_sql = "select stock_id,stock_name,high_price,low_price,open_price,close_price,trade_date,increase " \
                    " FROM stock_trade_data " \
                    "where trade_date >= '{0}' and trade_date <= '{1}' " \
                    "AND stock_id NOT LIKE 'ST%' AND stock_id NOT LIKE '%ST%' " \
                    "AND stock_id NOT like '300%' AND  stock_id NOT like '688%'".format(self.sql_start_date,self.date)
        print('trade_sql:{}'.format(trade_sql))
        self.trade_df = pub_uti.creat_df(sql=trade_sql)
        self.trade_df.fillna('',inplace=True)
        self.id_set = set(self.trade_df['stock_id'].tolist())
        #test
        # self.id_set = ('603035','603036')
        # print(self.df.columns)
    def init_stock(self,id):
        single_df = self.trade_df.loc[self.trade_df.stock_id == id]
        single_df.reset_index(inplace=True)
        if len(single_df) < 30 :
            return
        # single_df = single_df.head(10)
        # i1 = single_df['increase']
        single_df['flag'] = 0
        single_df['flag'] = single_df['increase'].apply(lambda x: 1 if x>=9.75 else 0)
        # i2 = single_df['increase']
        flag_list = single_df['flag'].to_list()[0:10]
        if sum(flag_list) > 2 or sum(flag_list) == 0:
            return
        stock_name = single_df.loc[0,'stock_name']
        self.stock_buffer[id] = stock_object = stock(id,self.date,single_df)
        stock_object.compute()
        sql = "insert into limit_up_single(trade_code,stock_id,stock_name,trade_date,grade) " \
              "values('{0}','{1}','{2}','{3}','{4}') " \
              "ON DUPLICATE KEY UPDATE trade_code='{0}',stock_id='{1}',stock_name='{2}',trade_date='{3}',grade='{4}' " \
              "".format(stock_object.trade_code,id,stock_name,self.date,stock_object.grade)
        print(stock_name,id, stock_object.grade)
        self.save.add_sql(sql)
    def get_stock(self,id):
        pass


'''

计算历史指定日期情况（用于验证）
'''
def history(start_date,end_date):
    sql = "select distinct date_format(trade_date ,'%Y-%m-%d') as trade_date from stock_trade_data where trade_date>= '{}' and trade_date <= '{}'".format(start_date,end_date)
    date_tuple = pub_uti.select_from_db(sql=sql) #(('2021-06-14',),('2021-06-15',))
    date_list = list(chain.from_iterable(date_tuple))
    p = Pool(8)
    for i in range(0, len(date_list)):
        st_buff = stock_buffer(date_list[i])
        p.apply_async(st_buff.init_buffer)
    #    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')


if __name__ == '__main__':
    date =None#'2021-02-01' #'2021-01-20'
    st_buff = stock_buffer(date)
    st_buff.init_buffer()
    # history(start_date= '2021-01-01', end_date= '2021-06-30')
    print('completed.')
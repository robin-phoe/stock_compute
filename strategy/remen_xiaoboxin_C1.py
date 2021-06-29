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
计算最后日情况
一期：计算最后低点后3%以内情况
二期：增加高点3个点一下回调情况（前期高热）
三期：增加点后多日形态筛选
四期：分离尾盘入场类（圆滑跌后收稳）
'''
class stock:
    def __init__(self,id,date,single_df):
        self.id = id
        self.single_df = single_df #时间倒序
        self.date = date
        self.grade = 0
        self.point_tuple = ()
        self.low_standard = 1.03
        self.wave_long = 35
        self.range_day = 3
        self.last_point_index = ''
        self.last_point_value = 0
        self.trade_code = re.sub('-', '', self.date) + self.id

    def compute(self):
        if not self.jugement_last_point():
            return
        if not self.jugement_increase_after_point():
            return
        if not self.jugement_wave_acount():
            return
        if not self.jugement_long():
            return
        self.grade = 10001
    # 判断最后点是否为低点 & 点是否在 时间范围内
    def jugement_last_point(self):
        # 取最后一组tuple
        if len(self.single_df) <= self.range_day:
            return False
        for i in range(self.range_day):
            point_type = self.single_df.loc[i,'point_type']
            if point_type == 'l':
                self.last_point_index = i
                self.last_point_value = self.single_df.loc[i,'wave_data']
                return True
            elif point_type == 'h':
                return False
        return False
    #判断在低点3%以内
    def jugement_increase_after_point(self):
        price_rate = self.single_df.loc[0,'close_price'] / self.last_point_value #长下影线会被排除
        if price_rate < self.low_standard:
            print('在低点3%以内')
            return True
        else:
            print('不在低点3%以内！')
            return False
    #判断20个交易日内应该有三个以上的tuple（1.5个组波形）
    def jugement_wave_acount(self):
        if len(self.single_df) < 20:
            print('point_json长度小于4')
            return False
        type_list = self.single_df['point_type'][0:20].tolist()
        l_count = type_list.count('l')
        h_count = type_list.count('h')
        if l_count >= 3 or h_count >= 3:
            print('波形符合')
            return True
        else:
            return False
    #判断单个段长度超过8个交易日
    def jugement_long(self):
        type_list = self.single_df['point_type'][0:20].tolist()
        count = 0
        for flag in type_list:
            # print('flag:',flag)
            if flag == '':
                count += 1
                # print('count:',count)
                if count > 8:
                    return False
            else:
                count = 0
        return True

class stock_buffer:
    def __init__(self,date = None):
        self.stock_buffer = {}
        self.trade_df = ''
        self.date = date
        self.sql_range_day = 90
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
        sql = "delete from remen_xiaoboxin_c where trade_date = '{}'".format(self.date)
        pub_uti.commit_to_db(sql)
    def select_info(self):
        trade_sql = "select stock_id,stock_name,high_price,low_price,close_price,trade_date,wave_data,point_type " \
                    " FROM stock_trade_data " \
                    "where trade_date >= '{0}' and trade_date <= '{1}' ".format(self.sql_start_date,self.date)
        print('trade_sql:{}'.format(trade_sql))
        self.trade_df = pub_uti.creat_df(sql=trade_sql)
        self.trade_df.fillna('',inplace=True)
        self.id_set = set(self.trade_df['stock_id'].tolist())
        # print(self.df.columns)
    def init_stock(self,id):
        single_df = self.trade_df.loc[self.trade_df.stock_id == id]
        single_df.reset_index(inplace=True)
        if len(single_df) < 20 :
            return
        stock_name = single_df.loc[0,'stock_name']
        self.stock_buffer[id] = stock_object = stock(id,self.date,single_df)
        stock_object.compute()
        sql = "insert into remen_xiaoboxin_c(trade_code,stock_id,stock_name,trade_date,grade) " \
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
    date ='2021-06-29'#None#'2021-02-01' #'2021-01-20'
    st_buff = stock_buffer(date)
    st_buff.init_buffer()
    # history(start_date= '2021-01-01', end_date= '2021-06-14')
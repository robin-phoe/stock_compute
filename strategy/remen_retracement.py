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
        self.l_index_list = []
        self.h_index_list = []
        self.last_inc = 0
        self.last_price_del = 0
        self.inc_range = ()
        self.inc_garde = 0
        self.retra_grade = 0
        self.stabilize_grade = 0
        self.turnover_grade = 0
    def compute(self):
        if not self.jugement_lastday_inc():
            return False
        if not self.jugement_last_inc():
            return False
        if not self.jugement_turnover():
            return False
        if not self.jugement_retracement():
            return False
        if not self.jugement_increase_after_point():
            return False
        self.grade = 10001 + self.inc_garde + self.turnover_grade + self.stabilize_grade + self.retra_grade
        print('总分：{4} ,inc_garde：{0}，turnover_grade：{1}，stabilize_grade：{2}，retra_grade：{3}'.format(self.inc_garde,
                                                                                           self.turnover_grade ,
                                                                                           self.stabilize_grade ,
                                                                                           self.retra_grade,
                                                                                            self.id))
        logging.info('总分：{4} ,inc_garde：{0}，turnover_grade：{1}，stabilize_grade：{2}，retra_grade：{3}'.format(self.inc_garde,
                                                                                           self.turnover_grade ,
                                                                                           self.stabilize_grade ,
                                                                                           self.retra_grade,
                                                                                            self.id))
        return True
    #判断最后一日涨幅
    def jugement_lastday_inc(self):
        if self.single_df.loc[0,'increase'] >= 5:
            print('最后日涨幅大于5%。')
            return False
    # 判断热门(涨幅)
    def jugement_last_inc(self):
        def compare_price(first_p,second_p):
            if first_p <= second_p:
                return (first_p, second_p)
            else:
                return (second_p, first_p)
        self.l_index_list = self.single_df[self.single_df.point_type == 'l'].index.to_list()
        self.h_index_list = self.single_df[self.single_df.point_type == 'h'].index.to_list()
        if len(self.l_index_list) ==0 or len(self.h_index_list) ==0 :
            return
        if len(self.l_index_list) >= 2 and self.l_index_list[0] < self.h_index_list[0] :
            self.min_price = compare_price(self.single_df.loc[self.l_index_list[1],'close_price'],
                                           self.single_df.loc[self.l_index_list[1],'open_price'])[0]
            self.inc_range = (self.l_index_list[1],self.h_index_list[0])
        else:
            self.min_price = compare_price(self.single_df.loc[self.l_index_list[0], 'close_price'],
                                           self.single_df.loc[self.l_index_list[0], 'open_price'])[0]
            self.inc_range = ( self.l_index_list[0],self.h_index_list[0])
        self.max_price = compare_price(self.single_df.loc[self.h_index_list[0], 'close_price'],
                                           self.single_df.loc[self.h_index_list[0], 'open_price'])[1]
        self.last_inc = self.max_price / self.min_price - 1
        print('last_inc:',self.last_inc)
        if self.last_inc < 0.2:
            return False
        #日均涨幅
        per_inc = ((self.max_price - self.min_price)/self.min_price) / (self.inc_range[0] - self.inc_range[1])
        if per_inc >= 0.08:
            self.inc_garde = 2000
        elif per_inc >=0.06:
            self.inc_garde = 1000
        elif per_inc <= 0.05:
            self.inc_garde = -1000
        elif per_inc <= 0.03:
            self.inc_garde = -2000
        #涨停分数
        limit_up_list = self.single_df['limit_flag'].to_list()[self.inc_range[1] : self.inc_range[0]]
        limit_count = sum(limit_up_list)
        if limit_count > 3 :
            self.inc_garde += 3000
        elif limit_count >= 2 :
            self.inc_garde += 2000
        elif limit_count > 0 :
            self.inc_garde += 1000
        #涨幅分数
        if self.last_inc >= 0.3:
            self.inc_garde += 1000
        if self.inc_garde < 1000:
            return False
        return True
    # 判断热门(换手率)
    def jugement_turnover(self):
        # print('id:',self.id)
        turnover_rate_list = self.single_df['turnover_rate'].to_list()
        # print('id:', self.id ,self.inc_range )
        turnover_rate_sum = sum(turnover_rate_list[self.inc_range[1]:self.inc_range[0]])
        per_rate = turnover_rate_sum / (self.inc_range[0] - self.inc_range[1])
        print('turnover:', per_rate)
        #发现603633上升前平缓段被纳入上升区间，日均换手被拉低。暂时放弃拉升区间日均换手限定
        # if per_rate < 3:
        #     return False
        #热门分数(回撤部分换手)
        turnover_sum = sum(turnover_rate_list[0:self.inc_range[1]+1])
        if turnover_sum >= 30:
            self.turnover_grade = 3000
        return True
    #判断回撤幅度
    def jugement_retracement(self):
        #高点是今天退出
        if self.h_index_list[0] == 0:
            return False
        if self.l_index_list[0] < self.h_index_list[0]:
            self.retra = (self.max_price - self.single_df.loc[self.l_index_list[0],'low_price'])/(self.max_price - self.min_price)
        else:
            self.retra = (self.max_price - self.single_df.loc[0,'low_price']) / (self.max_price - self.min_price)
        print('retracment:',self.retra)
        if self.retra < 0.2:
            return False
        #判断最后日期收稳
        if self.retra >= 0.4:
            self.retra_grade = 1000
        return True
    #判断在低点3%以内
    def jugement_increase_after_point(self):
        #判断最后日期收稳
        if 3 >= self.single_df.loc[0,'increase'] >= -1.5:
            self.stabilize_grade = 10000
        if self.l_index_list[0] > self.h_index_list[0] or self.l_index_list[0] == 0:
            return True
        self.last_point_value = self.single_df.loc[self.l_index_list[0],'close_price']
        price_rate = self.single_df.loc[0,'close_price'] / self.last_point_value #长下影线会被排除
        if price_rate > self.low_standard:
            print('不在低点3%以内！')
            return False
        inc_list = self.single_df['increase'].to_list()[0:self.l_index_list[0]]
        #判断距离低点的距离
        if self.l_index_list[0] >= 3:
            self.stabilize_grade = 0
        inc_count = 0
        for inc in inc_list:
            inc_count += abs(inc)
        if inc_count/self.l_index_list[0] > 1.5:
            return False
        return True



class stock_buffer:
    def __init__(self,date = None):
        self.stock_buffer = {}
        self.trade_df = ''
        self.date = date
        self.sql_range_day = 120
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
        sql = "delete from remen_retracement where trade_date = '{}'".format(self.date)
        pub_uti.commit_to_db(sql)
    def select_info(self):
        trade_sql = "select stock_id,stock_name,high_price,low_price,open_price,close_price,trade_date,wave_data,point_type,turnover_rate,increase " \
                    " FROM stock_trade_data " \
                    "where trade_date >= '{0}' and trade_date <= '{1}' " \
                    " AND stock_id not like '300%' AND stock_id not like '688%' " \
                    " AND stock_name not like 'ST%' AND stock_name not like '*ST%' ".format(self.sql_start_date,self.date)

        # trade_sql = "select stock_id,stock_name,high_price,low_price,open_price,close_price,trade_date,wave_data,point_type,turnover_rate,increase " \
        #             " FROM stock_trade_data " \
        #             "where trade_date >= '{0}' and trade_date <= '{1}' " \
        #             " and stock_id = '601999' ".format(self.sql_start_date,self.date)

        print('trade_sql:{}'.format(trade_sql))
        self.trade_df = pub_uti.creat_df(sql=trade_sql)
        self.trade_df.fillna('',inplace=True)
        #标价涨停
        self.trade_df['limit_flag'] = self.trade_df['increase'].apply(lambda x: 1 if x >=9.75 else 0)
        self.id_set = set(self.trade_df['stock_id'].tolist())
        # print(self.df.columns)
    def init_stock(self,id):
        single_df = self.trade_df.loc[self.trade_df.stock_id == id]
        single_df.reset_index(inplace=True)
        # if len(single_df) < 20 :
        #     return
        stock_name = single_df.loc[0,'stock_name']
        self.stock_buffer[id] = stock_object = stock(id,self.date,single_df)
        res = stock_object.compute()
        if not res:
            print('本条退出。')
            return
        sql = "insert into remen_retracement(trade_code,stock_id,stock_name,trade_date,grade) " \
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
    start_time = datetime.datetime.now()
    date =None#'2021-01-20'
    st_buff = stock_buffer(date)
    st_buff.init_buffer()
    # history(start_date= '2021-01-01', end_date= '2021-06-24')
    print(datetime.datetime.now() - start_time)
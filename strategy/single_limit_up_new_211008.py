# coding:utf-8
'''
# 211008 优化版
将波形细分为【标准型、双涨停型、波型、低V反弹型】四种分流计算，以应对不同波型的注重方面。
注意点：忌低开上涨进入，低开大概率信心未稳。

'''

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
import pub_uti_a


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
        self.grade = 0 #20000+ 表示上涨即可进入，10000+ 表示优秀
        self.limit_type = None #低V反弹 v_rebound；波型 wave；标准型 standard；双涨停 double_limit；
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
        self.gap_inc_rate = 1.3
        self.fall_vol_rate =None
        self.fall_slope =None
        self.inc_day_count =None
        self.inc_sum =None
        self.standard_amplitude =None
        self.extreme_amplitude =None
        self.lastest_limit_index = None
    '''
    区分单涨停类型，低V反弹 v_rebound；波型 wave；标准型 standard；双涨停 double_limit；
    '''
    def distinguish_type(self):
        self.lastest_limit_index = self.single_df[self.single_df.flag == 1].index.to_list()[0]
        #判断连续双涨停
        if sum(self.single_df['flag'][0:10].to_list()) ==2:
            limit_list = self.single_df[self.single_df.flag == 1].index.to_list()
            if limit_list[1] -limit_list[0] == 1:
                self.limit_type = 'double_limit'
                self.com_double_limit_grade()
                return
        #判断低V反弹
        down_rate = 0
        l_index_list = self.single_df[self.single_df.point_type == 'l'].index.to_list()
        h_index_list = self.single_df[self.single_df.point_type == 'h'].index.to_list()
        l_vol_list = self.single_df[self.single_df.point_type == 'l'].low_price.to_list()
        h_vol_list = self.single_df[self.single_df.point_type == 'h'].high_price.to_list()
        if len(l_index_list) !=0 and len(h_index_list) !=0:
            if min(l_index_list) < min(h_index_list):
                down_rate = (h_vol_list[0] / l_vol_list[0] -1)*100
        if down_rate >= 15:
            self.limit_type = 'v_rebound'
            return
        #判断波型
        limit_open_price = self.single_df[self.single_df.flag == 1].open_price.to_list()[0]
        lastest_close_price = self.single_df.loc[0,'close_price']
        delta_rate = (lastest_close_price / limit_open_price - 1) * 100
        if delta_rate <= 3:
            self.limit_type = 'wave'
            return
        #剩余是标准型
        self.limit_type = 'standard'
    '''
    计算双涨停型分数
    '''
    def com_double_limit_grade(self):
        double_multiple = 150  #内函数总分68 * 15 =10000
        grade = 0
        #第三日无实线、上影线冒高 bool
        lastest_limit_c_price = self.single_df.loc[self.lastest_limit_index,'close_price']
        three_h_price = self.single_df.loc[self.lastest_limit_index-1,'high_price']
        delta_rate = (three_h_price / lastest_limit_c_price - 1) *100
        if delta_rate >= 1.5:
            self.grade = 0
            return
        #第三日回撤幅度 40
        three_inc = self.single_df.loc[self.lastest_limit_index - 1, 'increase']
        grade +=  (-three_inc * 5) if (-three_inc * 5)<40 else 40
        #整体回撤情况 30
        self.com_fall_data()
        grade +=(1-1/((self.fall_vol_rate-4) * (self.fall_slope/2) * self.lastest_limit_index/3)) * 30 - self.inc_day_count - self.inc_sum*2#(1-1/((回落量-4) * (斜率/2) * 回落天数/3)) * 30 - 阳线天数*1 -阳线总涨幅*2
        #企稳情况 30
        self.com_slow_fall_grade()
        grade += self.stready_grade/100*30
        self.grade = grade * double_multiple
        #换手情况 （预留）
    '''
    【辅助函数】计算回落形态
    '''
    def com_fall_data(self):
        after_limit_df = self.single_df[self.single_df.index < self.lastest_limit_index]
        #计算回落量及斜率
        lastest_limit_c_price = self.single_df.loc[self.lastest_limit_index, 'close_price']
        lastest_c_price = self.single_df.loc[0, 'close_price']
        self.fall_vol_rate =  (lastest_c_price / lastest_limit_c_price -1) * 100
        self.fall_slope = self.fall_vol_rate / self.lastest_limit_index
        #计算阳线天数比及阳线总涨幅
        inc_serise = after_limit_df[self.single_df.increase > 0]['increase']
        self.inc_day_count = inc_serise.count()
        self.inc_sum = inc_serise.sum()
        #计算振幅（标准振幅，极端振幅）
        mean = after_limit_df['close_price'].mean()
        after_limit_df['standard_amplitude'] = 0
        after_limit_df['extreme_amplitude'] = 0
        def com_delta(raw,mean):
            raw['standard_amplitude'] = (raw['close_price']/mean-1)*100
            raw['extreme_amplitude'] = (raw['high_price']/mean-1)*100 \
                if abs((raw['high_price']/mean-1)*100) > abs((raw['low_price']/mean-1)*100) \
                else (raw['low_price']/mean-1)*100
            return raw
        after_limit_df = after_limit_df.apply(com_delta,args=(mean,),axis = 1)
        self.standard_amplitude = after_limit_df['standard_amplitude'].mean()
        self.extreme_amplitude = after_limit_df['extreme_amplitude'].mean()
        # 换手情况（预留）
    '''
    【辅助函数】计算回落企稳情况 grade = 100
    '''
    def com_slow_fall_grade(self):
        self.stready_grade = 0
        #最后一日涨幅情况 50
        standard_delta = self.single_df.loc[0,'increase']
        extreme_delta = self.single_df.loc[0,'high_price'] / self.single_df.loc[0,'close_price'] -1
        self.stready_grade +=(-standard_delta**2 + 1.5**2 + standard_delta/3)/1.5**2 * 30#(-涨幅**2 + 1.5**2 + 涨幅/3)/1.5**2 * 30
        self.stready_grade += (-extreme_delta**2 + 3**2 +extreme_delta/3)/3**2 * 20#(-极值差**2 + 1.5**2 + 极值差/3)/1.5**2 * 20
        #预留金针探底
        if self.stready_grade < -5:
            # 兜底-5
            self.stready_grade = -5
        #总回落日期长度 20
        if self.lastest_limit_index <2:
            self.stready_grade +=0
        elif self.lastest_limit_index ==2:
            self.stready_grade += 5
        elif 3<=self.lastest_limit_index <=5:
            self.stready_grade += 20
        else:
            self.stready_grade += 1/(self.lastest_limit_index - 4)*20
        #总体放缓程度 30
        fall_vol_rate = (self.single_df.loc[0,'close_price']/self.single_df.loc[self.lastest_limit_index,'close_price'] - 1) * 100
        delta_rate = 0
        for i in range(1,self.lastest_limit_index):
            delta_rate += self.single_df.loc[self.lastest_limit_index -i,'increase'] / ((3/(5*i)-3/(5*(i+1))) * fall_vol_rate) -1
        print('delta_rate:',delta_rate)
        #换手情况（预留）

class stock_buffer:
    def __init__(self,date = None):
        self.stock_buffer = {}
        self.trade_df = ''
        self.date = date
        self.sql_range_day = 150
        self.sql_start_date = ''#'2021-06-10'
        self.id_set = set()
        self.save = ''
        #trade_data区间开始的时间
    def init_buffer(self):
        self.creat_time()
        # self.clean_tab()
        self.select_info()
        self.save = pub_uti_a.save()
        for id in self.id_set:
            self.init_stock(id)
        self.save.commit()
    def creat_time(self):
        if self.date == None:
            sql = "select DATE_FORMAT(max(trade_date),'%Y-%m-%d') as last_date from stock_trade_data "
            self.date = pub_uti_a.select_from_db(sql=sql)[0][0]
        self.sql_start_date = (datetime.datetime.strptime(self.date,'%Y-%m-%d') -
                               datetime.timedelta(days= self.sql_range_day)).strftime('%Y-%m-%d')
    def clean_tab(self):
        sql = "delete from limit_up_single where trade_date = '{}'".format(self.date)
        print('清除完成。')
        pub_uti_a.commit_to_db(sql)
    def select_info(self):
        trade_sql = "select stock_id,stock_name,high_price,low_price,open_price,close_price,trade_date,increase,turnover_rate,point_type " \
                    " FROM stock_trade_data " \
                    "where trade_date >= '{0}' and trade_date <= '{1}' " \
                    "AND stock_id NOT LIKE 'ST%' AND stock_id NOT LIKE '%ST%' " \
                    "AND stock_id NOT like '300%' AND  stock_id NOT like '688%'".format(self.sql_start_date,self.date)
        print('trade_sql:{}'.format(trade_sql))
        self.trade_df = pub_uti_a.creat_df(sql=trade_sql)
        self.trade_df.fillna('',inplace=True)
        self.id_set = set(self.trade_df['stock_id'].tolist())
        #test
        # self.id_set = ('603035','603036')
        # print(self.df.columns)
    '''
    对10日内涨停数不大于2的stock实例化
    '''
    def init_stock(self,id):
        single_df = self.trade_df.loc[self.trade_df.stock_id == id]
        single_df.reset_index(inplace=True)
        if len(single_df) < 30 :
            return
        # single_df = single_df.head(10)
        # i1 = single_df['increase']
        # single_df['flag'] = 0
        single_df['flag'] = single_df['increase'].apply(lambda x: 1 if x>=9.75 else 0)
        # i2 = single_df['increase']
        flag_list = single_df['flag'].to_list()[0:10]
        if sum(flag_list) > 2 or sum(flag_list) == 0:
            return
        #涨停在最后一日则退出
        if single_df.loc[0,'flag'] == 1:
            return
        stock_name = single_df.loc[0,'stock_name']
        self.stock_buffer[id] = stock_object = stock(id,self.date,single_df)
        stock_object.distinguish_type()
        sql = "insert into limit_up_single(trade_code,stock_id,stock_name,trade_date,grade) " \
              "values('{0}','{1}','{2}','{3}','{4}') " \
              "ON DUPLICATE KEY UPDATE trade_code='{0}',stock_id='{1}',stock_name='{2}',trade_date='{3}',grade='{4}' " \
              "".format(stock_object.trade_code,id,stock_name,self.date,stock_object.grade)
        if stock_object.grade>0:
            print(stock_name,id, stock_object.grade)
        self.save.add_sql(sql)
    def get_stock(self,id):
        pass
'''

计算历史指定日期情况（用于验证）
'''
def history(start_date,end_date):
    sql = "select distinct date_format(trade_date ,'%Y-%m-%d') as trade_date from stock_trade_data where trade_date>= '{}' and trade_date <= '{}'".format(start_date,end_date)
    date_tuple = pub_uti_a.select_from_db(sql=sql) #(('2021-06-14',),('2021-06-15',))
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
    # history(start_date= '2021-06-20', end_date= '2021-08-17')
    print('completed.')
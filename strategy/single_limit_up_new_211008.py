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
        self.h_point_fall = None
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
                # self.com_double_limit_grade()
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
            self.com_wave_grade()
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
        if delta_rate >= 2.5:
            self.grade = 1
            return
        #第三日回撤幅度 40
        three_inc = self.single_df.loc[self.lastest_limit_index - 1, 'increase']
        three_grade =  (-three_inc * 5) if (-three_inc * 5)<40 else 40
        print('第三日回撤幅度:{}'.format(three_grade))
        grade += three_grade
        #整体回撤情况 30
        self.com_fall_data()
        fall_grade =(1-1/(1+(self.fall_vol_rate/2) * (self.fall_slope/2) * self.lastest_limit_index/3)) * 30 - self.inc_day_count - self.inc_sum*2#(1-1/((回落量/2) * (斜率/2) * 回落天数/3)) * 30 - 阳线天数*1 -阳线总涨幅*2
        print('整体回撤情况:{}'.format(fall_grade))
        grade += fall_grade
        #企稳情况 30
        self.com_slow_fall_grade()
        print('企稳情况:{}'.format(self.stready_grade))
        grade += self.stready_grade/100*30
        self.grade = grade * double_multiple
        #换手情况 （预留）
    '''
    计算波型分数
    '''
    def com_wave_grade(self):
        wave_multiple = 200  # 内函数总分50 * 200 =10000
        grade = 0
        #前期走势，上涨波还是下跌波40
        before_flag = self.com_before_trend()
        print('before_flag:',before_flag)
        if before_flag == 1:
            if self.h_point_fall <-10:
                # 下跌波
                wave_multiple = 150  # 内函数总分68 * 150 =10000
            else:
                #上升波
                wave_multiple = 200  # 内函数总分50 * 200 =10000

            trend_grade =(1/(1+(self.bofore_delta_inc/8) * (self.before_slope/2))) * 40   # (1/((回落量/2) * (斜率/2) )) * 30
            grade +=trend_grade
        else:
            grade +=30
        #回撤情况50
        self.com_fall_data()
        fall_grade =(1-1/(1+(self.fall_vol_rate/2) * (self.fall_slope/2) * self.lastest_limit_index/5)) * 50 - self.inc_day_count - self.inc_sum*2  # (1-1/((回落量/2) * (斜率/2) * 回落天数/3)) * 30 - 阳线天数*1 -阳线总涨幅*2
        grade += fall_grade
        #企稳情况10
        self.com_slow_fall_grade()
        grade += self.stready_grade/100*10
        self.grade = grade * wave_multiple
        print('涨停前大趋势：{}，涨停前走势分数：{}，回落分数：{}，企稳情况:{}'.format(self.h_point_fall,trend_grade,fall_grade,self.stready_grade))
        print('波型总分：{}'.format(self.grade))
        #换手（预留）
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
        print('回撤量：{}，回撤斜率：{},阳线天数：{}，阳线总涨幅：{},回撤天数：{}'.format(self.fall_vol_rate, self.fall_slope,self.inc_day_count,self.inc_sum,self.lastest_limit_index))
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
        # print('企稳--最后一日涨幅分数：',self.stready_grade)
        #总回落日期长度 20
        if self.lastest_limit_index <2:
            self.stready_grade +=0
            # print('企稳--回落日期长度分数：', 0)
        elif self.lastest_limit_index ==2:
            self.stready_grade += 5
            # print('企稳--回落日期长度分数：', 5)
        elif 3<=self.lastest_limit_index <=5:
            self.stready_grade += 20
            # print('企稳--回落日期长度分数：', 20)
        else:
            self.stready_grade += 1/(self.lastest_limit_index - 4)*20
            # print('企稳--回落日期长度分数：', 1/(self.lastest_limit_index - 4)*20)
        #总体放缓程度 30
        fall_vol_rate = (self.single_df.loc[0,'close_price']/self.single_df.loc[self.lastest_limit_index,'close_price'] - 1) * 100
        delta_rate = 0
        for i in range(1,self.lastest_limit_index):
            delta_rate += self.single_df.loc[self.lastest_limit_index -i,'increase'] / ((3/(5*i)-3/(5*(i+1))) * fall_vol_rate) -1
        # print('企稳--整体趋势分数:',delta_rate)
        #换手情况（预留）
    '''
    【辅助函数】计算涨停前趋势 
    '''
    def com_before_trend(self):
        #涨停前最后一组H L点差值及斜率
        before_limit_df = self.single_df[self.single_df.index > self.lastest_limit_index]
        l_list = before_limit_df[before_limit_df.flag == 'l'].index.to_list()
        h_list = before_limit_df[before_limit_df.flag == 'h'].index.to_list()
        print('before_limit_df:',before_limit_df)
        if l_list == [] or h_list == []:
            return 0
        before_extremum_day_count = h_list[0] - l_list[0]
        self.bofore_delta_inc = (before_limit_df.loc[h_list[0],'high_price']/before_limit_df.loc[l_list[0],'low_price'] -1)*100
        self.before_slope = self.bofore_delta_inc/before_extremum_day_count
        #涨停前多个H点的走势，差值
        last_price = 0
        self.h_point_fall = 0
        for i in h_list:
            new_price = before_limit_df.loc[i,'high_price']
            if new_price < last_price:
                break
            self.h_point_fall += new_price/last_price -1
            last_price = new_price
        self.h_point_fall *= 100
        #涨停日与前最近L点距离（涨停前是否已有涨幅），及涨幅情况
        self.day_delta_before_limit = l_list[0] -  self.lastest_limit_index
        self.inc_delta_before_limit = sum(self.single_df['increase'][self.lastest_limit_index:l_list[0]])
        return 1


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
        time1 = datetime.datetime.now()
        self.creat_time()
        print('创建时间耗时：',datetime.datetime.now() - time1)
        time2 = datetime.datetime.now()
        self.clean_tab()
        print('清除数据耗时：', datetime.datetime.now() - time2)
        time3 = datetime.datetime.now()
        self.select_info()
        print('查询数据耗时：', datetime.datetime.now() - time3)
        time4 = datetime.datetime.now()
        self.save = pub_uti_a.save()
        for id in self.id_set:
            # time5 = datetime.datetime.now()
            self.init_stock(id)
            # print('单条stock耗时：', datetime.datetime.now() - time5)
        self.save.commit()
        print('整体存储耗时：', datetime.datetime.now() - time4)
        print('总耗时：', datetime.datetime.now() - time1)
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
        sel_start_time = datetime.datetime.now()
        self.trade_df = pub_uti_a.creat_df(sql=trade_sql)
        print('df DB查询消耗时间：',datetime.datetime.now() - sel_start_time)
        self.trade_df.fillna('',inplace=True)
        self.id_set = set(self.trade_df['stock_id'].tolist())
        #test
        # self.id_set = ('603035','603036')
        # print(self.df.columns)
    '''
    对10日内涨停数不大于2的stock实例化
    '''
    def init_stock(self,id):
        start_time = datetime.datetime.now()
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
        print('涨停数判断耗时：',datetime.datetime.now()-start_time)
        stock_name = single_df.loc[0,'stock_name']
        self.stock_buffer[id] = stock_object = stock(id,self.date,single_df)
        start_time = datetime.datetime.now()
        stock_object.distinguish_type()
        print('单stock耗时：', datetime.datetime.now() - start_time)
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
    date ='2021-10-15' #'2021-01-20'
    st_buff = stock_buffer(date)
    st_buff.init_buffer()
    # history(start_date= '2021-06-20', end_date= '2021-08-17')
    print('completed.')
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

logging.basicConfig(level=logging.DEBUG, filename='../log/single_limit_up_new.log', filemode='w',
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
        self.bofore_delta_inc = None
        self.before_slope = None
        self.h_point_amplitude =None
        self.day_delta_before_limit = None
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
                down_rate = (l_vol_list[0]/h_vol_list[0]  -1)*100
        # print('stock_name:{},down_rate:{},h_price:{},l_price:{}'.format(self.single_df.loc[0,'stock_name'],down_rate,h_vol_list[0] , l_vol_list[0]))
        if down_rate <= -25 and abs(h_index_list[0] - l_index_list[0]) >= 6:
            self.limit_type = 'v_rebound'
            self.com_v_rebound()
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
        self.com_standard_grade()
    '''
    【功能】计算双涨停型分数 double_limit
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
    【功能】计算波型分数 wave
    '''
    def com_wave_grade(self):
        wave_multiple = 200  # 内函数总分50 * 200 =10000
        grade = 0
        #前期走势 20
        before_flag = self.com_before_trend()
        print('before_flag:',before_flag)
        if before_flag != 1:
            self.grade = -3
            return
        print('h_point_fall:',self.h_point_fall)
        if self.h_point_fall <-10:
            # 下跌波
            wave_multiple = 150  # 内函数总分68 * 150 =10000
        else:
            #上升波
            wave_multiple = 200  # 内函数总分50 * 200 =10000

        #大走勢得分 30
        '''
        #此算法比较复杂，暫時不使用
        long_trend_slope= self.h_point_amplitude / self.h_point_count
        print('大走勢總波動：{}，總天數：{}，斜率：{}'.format(self.h_point_amplitude,self.h_point_count,long_trend_slope))
        long_trend_grade = 1/(1+long_trend_slope /(self.h_point_count/10)) *30
        grade += long_trend_grade
        '''
        #<0.25,滿分 20
        long_trend_grade = 20
        if self.before_k_line_deviation <=0.025:
            pass
        elif self.before_k_line_deviation <=0.035:
            long_trend_grade *= 0.5
        elif self.before_k_line_deviation <= 0.04:
            long_trend_grade *= 0.25
        else:
            long_trend_grade = 0
        grade += long_trend_grade
        #漲停前趨勢得分
        print('漲停前差值：{}，漲停前斜率：{}'.format(self.bofore_delta_inc,self.before_slope))
        trend_grade =(1/(1+(self.bofore_delta_inc/30) * (self.before_slope/4))) * 30   # (1/((回落量/2) * (斜率/2) )) * 30
        grade +=trend_grade
        #回撤情况40
        self.com_fall_data()
        fall_grade =(1-1/(1+(self.fall_vol_rate/2) * (self.fall_slope/2) * self.lastest_limit_index/5)) * 40   # (1-1/((回落量/2) * (斜率/2) * 回落天数/3)) * 30
        grade += fall_grade
        #企稳情况10
        self.com_slow_fall_grade()
        grade += self.stready_grade/100*10
        self.grade = grade * wave_multiple
        print('大走勢得分:{},涨停前走势分数：{}，回落分数：{}，企稳情况:{}'.format(long_trend_grade,trend_grade,fall_grade,self.stready_grade/100*10))
        print('波型总分：{}'.format(self.grade))
        #换手（预留）
    '''
    【功能】计算低V反弹分数 v_rebound
    '''
    def com_v_rebound(self):
        grade = 0
        v_rebound_multiple = 150  # 内函数总分68 * 200 =10000
        #涨停后第二日无冒高20
        lastest_limit_c_price = self.single_df.loc[self.lastest_limit_index,'close_price']
        three_h_price = self.single_df.loc[self.lastest_limit_index-1,'high_price']
        delta_rate = three_h_price / lastest_limit_c_price
        if delta_rate > 1.03:
            three_inc_grade = 0
        elif delta_rate > 1.02:
            three_inc_grade = 10
        elif delta_rate > 1.01:
            three_inc_grade = 10
        else:
            three_inc_grade = 20
        grade += three_inc_grade
        #涨停后走势（要平缓）30
        self.com_fall_data()
        amplitude_value = (self.standard_amplitude - 1) + (self.extreme_amplitude - 2.5)
        amplitude_value = amplitude_value if amplitude_value > 0 else 0
        amplitude_grade = (1 / (1 + amplitude_value)) * 30
        grade += amplitude_grade
        #涨停前走势
        break_flag= self.com_before_trend()
        if break_flag == 0:
            return 0
        #涨停前已有涨幅（防范已有涨幅）[-30,50]
        before_inc_grade = 1/(1+self.inc_delta_before_limit/2+self.day_delta_before_limit/3)*50
        print('已有漲幅：{}，漲幅日期：{}'.format(self.inc_delta_before_limit,self.day_delta_before_limit))
        grade += before_inc_grade
        #换手（预留）
        self.grade += grade * v_rebound_multiple
        print('總分:{}，第二日无冒高:{},涨停后走势:{},涨停前已有涨幅:{}'.format(self.grade,three_inc_grade,amplitude_grade,before_inc_grade))
    '''
    【功能】计算標準型分数 standard
    '''
    '''
    标准波变量因子：1、前期走势20；2、涨停后平缓程度40； 3、涨停后一日冒高程度20；4、涨停前涨幅20；5、换手热门程度(预留)
    '''
    def com_standard_grade(self):
        wave_multiple = 200  # 内函数总分50 * 200 =10000
        grade = 0
        # 漲停前趨勢得分 30
        break_flag = self.com_before_trend()
        if break_flag == 0:
            return 0
        print('漲停前差值：{}，漲停前斜率：{}'.format(self.bofore_delta_inc, self.before_slope))
        trend_grade = (1 / (1 + (self.bofore_delta_inc / 30) * (
                    self.before_slope / 4))) * 30  # (1/((回落量/2) * (斜率/2) )) * 30
        grade += trend_grade
        #涨停后第二日无冒高20
        lastest_limit_c_price = self.single_df.loc[self.lastest_limit_index,'close_price']
        three_c_price = self.single_df.loc[self.lastest_limit_index-1,'close_price']
        three_h_price = self.single_df.loc[self.lastest_limit_index - 1, 'high_price']
        delta_rate_c = three_c_price / lastest_limit_c_price
        delta_rate_h = three_h_price / lastest_limit_c_price
        if delta_rate_c > 1.05:
            three_inc_grade = 0
        elif delta_rate_c > 1.03 and delta_rate_h <=1.07:
            three_inc_grade = 10
        elif delta_rate_c > 1.02 and delta_rate_h <=1.05 :
            three_inc_grade = 15
        else:
            three_inc_grade = 20
        grade += three_inc_grade
        #涨停后走势（要平缓）30
        self.com_fall_data()
        amplitude_value = (self.standard_amplitude -1) + (self.extreme_amplitude -2.5)
        amplitude_value = amplitude_value if amplitude_value > 0 else 0
        amplitude_grade = (1/(1+amplitude_value))*30
        grade += amplitude_grade
        #涨停前已有涨幅 [-20,20]
        inc_param = 0
        if self.day_delta_before_limit != 0:
            slope = self.inc_delta_before_limit/self.day_delta_before_limit
        else:
            slope = 0
        if self.inc_delta_before_limit <= 4 or slope<=2.5:
            inc_param =0
        elif 4< self.inc_delta_before_limit <8 and slope<=3:
            inc_param = 0.5
        elif 8< self.inc_delta_before_limit <12 and slope<=4:
            inc_param = 1
        else:
            inc_param = 50
        before_inc_grade = 1/(1+inc_param)*20
        # print('已有漲幅：{}，漲幅日期：{}'.format(self.inc_delta_before_limit,self.day_delta_before_limit))
        grade += before_inc_grade

        self.grade = grade * wave_multiple
        print('漲停前趨勢得分:{},二日无冒高分数：{}，涨停后走势分数：{}，涨停前已有涨幅分数:{}'.format(trend_grade, three_inc_grade, amplitude_grade,
                                                           before_inc_grade))
        print('标准型总分：{}'.format(self.grade))
        # 换手（预留）
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
            raw['standard_amplitude'] = abs(raw['close_price']/mean-1)*100
            raw['extreme_amplitude'] = abs(raw['high_price']/mean-1)*100 \
                if abs((raw['high_price']/mean-1)*100) > abs((raw['low_price']/mean-1)*100) \
                else abs(raw['low_price']/mean-1)*100
            return raw
        after_limit_df = after_limit_df.apply(com_delta,args=(mean,),axis = 1)
        self.standard_amplitude = after_limit_df['standard_amplitude'].mean()
        self.extreme_amplitude = after_limit_df['extreme_amplitude'].mean()
        # 换手情况（预留）
        print('回撤量：{}，回撤斜率：{},阳线天数：{}，阳线总涨幅：{},回撤天数：{}'.format(self.fall_vol_rate, self.fall_slope,self.inc_day_count,self.inc_sum,self.lastest_limit_index))
        print('標準波動：{}，極端波動：{}'.format(self.standard_amplitude,self.extreme_amplitude))
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
        l_list = before_limit_df[before_limit_df.point_type == 'l'].index.to_list()
        h_list = before_limit_df[before_limit_df.point_type == 'h'].index.to_list()
        # print('before_limit_df:',before_limit_df)
        if l_list == [] or h_list == []:
            print('錯誤：，不包含高低點！')
            return 0
        if h_list[0] < l_list[0] :
            #最後一個低點在漲停當日
            before_extremum_day_count = h_list[0] - self.lastest_limit_index
        else:
            before_extremum_day_count = h_list[0] - l_list[0]
        self.bofore_delta_inc = (before_limit_df.loc[h_list[0],'high_price']/before_limit_df.loc[l_list[0],'low_price'] -1)*100
        self.before_slope = self.bofore_delta_inc/before_extremum_day_count
        #涨停前多个H点的走势，差值
        last_price = 0
        self.h_point_fall = 0
        self.h_point_amplitude = 0
        self.h_point_count = 0
        fall_flag = True
        amplitude_flag = True
        for i in h_list:
            new_price = before_limit_df.loc[i,'high_price']
            if fall_flag and new_price > last_price:
                if last_price != 0:
                    self.h_point_fall += new_price / last_price - 1
            else:
                fall_flag = False
            if amplitude_flag and (last_price == 0 or abs(new_price / last_price -1) <0.15):
                if last_price != 0:
                    self.h_point_amplitude += abs(new_price / last_price - 1)
            else:
                amplitude_flag = False
            if (not fall_flag) and (not amplitude_flag):
                break
            last_price = new_price
            self.h_point_count = i
        self.h_point_fall *= 100
        self.h_point_amplitude *= 100
        print('大走勢數據：day_count:{}，h_point_fall:{}，h_point_amplitude:{}'.format(self.h_point_count,self.h_point_fall,self.h_point_amplitude))
        #計算漲停前30天k綫波動，[abs(x1/mean-1) +abs(x2/mean-1)]/30
        if len(before_limit_df) >= 30:
            c_mean = before_limit_df['close_price'][0:30].mean()
            before_limit_df['delta_mean'] = abs(before_limit_df['close_price']/c_mean-1)
            self.before_k_line_deviation = before_limit_df['delta_mean'][0:30].mean()
            print('30日K线偏離值：{}'.format(self.before_k_line_deviation))
        else:
            self.before_k_line_deviation = 10000
        #涨停日与前最近L点距离（涨停前是否已有涨幅），及涨幅情况
        self.day_delta_before_limit = l_list[0] -  self.lastest_limit_index
        self.inc_delta_before_limit = sum(self.single_df['increase'][self.lastest_limit_index+1:l_list[0]+1])
        if h_list[0] < l_list[0] :
            #最後一個低點在漲停當日
            self.day_delta_before_limit = self.inc_delta_before_limit =0
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
        # print('创建时间耗时：',datetime.datetime.now() - time1)
        time2 = datetime.datetime.now()
        self.clean_tab()
        # print('清除数据耗时：', datetime.datetime.now() - time2)
        time3 = datetime.datetime.now()
        self.select_info()
        # print('查询数据耗时：', datetime.datetime.now() - time3)
        time4 = datetime.datetime.now()
        self.save = pub_uti_a.save()
        for id in self.id_set:
            # time5 = datetime.datetime.now()
            self.init_stock(id)
            # print('单条stock耗时：', datetime.datetime.now() - time5)
        self.save.commit()
        # print('整体存储耗时：', datetime.datetime.now() - time4)
        # print('总耗时：', datetime.datetime.now() - time1)
    def creat_time(self):
        if self.date == None:
            sql = "select DATE_FORMAT(max(trade_date),'%Y-%m-%d') as last_date from stock_trade_data "
            self.date = pub_uti_a.select_from_db(sql=sql)[0][0]
        self.sql_start_date = (datetime.datetime.strptime(self.date,'%Y-%m-%d') -
                               datetime.timedelta(days= self.sql_range_day)).strftime('%Y-%m-%d')
    def clean_tab(self):
        sql = "delete from limit_up_single_validate where trade_date = '{}'".format(self.date)
        print('清除完成。')
        pub_uti_a.commit_to_db(sql)
    def select_info(self):
        trade_sql = "select stock_id,stock_name,high_price,low_price,open_price,close_price,trade_date,increase,turnover_rate,point_type " \
                    " FROM stock_trade_data " \
                    "where trade_date >= '{0}' and trade_date <= '{1}' " \
                    "AND stock_id NOT LIKE 'ST%' AND stock_id NOT LIKE '%ST%' " \
                    "AND stock_id NOT like '300%' AND  stock_id NOT like '688%' " \
                    " AND stock_id = '603013'".format(self.sql_start_date,self.date)
        trade_sql = "select stock_id,stock_name,high_price,low_price,open_price,close_price,trade_date,increase,turnover_rate,point_type " \
                    " FROM stock_trade_data " \
                    "where trade_date >= '{0}' and trade_date <= '{1}' " \
                    "AND stock_id NOT LIKE 'ST%' AND stock_id NOT LIKE '%ST%' " \
                    "AND stock_id NOT like '300%' AND  stock_id NOT like '688%' " \
                    " ".format(self.sql_start_date,self.date)
        print('trade_sql:{}'.format(trade_sql))
        sel_start_time = datetime.datetime.now()
        self.trade_df = pub_uti_a.creat_df(sql=trade_sql)
        # print('df DB查询消耗时间：',datetime.datetime.now() - sel_start_time)
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
        # print('涨停数判断耗时：',datetime.datetime.now()-start_time)
        stock_name = single_df.loc[0,'stock_name']
        self.stock_buffer[id] = stock_object = stock(id,self.date,single_df)
        start_time = datetime.datetime.now()
        stock_object.distinguish_type()
        print('tyep:',stock_object.limit_type)
        # print('单stock耗时：', datetime.datetime.now() - start_time)
        sql = "insert into limit_up_single_validate(trade_code,stock_id,stock_name,trade_date,grade,type ) " \
              "values('{0}','{1}','{2}','{3}','{4}','{5}') " \
              "ON DUPLICATE KEY UPDATE trade_code='{0}',stock_id='{1}',stock_name='{2}',trade_date='{3}',grade='{4}',type='{5}' " \
              "".format(stock_object.trade_code,id,stock_name,self.date,stock_object.grade,stock_object.limit_type)
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
    # p = Pool(8)
    for i in range(0, len(date_list)):
        print('日期：{}'.format(date_list[i]))
        st_buff = stock_buffer(date_list[i])
        st_buff.init_buffer()
    #     st_buff = stock_buffer(date_list[i])
    #     p.apply_async(st_buff.init_buffer)
    # #    p.apply_async(main, args=('1',date,))

    print('Waiting for all subprocesses done...')
    # p.close()
    # p.join()
    print('All subprocesses done.')


if __name__ == '__main__':
    # date ='2021-08-10' #'2021-01-20'
    # st_buff = stock_buffer(date)
    # st_buff.init_buffer()
    history(start_date= '2021-01-29', end_date= '2021-10-31')
    print('completed.')
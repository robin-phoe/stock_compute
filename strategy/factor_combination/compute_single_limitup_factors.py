'''
用于计算单涨停回撤策略中包含因子的计算，得到单个因子和多个因子联合作用与收益结果的关系
结果存储table/csv：'../factor_verify_res/compute_single_limitup_factors.csv'
因子：{
单(双)涨停后(base)、单日涨幅大于4%退出(base)、
日期、benchmark、
前斜率、后斜率、15日换手率、当前价/125日换手率比值、涨停次日开盘价/涨停价、涨停次日收盘价/涨停价、涨停次日最高价/涨停价、涨停前波动绝对值、涨停后波动绝对值、单双涨停标志、最后日企稳情况、
}
'''
# coding:utf-8
# import tushare as ts
import pandas as pd
import pymysql
import datetime
import re
from multiprocessing import Pool
from itertools import chain
import json
import copy
import numpy as np
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
import pub_uti_a


#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)


def create_df():
    res_df = pd.DataFrame(columns=('name','id','日期','benchmark','前斜率','后斜率','15日换手率','当前价/125日换手率比值','涨停次日开盘价/涨停价',
    '涨停次日收盘价/涨停价','涨停次日最高价/涨停价','涨停前波动绝对值','涨停后波动绝对值','单双涨停标志','最后日企稳情况'))
    return res_df
class stock:
    def __init__(self,id,date,single_df):
        self.id = id
        self.single_df = single_df #时间倒序
        self.date = date
        self.grade = 0 #20000+ 表示上涨即可进入，10000+ 表示优秀
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
        self.last_increase = 0
        self.stop = False
        self.gap_inc_rate = 1.3
        self.after_day_open = 0
        self.after_day_close = 0
        self.after_day_high = 0
        self.arg_rate_15 =0
        self.arg_lastest_rate = 0
    def compute(self):
        #判断是否属于涨停后区间
        if not self.com_inc():
            return
        self.count_limit_up()
        self.com_last_price()
        self.validate_redu()
        self.validate_inc_long()
#        self.grade = self.single_limit + self.after_day + abs(int(self.after_inc)) * 100 + self.last_price

    '''
    判断是否属于单涨停回撤区间 & 计算涨停前inc（及绝对值）累加
    a,期间涨幅大于5False；b,
    return bool
    '''
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
                if self.single_df.loc[i,'increase'] >= 4:
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

    '''
    计算十日内有几个涨停
    '''
    def count_limit_up(self):
        limit_up_df = self.single_df.head(10)
        flag_index_list = limit_up_df[limit_up_df.flag == 1].index.to_list()
        #单个涨停grade=15000
        if len(flag_index_list) == 1:
            self.single_limit = 1
        else:
            #如果两个涨停连续，grade =10000，两个涨停不连续，则grade =15000
            if abs(flag_index_list[0] - flag_index_list[1]) == 1:
                self.single_limit = 2
            else:
                self.single_limit = 1.5
    #涨停第二日开收盘价与前日涨停价格比照
    def com_price(self,i):
        limit_c_price = self.single_df.loc[i,'close_price']
        self.after_day_open = self.single_df.loc[i-1,'open_price'] / limit_c_price -1
        self.after_day_close = self.single_df.loc[i-1,'close_price'] / limit_c_price -1
        self.after_day_high = self.single_df.loc[i - 1, 'high_price'] / limit_c_price -1
    '''
    判断是否属于回落后企稳
    '''
    def com_last_price(self):
        #涨停后一日企稳不在范围内  pass
        self.last_increase = self.single_df.loc[1,'increase']
    #热度判断，15日换手率日均大于2.5%则过高
    def validate_redu(self):
        self.arg_rate_15 = self.single_df['turnover_rate'][0:15].mean()
        # print("arg_rate_15:",self.arg_rate_15)

    #涨幅判断，125日均线
    def validate_inc_long(self):
        len_arg = 125
        if len(self.single_df) < len_arg:
            len_arg = len(self.single_df)
        arg_price = self.single_df['close_price'][0:len_arg].mean()
        # print('{} arg_price:{} {}'.format(self.id,self.single_df.loc[0,'close_price'],arg_price))
        self.arg_lastest_rate = self.single_df.loc[0, 'close_price'] / arg_price
class stock_buffer:
    def __init__(self,start_date,end_date):
        self.stock_buffer = {}
        self.trade_df = ''
        self.start_date = start_date
        self.end_date = end_date
        self.date_list = set()
        self.id_set = set()
        self.res_df = create_df()
        #trade_data区间开始的时间
    def init_buffer(self):
        self.creat_time()
        self.select_info()
        count = 0
        for id in self.id_set:
            self.init_stock(id)
            count += 1
            print('count:',count,id)
        # self.res_df.to_csv('../factor_verify_res/compute_single_limitup_factors.csv')
    def creat_time(self):
        sql = "select distinct trade_date as trade_date from stock_trade_data " \
              " where trade_date >= '{0}' and trade_date <= '{1}' ".format(self.start_date,self.end_date)
        self.date_list = pub_uti_a.creat_df(sql=sql,ascending=True)['trade_date'].to_list()
        print('date_list:{}'.format(self.date_list))
    def select_info(self):
        trade_sql = "select stock_id,stock_name,high_price,low_price,open_price,close_price,trade_date,increase,turnover_rate " \
                    " FROM stock_trade_data " \
                    "where trade_date >= '{0}' and trade_date <= '{1}' " \
                    "AND stock_id NOT LIKE 'ST%' AND stock_id NOT LIKE '%ST%' " \
                    "AND stock_id NOT like '300%' AND  stock_id NOT like '688%' " \
                    " AND stock_id = '002712'".format(self.start_date,self.end_date)
        print('trade_sql:{}'.format(trade_sql))
        self.trade_df = pub_uti_a.creat_df(sql=trade_sql)
        self.trade_df.fillna('',inplace=True)
        self.id_set = set(self.trade_df['stock_id'].tolist())
    def init_stock(self,id):
        single_stock_df = self.trade_df[self.trade_df.stock_id == id]
        single_stock_df.reset_index(inplace=True,drop= True)
        for date in self.date_list:
            single_df = single_stock_df[single_stock_df.trade_date <= date]
            single_df.reset_index(inplace=True,drop= True)
            if len(single_df) < 30 :
                continue
            single_df['flag'] = single_df['increase'].apply(lambda x: 1 if x>=9.75 else 0)
            flag_list = single_df['flag'].to_list()[0:10]
            if sum(flag_list) > 2 or sum(flag_list) == 0:
                continue
            stock_name = single_df.loc[0,'stock_name']
            s = stock(id,date,single_df)
            s.compute()
            self.res_df.loc[len(self.res_df)] = (stock_name,id,date,0,0,0,s.arg_rate_15,s.arg_lastest_rate,s.after_day_open,
    s.after_day_close,s.after_day_high,s.before_inc_abs,s.after_inc_abs,s.single_limit,s.last_increase)
            # print(stock_name,id, stock_object.grade)



if __name__ == '__main__':
    st_buff = stock_buffer(start_date= '2019-01-01', end_date= '2019-08-09')
    st_buff.init_buffer()
    # history(start_date= '2022-02-16', end_date= '2022-04-14')
    print('completed.')
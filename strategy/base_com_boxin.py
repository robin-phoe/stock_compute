import pymysql
import logging
import datetime
from multiprocessing import Pool
import pandas as pd
import numpy as np
import mpl_finance
import matplotlib.pyplot as plt
from matplotlib import ticker
from copy import deepcopy
import re
import pub_uti_a
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config


logging.basicConfig(level=logging.DEBUG, filename='../log/base_com.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
class creat_df_from_db:
    def __init__(self):
        pass
    def creat_df(self,sql):
        global db
        cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
        cursor.execute(sql)  # 执行SQL语句
        data = cursor.fetchall()
        # 下面为将获取的数据转化为dataframe格式
        columnDes = cursor.description  # 获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
        df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
        if 'trade_date' in df.columns:
            df = df.sort_values(axis=0, ascending=False, by='trade_date', na_position='last')
            df.reset_index(inplace=True)
            df['trade_date'] = df['trade_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
        cursor.close()
        # print('df:',df)
        return df
# class save:
#     def __init__(self,db):
#         self.db = db
#         self.cursor = db.cursor()
#     def add_stock(self,id,boxin_list):
#         boxin_list = str(boxin_list).replace('\'','"')
#         sql = 'insert into boxin_data(stock_id,boxin_list) ' \
#               'values(\'{0}\',\'{1}\') ' \
#               'ON DUPLICATE KEY UPDATE stock_id=\'{0}\',boxin_list=\'{1}\' ' \
#               ''.format(id,boxin_list)
#         print('sql:',sql)
#         self.cursor.execute(sql)
#     def commit(self):
#         try:
#             self.db.commit()
#             print('存储完成')
#             logging.info('存储完成')
#         except Exception as err:
#             self.db.rollback()
#             print('存储失败:', err)
#             logging.error('存储失败:{}'.format(err))
#         self.cursor.close()
class point:
    def __init__(self,type,date,open_price,close_price,high_price,low_price):
        self.date = date
        self.type = type #True：高点，False:低点
        self.high_price = high_price
        self.low_price = low_price
        self.use_price = 0
        self.open_price = open_price
        self.close_price = close_price
        if self.open_price >= self.close_price:
            self.big_price = self.open_price
            self.small_price = self.close_price
        else:
            self.big_price = self.close_price
            self.small_price = self.open_price
        # self.com_use_price()
    # def com_use_price(self):
    #     if self.type == 'high':
    #         self.use_price = self.high_price
    #     elif self.type == 'low':
    #         self.use_price = self.low_price
    #     else:
    #         print('ERROR: {} point type not match!'.format(self.date))
    #         logging.ERROR('ERROR:point type not match!'.format(self.date))
class com_point:
    def __init__(self,id): #
        self.id = id
        self.new_point = ''
        self.dynamic_point1 = ''
        self.dynamic_point2 = ''
        self.confirm_point = ''
        self.delta_standard = 1.05
        self.res_list = []
        self.save_sql = ''
    def enter_new_point(self,date,high_price,low_price,open_price,close_price):
        self.new_point = point(type=False, date=date, high_price=high_price,low_price=low_price,open_price = open_price,close_price = close_price)
        if self.dynamic_point1 == '':
            self.dynamic_point1 = deepcopy(self.new_point)
            self.dynamic_point2 = deepcopy(self.new_point)
            self.dynamic_point2.type = not self.dynamic_point1.type
            return
        if self.dynamic_point1.type:
            if self.new_point.high_price > self.dynamic_point1.high_price:
                self.new_point.type = self.dynamic_point1.type
                self.dynamic_point1 = deepcopy(self.new_point)
                self.dynamic_point2 = deepcopy(self.dynamic_point1)
                self.dynamic_point2.type = not self.dynamic_point1.type
            elif self.new_point.low_price < self.dynamic_point2.low_price:
                self.new_point.type = self.dynamic_point2.type
                self.dynamic_point2 =deepcopy(self.new_point)
            else:
                return
        else:
            if self.new_point.low_price <= self.dynamic_point1.low_price:
                self.new_point.type = self.dynamic_point1.type
                self.dynamic_point1 = deepcopy(self.new_point)
                self.dynamic_point2 = deepcopy(self.dynamic_point1)
                self.dynamic_point2.type = not self.dynamic_point1.type
            elif self.new_point.high_price > self.dynamic_point2.high_price:
                self.new_point.type = self.dynamic_point2.type
                self.dynamic_point2 =deepcopy(self.new_point)
            else:
                return
        #判断区间成立
        if self.dynamic_point1.date == self.dynamic_point2.date:
            return
        if self.dynamic_point1.type :
            delta = self.dynamic_point1.big_price / self.dynamic_point2.small_price
        else:
            delta = self.dynamic_point2.big_price / self.dynamic_point1.small_price
        if delta < self.delta_standard:
            return
        #条件成立，记录数据
        else:
            if self.confirm_point != '':
                if self.confirm_point.type:
                    self.res_list.append(((self.dynamic_point1.date,self.dynamic_point1.low_price),
                                          (self.confirm_point.date,self.confirm_point.high_price))) #(低、高)
                    self.save(self.dynamic_point1.low_price, self.dynamic_point1.date,'l')
                else:
                    self.res_list.append(((self.confirm_point.date,self.confirm_point.low_price),
                                          (self.dynamic_point1.date,self.dynamic_point1.high_price)))  # (低、高)
                    self.save(self.dynamic_point1.high_price, self.dynamic_point1.date,'h')
            else:
                if self.dynamic_point1.type:
                    self.save(self.dynamic_point1.high_price,self.dynamic_point1.date,'h')
                else:
                    self.save(self.dynamic_point1.low_price, self.dynamic_point1.date,'l')
            self.confirm_point = deepcopy(self.dynamic_point1)
            self.dynamic_point1 = deepcopy(self.dynamic_point2)
            self.dynamic_point2.type = not self.dynamic_point1.type
    def save(self,price,date,point_type):
        self.save_sql = "update stock_trade_data set wave_data = '{0}',point_type = '{3}' " \
                        "where trade_date = '{1}' and stock_id = '{2}'".format(price,date,self.id,point_type)

class main:
    def __init__(self):
        self.df = ''
    def select_df(self):
        cf = creat_df_from_db()
        sql = "select stock_name,trade_date,high_price,low_price from stock_trade_data " \
              "where stock_id = '000892' and trade_date >= '2020-10-01' "
        self.df = cf.creat_df(sql)
    def compute(self):
        self.select_df()
        cp =com_point()
        for i in range(len(self.df)):
            cp.enter_new_point(self.df.loc[i,'trade_date'],self.df.loc[i,'high_price'],self.df.loc[i,'low_price'],)
        # print(cp.res_list)
class history:
    def __init__(self,date,table=None):
        self.df = ''
        self.id_set = set()
        self.sto_tab = table
        self.date = date
    def select_df(self):
        cf = creat_df_from_db()
        if self.sto_tab != None:
            clean_sql = "update stock_trade_data set wave_data = 0 , point_type = '' " \
                        " where trade_date >= '{}' and stock_id like '%{}'".format(self.date,self.sto_tab)
            pub_uti_a.commit_to_db(clean_sql)
            sql = "select stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price from stock_trade_data " \
                  "where trade_date >= '{}' and stock_id like '%{}'".format(self.date,self.sto_tab)
        else:
            clean_sql = "update stock_trade_data set wave_data = 0 , point_type = '' " \
                        " where trade_date >= '{}' ".format(self.date)
            pub_uti_a.commit_to_db(clean_sql)
            sql = "select stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price from stock_trade_data " \
                  "where trade_date >= '{}' ".format(self.date)
        self.df = cf.creat_df(sql)
    def core(self):
        self.select_df()
        # print('stock_id:',self.df['stock_id'].tolist())
        self.id_set = set(self.df['stock_id'].tolist())
        save_wave = pub_uti_a.save()
        save_trade = pub_uti_a.save()
        start = datetime.datetime.now()
        for stock_id in self.id_set:
            print('id:',stock_id)
            cp = com_point(stock_id)
            single_df = self.df[self.df['stock_id'] == stock_id]
            single_df = single_df.reset_index()
            # print('len_df:',len(single_df),single_df.index)
            for i in range(len(single_df)):
                cp.enter_new_point(single_df.loc[i, 'trade_date'], single_df.loc[i, 'high_price'],
                                   single_df.loc[i, 'low_price'],single_df.loc[i, 'open_price'],single_df.loc[i, 'close_price'] )
                if cp.save_sql != '':
                    save_trade.add_sql(cp.save_sql)
            print(cp.res_list)
            # boxin_list = str(cp.res_list).replace('\'', '"')
        #     wave_sql = 'insert into boxin_data(stock_id,boxin_list) ' \
        #           'values(\'{0}\',\'{1}\') ' \
        #           'ON DUPLICATE KEY UPDATE stock_id=\'{0}\',boxin_list=\'{1}\' ' \
        #           ''.format(id, boxin_list)
        #     save_wave.add_sql(wave_sql)
        # save_wave.commit()
        save_trade.commit()
        print('耗时：',datetime.datetime.now() - start)
def run(date):
    p = Pool(8)
    for i in range(0, 10):
        his = history(date,str(i))
        p.apply_async(his.core)
    #    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
if __name__ == '__main__':
    db_config = read_config('db_config')
    db = pymysql.connect(host=db_config["host"], user=db_config["user"], password=db_config["password"], database=db_config["database"])
    # m = main()
    # m.compute()

    h = history(date='2020-10-01')
    h.core()
    # run(date='2021-05-01')
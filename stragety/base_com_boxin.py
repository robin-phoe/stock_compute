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
        cursor.close()
        print('df:',df)
        return df
class point:
    def __init__(self,type,date,high_price,low_price):
        self.date = date
        self.type = type #True：高点，False:低点
        self.high_price = high_price
        self.low_price = low_price
        self.use_price = 0
        self.open_price = 0
        self.close_price = 0
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
    def __init__(self): #
        self.new_point = ''
        self.dynamic_point1 = ''
        self.dynamic_point2 = ''
        self.confirm_point = ''
        self.delta_satndard = 1.05
        self.res_list = []
    def enter_new_point(self,date,high_price,low_price):
        self.new_point = point(type=False, date=date, high_price=high_price,low_price=low_price)
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
            delta = self.dynamic_point1.high_price / self.dynamic_point2.low_price
        else:
            delta = self.dynamic_point2.high_price / self.dynamic_point1.low_price
        if delta < self.delta_satndard:
            return
        else:
            if self.confirm_point != '':
                if self.confirm_point.type:
                    self.res_list.append((self.confirm_point.date, self.dynamic_point1.date)) #(低、高)
                else:
                    self.res_list.append(( self.dynamic_point1.date, self.confirm_point.date))  # (低、高)
            self.confirm_point = deepcopy(self.dynamic_point1)
            self.dynamic_point1 = deepcopy(self.dynamic_point2)
            self.dynamic_point2.type = not self.dynamic_point1.type
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
        print(cp.res_list)
if __name__ == '__main__':
    db_config = read_config('db_config')
    db = pymysql.connect(host=db_config["host"], user=db_config["user"], password=db_config["password"], database=db_config["database"])
    m = main()
    m.compute()


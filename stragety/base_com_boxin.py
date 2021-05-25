import pymysql
import logging
import datetime
from multiprocessing import Pool
import pandas as pd
import numpy as np
import mpl_finance
import matplotlib.pyplot as plt
from matplotlib import ticker
import re

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
        if 'trad_date' in df.columns:
            df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
            df.reset_index(inplace=True)
        cursor.close()
        return df

class point:
    def __init__(self,type,date,high_price,low_price):
        self.date = date
        self.type = type #high low
        self.high_price = high_price
        self.low_price = low_price
        self.use_price = 0
        self.open_price = 0
        self.close_price = 0
        self.com_use_price()
    def com_use_price(self):
        if self.type == 'high':
            self.use_price = self.high_price
        elif self.type == 'low':
            self.use_price = self.low_price
        else:
            print('ERROR: {} point type not match!'.format(self.date))
            logging.ERROR('ERROR:point type not match!'.format(self.date))
class com_point:
    def __init__(self): #
        self.
        self.right_point = ''
        self.mid_point = ''
        self.left_point = ''
        self.new_point = ''
    def get_new_point(self,type,date,high_price,low_price):
        self.new_point = point(type,date,high_price,low_price)
        if self.near_low_point == '':
            self.near_low_point
# coding:utf-8
import pandas as pd
import pymysql
import datetime
import logging
import re
import json
import copy
import numpy as np
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config
import pub_uti

def add_rank_fun(start_date,end_date):
    sql = "select bk_id,trade_date,bk_name,redu from bankuai_day_data where trade_date >= '{}' and trade_date <= '{}'".format(start_date,end_date)
    df = pub_uti.creat_df(sql)
    date_set = set(df['trade_date'].to_list())
    print('date_set',date_set)
    for date in date_set:
        single_df = df[df.trade_date == date]
        # print('single:',single_df)
        single_df = single_df.sort_values(axis=0, ascending=False, by='redu', na_position='last')
        single_df.reset_index(inplace=True)
        single_df['ranks'] = single_df.index + 1
        print('single_df:',single_df)
        update_data(single_df)
def update_data(df):
    s = pub_uti.save()
    for i in range(len(df)):
        sql = "update bankuai_day_data set ranks = {} where bk_id = '{}' ".format(df.loc[i,'ranks'],df.loc[i,'bk_id'])
        # print('sql:',sql)
        s.add_sql(sql)
    s.commit()

if __name__ == '__main__':
    add_rank_fun(start_date='2021-07-01',end_date='2021-08-01')
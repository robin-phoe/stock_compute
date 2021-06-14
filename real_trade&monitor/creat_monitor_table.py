import redis
import pymysql
import logging
import time
import datetime
from wxpy import *
import pandas as pd
import numpy as np
import re
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config

logging.basicConfig(level=logging.DEBUG, filename='../log/creat_monitor_table.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
db_config = read_config('db_config')
db = pymysql.connect(host=db_config["host"], user=db_config["user"], password=db_config["password"],
                     database=db_config["database"])
#记录需要查询的类型表及SQL
table_dict = {}
table_code = {'zhuang':'1','remen_xiaoboxin':'2'}
def creat_sql(trade_date):
    table_dict['zhuang'] = 'SELECT stock_id,stock_name,zhuang_grade as grade,"zhuang" ' \
                           'FROM com_zhuang ' \
                           'WHERE zhuang_grade >= 1000 AND zhuang_grade <10000  AND lasheng_flag = 0 ' \
                           ' AND monitor = 1 AND stock_id not like "688%"'
    table_dict['remen_xiaoboxin'] ='SELECT stock_id,stock_name,grade,"remen_xiaoboxin" ' \
                                   'FROM remen_xiaoboxin ' \
                                   'WHERE trade_date = "{0}"  AND monitor = 1 AND stock_id not like "688%"'.format(trade_date) #AND grade >= 0 分数尚未完善
    table_dict['remen_xiaoboxin_c'] ='SELECT stock_id,stock_name,grade,"remen_xiaoboxin_c" ' \
                                   'FROM remen_xiaoboxin_c ' \
                                   'WHERE trade_date = "{0}"  AND monitor = 1 AND grade > 10000 AND stock_id not like "688%"'.format(trade_date) #AND grade >= 0 分数尚未完善
def sel_lastest_day():
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    sql  = "select max(trade_date) from stock_trade_data"
    print('sql:',sql)
    cursor.execute(sql)  # 执行SQL语句
    lastest_day = cursor.fetchall()[0][0].strftime("%Y-%m-%d")
    print('lastest_day:',lastest_day)
    logging.info('lastest_day:{}'.format(lastest_day))
    cursor.close()
    return lastest_day
def deal_data(lastest_day):
    lastest_day_str = re.sub('-','',lastest_day)
    cursor = db.cursor()
    stock_list = []
    for table in table_dict:
        logging.info('table info:{},{}'.format(table,table_dict[table]))
        print('table info:{},{}'.format(table,table_dict[table]))
        cursor.execute(table_dict[table])
        data = cursor.fetchall()
        print('data:',data)
        stock_list.extend(list(data))
    for i in range(len(stock_list)):
        stock_l = list(stock_list[i])
        #stock_id + date + table_code
        stock_l.append(sotck_l[0] + lastest_day_str + table_code[stock_l[3]])
        stock_l.append(lastest_day)
        stock_list[i] = tuple(stock_l)
    print('stock_list:',stock_list)
    del_sql = "delete from monitor where trade_date = '{}'".format(lastest_day)
    cursor.execute(del_sql)
    db.commit()
    print('{}：清除成功'.format(lastest_day))
    insert_sql = "insert into monitor (stock_id,stock_name,grade,monitor_type,trade_code,trade_date) values (%s,%s,%s,%s,%s,%s)"
    cursor.executemany(insert_sql, stock_list)
    db.commit()
    print('{}：存储成功'.format(lastest_day))
    cursor.close()
def main(lastest_day):
    if lastest_day == None:
        lastest_day = sel_lastest_day()
    creat_sql(lastest_day)
    deal_data(lastest_day)
if __name__ == '__main__':
    date = None#'2021-05-07'
    main(date)
'''
待优化：
1、update 的df转存储，待优化成excutemany
'''
#coding=utf-8
import requests
import re
import pymysql
import pandas as pd
import logging
#import threading
import json
import time
import tushare as ts
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config

logging.basicConfig(level=logging.DEBUG, filename='../log/stock_base_tushare.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

#初始化tushare api
pro = ts.pro_api('87b1df604b58eac3662ebaeabe6bb3436792125d2bf1f73a4a11f06a')
def get_df_from_db(sql, db):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    cursor.close()
    return df
def git_base_info(db):
    #清除原数据
    # sql = "delete from stock_informations"
    # cursor = db.cursor()
    # cursor.execute(sql)
    # cursor.close()
    data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    print(data)
    cursor = db.cursor()
    for i in range(len(data)):
        stock_id = data.loc[i,'symbol']
        stock_name = data.loc[i, 'name']
        bk_name = data.loc[i, 'industry']
        quyu = data.loc[i, 'area']
        h_table = stock_id[-1]
        try:
            sql = "insert into stock_informations(stock_id,stock_name,bk_name,区域,h_table) " \
                  "values('{0}','{1}','{2}','{3}','{4}')" \
                  "ON DUPLICATE KEY UPDATE stock_id='{0}',stock_name='{1}',bk_name='{2}',区域='{3}',h_table='{4}' " \
                .format(stock_id,stock_name,bk_name,quyu,h_table)
            # sql="update stock_informations set 发行量={0},bk_name='{1}', 证监会行业='{2}', 上市日期='{3}', 曾用名='{4}', 每股发行价='{5}', 区域='{6}', \
            #     雇员人数='{7}', 经营范围='{8}', 公司简介='{9}' where stock_id = '{10}'\
            #     ".format(fxl,dchy,zjhy,ssrq,cym,mgfxj,qy,gyrs,jyfw,gsjj,stock_id)
            print('sql',sql)
            cursor.execute(sql)
            db.commit()
            print('存储完成')
            logging.info('存储完成:id:{}'.format(stock_id))
        except Exception as err:
            db.rollback()
            print('存储失败:',err)
            logging.error('存储失败:id:{},{}'.format(stock_id,err))
    cursor.close()

def update_other_tab(db):
    table_list = ['stock_trade_data','monitor','com_zhuang']
    sql = "select * from stock_informations"
    df = get_df_from_db(sql, db)
    cursor = db.cursor()
    for i in range(len(df)):
        stock_name = df.loc[i,'stock_name']
        bk_name = df.loc[i,'bk_name']
        stock_id = df.loc[i, 'stock_id']
        print('stock_id:{}'.format(stock_id))
        for tab in table_list:
            sql = "update {0} set stock_name='{1}' where stock_id = '{2}'".format(tab, stock_name,stock_id)
            cursor.execute(sql)
    try:

        db.commit()
        print('存储完成')
    except Exception as err:
        db.rollback()
        print('存储失败:id:{},{}'.format(stock_id, err))
        logging.error('存储失败:id:{},{}'.format(stock_id, err))
    cursor.close()

def main(update_flag = 0):
    db_config = read_config('db_config')
    db = pymysql.connect(host=db_config["host"], user=db_config["user"],
                         password=db_config["password"], database=db_config["database"])
    # cursor = db.cursor()
    #get_data(stock_id='603828')#000790
    #get_data(stock_id='000790')
    # stock_id_list=select_info(db)
    #stock_id_list = [('002038',)]
    # for stock in stock_id_list:
    #     print('stock[0]:',stock[0])
    #     get_data(stock[0],db)
    if update_flag ==1:
        git_base_info(db)
        update_other_tab(db)
    elif update_flag == 0:
        git_base_info(db)
    elif update_flag == 2:
        update_other_tab(db)



if __name__ == '__main__':
    main(update_flag = 2)



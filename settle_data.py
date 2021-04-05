#coding=utf-8
import requests
import re
import pymysql
import pandas as pd
import logging
import math
import datetime
from multiprocessing import Pool
pd.set_option('display.max_rows',1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth',1000)

logging.basicConfig(level=logging.DEBUG,filename='settle_data.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
def select_info(db):
    cursor = db.cursor()
    sql="select stock_id from stock_informations "
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    cursor.close()
    #print(stock_id_list)
    return stock_id_list
def get_df_from_db(sql,db):
    cursor = db.cursor()#使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)# 执行SQL语句
    """
    使用fetchall函数以元组形式返回所有查询结果并打印出来
    fetchone()返回第一行，fetchmany(n)返回前n行
    游标执行一次后则定位在当前操作行，下一次操作从当前操作行开始
    """
    data = cursor.fetchall()

    #下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description #获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))] #获取列名
    df = pd.DataFrame([list(i) for i in data],columns=columnNames) #得到的data为二维元组，逐行取出，转化为列表，再转化为df
    
    """
    使用完成之后需关闭游标和数据库连接，减少资源占用,cursor.close(),db.close()
    db.commit()若对数据库进行了修改，需进行提交之后再关闭
    """
    cursor.close()
    #db.close()
    return df

    #print("cursor.description中的内容：",columnDes)
def compute(df,db):
    cursor = db.cursor()
    df = df.dropna(axis=0, how='any')
    df.reset_index(inplace=True)
    #df = df.fillna(-1)
    df=df.sort_values(axis=0, ascending=True, by='trade_date',na_position='last')
    df.reset_index(inplace=True)
    #print('df:\n',df)
    flag = 0
    for i in range(len(df)):
        if flag > 0:
            flag -= 1
            continue
        if df.loc[i,'fantan_grade'] < 20:
            continue
        value_list = []
        close_price = df.loc[i,'close_price']
        print('close_price:',close_price)
        low_price = df.loc[i,'low_price']
        #min_val = df['low_price'][i:i+3].min()
        max_val_3 = df['high_price'][i+1:i+3].max()
        max_val_4 = df['high_price'][i+1:i+4].max()
        print('max_val_3:',max_val_3)
        print('max_val_4:',max_val_4)
        #print('trade_code:',df.loc[i,'trade_code'])
        print("df['high_price'][i+1:i+3]:",df['high_price'][i+1:i+3])
        #print('df max i:',df.loc[i+1],'\n')
        #print('df max i+1:',df.loc[i+1],'\n')
        #print('df max i+2:',df.loc[i+1],'\n')
        #print("df.loc[i,'close_price']",df.loc[i,'close_price'])
        print('i:',i)
        mid_delta_val_3 = (float(max_val_3) - float(close_price))/float(close_price)
        max_delta_val_3 = (float(max_val_3) - float(low_price))/float(low_price)
        mid_delta_val_4 = (float(max_val_4) - float(close_price))/float(close_price)
        max_delta_val_4 = (float(max_val_4) - float(low_price))/float(low_price)
        print('mid_delta_val:',mid_delta_val_3,'max_delta_val:',max_delta_val_3)
        print('mid_delta_val:',mid_delta_val_4,'max_delta_val:',max_delta_val_4)
        try:
            sql="replace into settle_data_result(trade_code,trade_date,stock_id,stock_name,3_mid_delta_val,3_max_delta_val,4_mid_delta_val,4_max_delta_val) \
                values('{0}','{1}','{2}','{3}','{4}','{5}','{4}','{5}')\
                ".format(df.loc[i,'trade_code'],df.loc[i,'trade_date'],df.loc[i,'stock_id'],df.loc[i,'stock_name'],mid_delta_val_3,max_delta_val_3,mid_delta_val_4,max_delta_val_4)
            cursor.execute(sql)
            db.commit()
            print('存储完成')
            logging.info('存储完成:id:{},name:{}'.format(df.loc[i,'stock_id'],df.loc[i,'stock_name']))
        except Exception as err:
            db.rollback()
            print('存储失败:',err)
            logging.error('存储失败:id:{},name:{}\n{}'.format(df.loc[i,'stock_id'],df.loc[i,'stock_name'],err))
        flag = 2
    cursor.close()
        
def make_df():
    count = 0
    db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
    stock_id_list = select_info(db)
    #stock_id_list = [('000828',),]
    for ids in stock_id_list:
        sql = "select * from compute_result where stock_id = '{}'".format(ids[0])
        df = get_df_from_db(sql,db)
        print(ids[0],count)
        compute(df,db)
        #print(ids[0],count)
        count += 1
make_df()

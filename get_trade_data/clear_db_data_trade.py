#处理数据表中的increase
import pymysql
from multiprocessing import Pool
import pandas as pd
import numpy as np
import logging
import datetime
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config
#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
logging.basicConfig(level=logging.DEBUG, filename='../log/clear_db_data.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
def get_df_from_db(sql, db):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    # df = df.set_index(keys=['trade_date'])
    df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
    df.reset_index(inplace=True)
    df = df.dropna(axis=0, how='any')
    df.reset_index(inplace=True)
    # print('get df:',df.columns)
    del df['level_0']
    cursor.close()
    return df
def core(df,stock_id):
    # print('df column:',df.columns)
    df_single = df.drop(df[df.stock_id != stock_id].index)
    # print('df_single column:', df_single.columns)
    # print('df_single:',df_single)
    df_single.reset_index(inplace=True)
    df_single['yesterday'] = df_single['close_price'].shift(1)
    df_single['increase'] = (df_single['close_price']/df_single['yesterday'] - 1) * 100
    df_single.fillna(0,inplace=True)
    df_single = df_single[['increase','trade_code']]
    # print('df_single:',df_single)
    return df_single
def make_data(df,db,id_list):
    cursor = db.cursor()
    for id in id_list:
        time4 = datetime.datetime.now()
        df_single = core(df,id[0])
        print('deal single df:',datetime.datetime.now() - time4)
        increase_list = df_single.apply(lambda row: tuple(row), axis=1).values.tolist()
        print('increase_list:',increase_list)
        try:
            sql = "update stock_trade_data SET increase=(%s) where trade_code=(%s)"
            cursor.executemany(sql, increase_list)  # commit_id_list上面已经说明
            db.commit()
            print('存储成功。')
        except Exception as err:
            logging.exception('id:',id,err)
            db.rollback()
            print('存储失败:',id,err)

    cursor.close()
def clear_main(h_table,start_date,end_date):
    db_config = read_config('db_config')
    db = pymysql.connect(host=db_config["host"], user=db_config["user"],
                         password=db_config["password"], database=db_config["database"])
    cursor = db.cursor()
    sql = "select distinct stock_id from stock_trade_data where stock_id like '%{}'".format(h_table)
    time1 = datetime.datetime.now()
    cursor.execute(sql)  # 执行SQL语句
    id_list = cursor.fetchall()
    print('id select time:',datetime.datetime.now() - time1)
    cursor.close()
    sql = "select stock_id,trade_code,close_price,trade_date " \
          "from stock_trade_data " \
          "where  stock_id not like '688%' and trade_date >= '{0}' and trade_date <= '{1}'".format(start_date,end_date)
    time2 = datetime.datetime.now()
    df = get_df_from_db(sql,db)
    print('df select time:', datetime.datetime.now() - time2)
    # print(df)
    time3 = datetime.datetime.now()
    make_data(df, db, id_list)
    print('deal df select time:', datetime.datetime.now() - time3)

def run(start_date,end_date):
    p = Pool(8)
    for i in range(0, 10):
        p.apply_async(clear_main, args=(str(i),start_date,end_date,))
    #    p.apply_async(clear_main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
if __name__ == '__main__':
    start_date = '2018-10-01'
    end_date = '2021-04-30'
    # h_table = '0'
    # clear_main(h_table,start_date,end_date)
    #
    run(start_date,end_date)

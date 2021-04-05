#筛选出大涨幅的个股区间，用作分析数据（连续三个涨停以上）
# coding:utf-8
import pandas as pd
import pymysql
import numpy as np
import datetime
import logging
import re
from multiprocessing import Pool


logging.basicConfig(level=logging.DEBUG, filename='tongji_lasheng.py.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
def save(db,trade_code,stock_id,stock_name,trade_date,count):
    cursor = db.cursor()
    sql = "insert into tongji_dalasheng(trade_code,trade_date,stock_id,stock_name,zhangting_count) \
        values('{0}','{1}','{2}','{3}','{4}') " \
          "ON DUPLICATE KEY UPDATE trade_code='{0}',trade_date='{1}',stock_id='{2}',stock_name='{3}'," \
          "zhangting_count ='{4}' ".format(trade_code,trade_date,stock_id,stock_name,count)
    print('sql:', sql)
    cursor.execute(sql)
    try:
        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{},name:{}'.format(stock_id, stock_name))
    except Exception as err:
        db.rollback()
        print('存储失败:', err)
        logging.error('存储失败:id:{},name:{}\n{}'.format(stock_id, stock_name, err))
    cursor.close()
def main(h_tab,start_t,end_t):
    # if date == None:
    #     date = datetime.datetime.now().strftime('%Y-%m-%d')
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    sql = "select distinct  stock_id,stock_name from stock_history_trade{0}".format(h_tab)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    # date_time = datetime.datetime.strptime(date, '%Y-%m-%d')
    # start_t = (date_time - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    # stock_id_list = (('600121','郑州煤电'),)
    for ids_tuple in stock_id_list:
        ids = ids_tuple[0]
        if ids[0:3] =='300' or ids[0:3] =='688':
            continue
        stock_name = ids_tuple[1]
        # trade_code = re.sub('-', '', date[0:10]) + id
        sql = "SELECT trade_date  FROM stock_history_trade{0} \
                where trade_date >= '{1}' and trade_date <= '{2}' and  stock_id  = '{3}' " \
              "and increase >= 9.75 ".format(h_tab, start_t, end_t,ids)
        cursor.execute(sql)
        date_res = cursor.fetchall()
        print('date_res:',date_res)
        if len(date_res) <3 :
            continue
        date_list = []
        for date in date_res:
            # print('date:',type(date[0]))
            date_list.append(date[0])
        date_list.sort(reverse=True)
        # date_list = [datetime.datetime(2019, 5, 31, 0, 0), datetime.datetime(2019, 3, 25, 0, 0), datetime.datetime(2019, 2, 21, 0, 0), datetime.datetime(2018, 11, 16, 0, 0), datetime.datetime(2018, 11, 15, 0, 0)]
        print('date_list:',date_list)
        i = 0
        delta = datetime.timedelta(days =3)
        count = 0
        while i < (len(date_list)-2):
            # date = datetime.datetime.strptime(date_res[i], "%Y-%m-%d")
            date = date_list[i]
            # date_second = datetime.datetime.strptime(date_res[j], "%Y-%m-%d")
            date_second = date_list[i+1]
            print('cha:',date , date_second)
            if date - date_second > delta:
                print('flag:',date_list[i].strftime("%Y%m%d"))
                if count >= 3:
                    trade_code = date_list[i].strftime("%Y%m%d") + ids
                    # trade_code = re.sub('-', '', date_res[i][0:10]) + ids
                    save(db, trade_code, ids, stock_name, date_list[i].strftime("%Y-%m-%d"), count)
                    count = 0
                else:
                    count = 0
                    i += 1
                    continue
            else:
                count += 1
            i += 1

def run(start_t,end_t):
    p = Pool(8)
    for i in range(1, 11):
        p.apply_async(main, args=(str(i),start_t,end_t,))
    #    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')

if __name__ == '__main__':
    end_t ='2021-02-23'#None#'2021-02-01' #'2021-01-20'
    start_t= '2018-01-01'

    # h_tab = '3'
    # main(h_tab,start_t,end_t)

    run(start_t,end_t)
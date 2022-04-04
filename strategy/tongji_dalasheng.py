#筛选出大涨幅的个股区间，用作分析数据（连续三个涨停以上）
# coding:utf-8
import pandas as pd
import pymysql
import numpy as np
import datetime
import logging
import re
from multiprocessing import Pool
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from ·312·readconfig import read_config
import pub_uti_a

logging.basicConfig(level=logging.DEBUG, filename='../log/tongji_lasheng.py.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
def save(db,trade_code,stock_id,stock_name,arise_date,count):
    cursor = db.cursor()
    sql = "insert into tongji_dalasheng(trade_code,arise_date,stock_id,stock_name,zhangting_count) \
        values('{0}','{1}','{2}','{3}','{4}') " \
          "ON DUPLICATE KEY UPDATE trade_code='{0}',arise_date='{1}',stock_id='{2}',stock_name='{3}'," \
          "zhangting_count ='{4}' ".format(trade_code,arise_date,stock_id,stock_name,count)
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
    db_config = read_config('db_config')
    db = pymysql.connect(host=db_config["host"], user=db_config["user"],
                         password=db_config["password"], database=db_config["database"])
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    sql = "select distinct  stock_id,stock_name from stock_trade_data where stock_id like '%{}'".format(h_tab)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    # date_time = datetime.datetime.strptime(date, '%Y-%m-%d')
    # start_t = (date_time - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    # stock_id_list = (('600121','郑州煤电'),)
    for ids_tuple in stock_id_list:
        ids = ids_tuple[0]
        print('ids:',ids)
        if ids[0:3] =='300' or ids[0:3] =='688':
            continue
        stock_name = ids_tuple[1]
        # trade_code = re.sub('-', '', date[0:10]) + id
        sql = "SELECT trade_date  FROM stock_trade_data \
                where trade_date >= '{0}' and trade_date <= '{1}' and  stock_id  = '{2}' " \
              "and increase >= 9.75 ".format( start_t, end_t,ids)
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
    for i in range(0, 10):
        p.apply_async(main, args=(str(i),start_t,end_t,))
    #    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
##补充量异动统计
class add_value_info:
    def __init__(self):
        self.save = pub_uti_a.save()
    def select_buffer(self):
        sql = "SELECT D.trade_code,D.stock_id,D.stock_name,D.zhangting_count," \
              " D.arise_date,S.trade_date,S.value_abnormal " \
              " FROM tongji_dalasheng D " \
              "LEFT JOIN stock_trade_data S " \
              "ON D.stock_id = S.stock_id "
        self.df = pub_uti_a.creat_df(sql,ascending=True)
        stock_id_set = set(self.df['stock_id'].to_list())
        for id in stock_id_set:
            single_df = self.df[self.df['stock_id'] == id]
            arise_date_set = set(single_df['arise_date'].to_list())
            for lasheng_date in arise_date_set:
                single_df_sun = single_df[single_df['arise_date'] == lasheng_date]
                s = stock(single_df_sun,lasheng_date)
                save_sql = "update tongji_dalasheng set count_single= '{}',lastest_info = '{}' " \
                           "WHERE trade_code = '{}'".format(s.count_single,s.lastest_info,s.trade_code)
                self.save.add_sql(save_sql)
                print(s.stock_name,s.arise_date,s.zhangting_count,s.count_single,s.lastest_info)
        self.save.commit()
class stock:
    def __init__(self,single_df,arise_date):
        self.trade_code = ''
        self.stock_id = ''
        self.stock_name = ''
        self.zhangting_count = 0
        self.count_single= 0
        self.lastest_info = 'N'
        self.df = single_df
        self.arise_date = str(arise_date)
        self.info_tup = ('single','single2','hat','highland')
        self.deal_data()
    def deal_data(self):
        self.df.reset_index(inplace=True,drop=True)
        self.stock_id = self.df.loc[0,'stock_id']
        self.stock_name = self.df.loc[0,'stock_name']
        self.trade_code = self.df.loc[0,'trade_code']
        self.zhangting_count = self.df.loc[0,'zhangting_count']
        index_list = self.df[self.df['trade_date'] == self.arise_date].index.to_list()
        if len(index_list) == 1:
            arise_index = index_list[0]
            if arise_index <20: #新上市
                self.lastest_info = 'init'
                return 0
        else:
            print('ERROR:{} {} has error,when finding a index. {}'.format(self.stock_id,self.stock_name,len(index_list)))
            return 0
        for ind in range(arise_index-1,-1,-1):
            if self.df.loc[ind,'value_abnormal'] in self.info_tup:
                self.lastest_info = self.df.loc[ind,'value_abnormal']
                self.count_single +=1
            else:
                break


if __name__ == '__main__':
    # end_t ='2021-12-31'#None#'2021-02-01' #'2021-01-20'
    # start_t= '2018-01-01'
    #
    # time1 = datetime.datetime.now()
    # # h_tab = '1'
    # # main(h_tab,start_t,end_t)
    # run(start_t,end_t)
    # print('time_delta:', datetime.datetime.now() - time1)

    add_value_info().select_buffer()
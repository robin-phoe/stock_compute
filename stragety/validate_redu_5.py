#通过热门回头捕捉反弹
#短点：单涨停后小回头，前几日有单涨停加分，10日均线低于30日均线加分。【单涨停】：收盘是10日内最大值，10日increase<= 20%。
#大热门：30日内大热门，大回头

import mpl_finance
# import tushare as ts
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pymysql
from matplotlib import ticker
from matplotlib.pylab import date2num
import numpy as np
import datetime
import logging
import re
from multiprocessing import Pool
import json
import openpyxl

logging.basicConfig(level=logging.DEBUG, filename='comp_redu_210120.log', filemode='w',
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
    # df = df.dropna(axis=0, how='any')
    # df.reset_index(inplace=True)
    # df['trade_date2'] = df['trade_date'].copy()
    # print('trade_date2:',type(df['trade_date2'][0]))
    # df['trade_date2'] = pd.to_datetime(df['trade_date2']).map(date2num)
    # df['dates'] = np.arange(0, len(df))
    # df['avg_10'] = df['close_price'].rolling(10).mean()
    df['avg_5'] = df['close_price'].rolling(5).mean()
    cursor.close()
    # print(df)
    # df['trade_date'] = date2num(df['trade_date'])
    # print('df:', df[['avg_10', 'close_price']])
    return df
def save(db,trade_code, id, trade_date, cou, increase, increase_sum, redu_5,stock_name):
    cursor = db.cursor()
    try:
        sql = "insert into verify_redu_5(trade_code, stock_id, trade_date, days, increase, increase_sum, redu_5,stock_name) \
            values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}') " \
              "ON DUPLICATE KEY UPDATE trade_code ='{0}', stock_id='{1}', trade_date='{2}', days='{3}', increase='{4}', " \
              "increase_sum='{5}', redu_5='{6}',stock_name='{7}' "\
            .format(trade_code, id, trade_date, cou, increase, increase_sum, redu_5,stock_name)
        print('sql:', sql)
        cursor.execute(sql)
        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{},name:{},date:{}'.format(id, stock_name,trade_date))
    except Exception as err:
        db.rollback()
        print('存储失败:', err)
        logging.error('存储失败:id:{},name:{},date:{}\n{}'.format(id, stock_name,trade_date, err))
    cursor.close()
#单涨停回头验证
def make_data(df,db,redu_5):
    pass
    # for i in range(11,len(df)-5):
    #     if df.loc[i,'increase'] >= 9.75 / 100 and df['increase'][i-10:i].sum() <= 17 / 100:
    #         avg_5_flag = 0
    #         increase_list = []
    #         for j in range(i+1,i+4):
    #             if df.loc[j,'increase'] >= 9.75 / 100:
    #                 if j ==i+1:
    #                     save(db, df.loc[i, 'trade_code'], df.loc[i, 'trade_date'], df.loc[i, 'stock_id'],
    #                          df.loc[i, 'stock_name'], 0, avg_5_flag, df.loc[j,'increase'],second_zhangting= 1)
    #                     break
    #                 else:
    #                     save(db,df.loc[i,'trade_code'],df.loc[i,'trade_date'],df.loc[i,'stock_id'],df.loc[i,'stock_name'],
    #                                                     1,avg_5_flag,df.loc[j,'increase'],second_zhangting=(j-i))
    #                     break
    #             elif min(df.loc[j,'high_price'],df.loc[j,'low_price']) <= df.loc[j,'avg_5']:
    #                 avg_5_flag = 1
    #                 increase_list.append(df.loc[j, 'increase'])
    #             else:
    #                 increase_list.append(df.loc[j,'increase'])
    #         else:
    #             save(db, df.loc[i, 'trade_code'], df.loc[i, 'trade_date'], df.loc[i, 'stock_id'],
    #                  df.loc[i, 'stock_name'], 0,avg_5_flag,increase=max(increase_list))
    #     else:
    #         continue


def main(start_date):
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db.cursor()
    sql = "select C.trade_code,C.stock_id,C.trade_date,I.h_table,C.redu_5,C.stock_name from com_redu C " \
          "left join stock_informations I on I.stock_id = C.stock_id where redu_5 > 0 and trade_date >= '{}'".format(start_date)
    cursor.execute(sql)  # 执行SQL语句
    id_list = cursor.fetchall()
    count = len(id_list)
    print('count:',count)

    # id_list = [(600126,),] #杭钢股份
    for stock in id_list:
        # stock = ('20200429600677', '600677', datetime.datetime(2020, 4, 29, 0, 0), '4', 1.00073, '*ST航通')
        print('stock:',stock)
        id = stock[1]
        h_table = stock[3]
        trade_date = stock[2]
        redu_5 = stock[4]
        trade_code = stock[0]
        stock_name = stock[5]
        sql = "select  increase " \
              "from stock_history_trade{0} where stock_id = '{1}' and trade_date > '{2}' limit 3 ".format(h_table,id,trade_date)
        # df = get_df_from_db(sql, db)
        cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
        cursor.execute(sql)  # 执行SQL语句
        res_list = cursor.fetchall()
        print('res_list:',res_list)
        cou =1
        increase_sum = 0
        increase_list = [0]
        for inc in res_list:
            increase = inc[0]
            if increase >= 0.07:
                save(db,trade_code,id,trade_date,cou,increase,increase_sum,redu_5,stock_name)
                break
            else:
                increase_sum += increase
                cou +=1
                increase_list.append(increase)
        else:
            increase = max(increase_list)
            save(db,trade_code, id, trade_date, cou, increase, increase_sum, redu_5,stock_name)
        # make_data(df, db,redu_5)
    cursor.close()
if __name__ == '__main__':
    main(start_date = '2020-07-01')
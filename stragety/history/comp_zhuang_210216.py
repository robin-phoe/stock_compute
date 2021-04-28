# coding:utf-8
#0216 zsz 提供的庄线算法
#1.底部（有位于125日半年线下方部分）
#2.底部之后有10日换手率和大于百分之25
#3.高换手部分涨幅和小于百分之30
#4.高换手之后有大于百分之15的降幅
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

logging.basicConfig(level=logging.DEBUG, filename='comp_zhaung_210114.log', filemode='w',
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
    df = df.fillna(0)
    # df = df.dropna(axis=0, how='any')
    # df.reset_index(inplace=True)
    df['trade_date2'] = df['trade_date'].copy()
    # print('trade_date2:',type(df['trade_date2'][0]))
    df['trade_date2'] = pd.to_datetime(df['trade_date2']).map(date2num)
    df['dates'] = np.arange(0, len(df))
    df['arv_10'] = df['close_price'].rolling(10).mean()
    df['arv_5'] = df['close_price'].rolling(5).mean()
    # df['increase'] = df['increase'].astype('float')
    # df.loc[-1.5<float(df['increase'])<1.5,'increase_flag'] = 1 #increase 是str
    df['increase_flag'] = 0
    df['increase_abs'] = 0
    for i in range(1,len(df)-1):
        #涨幅绝对值
        df.loc[i, 'increase_abs'] = abs(float(df.loc[i, 'increase']))
        #DB中历史老数据缺失increase
        df.loc[i, 'increase'] = (df.loc[i,'close_price']-df.loc[i-1,'close_price']) / df.loc[i-1,'close_price']*100
        if -2 <= float(df.loc[i,'increase']) <=2:
            df.loc[i, 'increase_flag'] = 1
    cursor.close()
    # print(df)
    # df['trade_date'] = date2num(df['trade_date'])
    # print('df:', df[['increase','increase_flag']])
    return df
def compt_core(df,db, ids, stock_name,xielv=0.02,day_rate = 0.7,limit_count = 740,piece = 45):
    zhuang_date = ''
    zhuang_grade = 0
    lasheng = 0
    if len(df) <= 200:
        print('少于200条记录')
        return zhuang_date, zhuang_grade, lasheng
    df['125'] = df['close_price'].rolling(125).mean()
    #计算底部
    mean_flag = 1 #0 表示连续低于125均线
    interval = 0
    for i in range(len(df) - 15, len(df) - limit_count, -1):
        if interval >0:
            interval -= 1
            continue
        if i  <= 125:
            break
        print(df.loc[i,'trade_date'],df.loc[i,'close_price'], df.loc[i,'125'])
        if  df.loc[i,'close_price'] >= df.loc[i,'125'] :
            mean_flag = 1
            continue
        print('flag1')
        if not mean_flag:
            continue
        print('flag2')
        #计算底部后10日换手和,10涨幅和
        turnover_rate_sum = sum(df['turnover_rate'][i:i+10])
        zhangfu_sum = (df.loc[i+10,'close_price'] - df.loc[i,'close_price'])/df.loc[i,'close_price']#sum(df['increase'][i:i+10])
        print('flag3',turnover_rate_sum,zhangfu_sum)
        #db中新版increase *100
        if turnover_rate_sum < 0.25 or zhangfu_sum >=0.3:
            mean_flag = 0
            continue
        #判断前100日内是否有大跌超过百分之40
        qujian_100 = df['close_price'][i-100:i]
        if qujian_100.max()/qujian_100.min() >= 1.3:
            continue
        #计算回调部分,
        max_value = 0
        min_value = -1
        for j in range(i+1,i+16):
            if df.loc[j,'close_price'] >= max_value:
                max_value = df.loc[j,'close_price']
                min_value = max_value
            if df.loc[j,'close_price'] <= min_value:
                min_value = df.loc[j,'close_price']

        huitiao = (max_value - min_value)/max_value
        zhuang_grade = 1
        if huitiao >=0.1 and huitiao <= 0.2:
            zhuang_grade = 2
        zhuang_date = df.loc[i,'trade_date']
        #70日中最大值
        max_70 = df['close_price'][i:i+100].max()
        print('max_70',max_70,min_value)
        lasheng = max_70 /min_value
        print(zhuang_date,zhuang_grade,lasheng)
        date = df.loc[i,'trade_date'].strftime( '%Y-%m-%d')
        trade_code = re.sub('-', '', date) + ids
        save(trade_code,db, ids, stock_name, zhuang_date, zhuang_grade, lasheng)
        interval = 10
    return zhuang_date,zhuang_grade,lasheng
def save(trade_code,db, ids, stock_name,zhuang_date,zhuang_grade,lasheng):

    cursor = db.cursor()
    try:
        print('zhuang_grade:', zhuang_grade)

        sql = "insert into compute_zhuang_test(trade_code,stock_id,stock_name,zhuang_grade,dibu_date,lasheng) \
            values('{0}','{1}','{2}','{3}','{4}','{5}') " \
              "ON DUPLICATE KEY UPDATE trade_code = '{0}',stock_id='{1}',stock_name='{2}',zhuang_grade='{3}',dibu_date='{4}'," \
              "lasheng = '{5}' \
            ".format(trade_code,ids, stock_name,zhuang_grade,zhuang_date,lasheng)
        print('sql:', sql)
        cursor.execute(sql)
        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{},name:{}'.format(ids, stock_name))
    except Exception as err:
        db.rollback()
        print('存储失败:', err)
        logging.error('存储失败:id:{},name:{}\n{}'.format(ids, stock_name, err))
    cursor.close()
def main(h_tab, start_t, end_t):
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    sql = "select distinct  stock_id,stock_name from stock_history_trade{0}".format(h_tab)
    #临时补漏
    # sql = "select distinct  h.stock_id,h.stock_name from stock_history_trade{0} h " \
    #       "right join com_zhuang c " \
    #       "on h.stock_id = c.stock_id " \
    #       "where c.zhuang_grade / 10000000 < 10 and c.zhuang_grade / 10000000 >= 1".format(h_tab)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    # stock_id_list = [('600121','郑州煤电'),] #测试数据 h_tab = 3
    # stock_id_list = [('600165', '新日恒力'), ] #h_tab = 1
    # stock_id_list = [('603967', '中创物流'), ] #h_tab = 2
    # stock_id_list = [('002889', '东方嘉盛'), ] #h_tab = 6
    # stock_id_list = [('002958', '青农商行'), ]  # h_tab = 6
    # stock_id_list = [('002221', '东华能源'), ]  # h_tab = 8
    # stock_id_list = [('603331', '百达精工'), ]  # h_tab = 3
    # stock_id_list = [('000937', '冀中能源'), ]  # h_tab = 9
    for ids_tuple in stock_id_list:
        print(ids_tuple[1])
        # zhuang_grade = 1
        # zhuang_json = {}
        ids = ids_tuple[0]
        if start_t != None and end_t != None:
            sql = "SELECT stock_id,trade_date,open_price,close_price,high_price,low_price,increase,turnover_rate  FROM stock_history_trade{0} \
                    where trade_date >= '{1}' and trade_date <= '{2}' and  stock_id  = '{3}'".format(h_tab, start_t, end_t,ids)
        else:
            sql = "SELECT stock_id,trade_date,open_price,close_price,high_price,low_price,increase,turnover_rate  FROM stock_history_trade{0} \
                    where stock_id  = '{1}'".format(h_tab,ids)
        df = get_df_from_db(sql, db)
        # print('flag1')
        zhuang_date,zhuang_grade,lasheng = compt_core(df,db, ids, ids_tuple[1])
        # if zhuang_grade != 0:
        #     save(db, ids, ids_tuple[1], zhuang_date,zhuang_grade,lasheng)
def run(start_t, end_t):
    p = Pool(8)
    for i in range(1, 11):
        p.apply_async(main, args=(str(i), start_t, end_t,))
    #    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
if __name__ == '__main__':
    start_t = '2018-01-01'#None#'2020-01-01'
    end_t = '2020-11-01'#None#'2021-01-14'

    # h_tab = 1
    # main(h_tab, start_t, end_t)
    run(start_t, end_t)
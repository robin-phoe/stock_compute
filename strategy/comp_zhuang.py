# coding:utf-8
import pandas as pd
import pymysql
from matplotlib.pylab import date2num
import numpy as np
import datetime
import logging
import re
from multiprocessing import Pool
import json
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config

logging.basicConfig(level=logging.DEBUG, filename='../log/comp_zhaung.log', filemode='w',
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
    print('df:', df[['increase','increase_flag']])
    return df
def compt_core(df,xielv=0.02,day_rate = 0.7,limit_count = 740,piece = 45):
    zhuang_grade = 0
    zhuang_long = 0
    max_avg_rate = 0
    lasheng_flag = 0
    yidong = []
    zhuang_date = []
    if len(df) <= 200:
        print('少于200条记录')
        return zhuang_date,zhuang_grade,yidong,zhuang_long,max_avg_rate,lasheng_flag
    #时间正序
    df['piece_flag_sum'] = df.increase_flag.rolling(piece).sum()
    df['increase_abs_sum'] = df.increase_flag.rolling(piece).sum()
    start_day = end_day = df.loc[len(df)-1,'trade_date']
    date_dict = {}
    print('start:',len(df)-limit_count,len(df)-1)
    for i in range(len(df)-1,len(df)-limit_count,-1):
        if i-piece < piece:
            break
        behind_cp = df.loc[i,'close_price']
        front_cp = df.loc[i-piece,'close_price']
        print('斜率：',abs(behind_cp - front_cp) / front_cp)
        if abs(behind_cp - front_cp) / front_cp > xielv:
            continue
        print('斜率达标：',df.loc[i,'trade_date'])
        print('涨幅平稳比例：', df.loc[i, 'piece_flag_sum'],df.loc[i, 'piece_flag_sum'] / piece, day_rate)
        if (df.loc[i,'piece_flag_sum'] / piece) < day_rate:
            continue
        print('涨幅平稳比例达标：',df.loc[i,'piece_flag_sum'] / piece)
        # 筛出中部凹凸
        per_day = (behind_cp - front_cp)/piece
        ind_start = df.query("trade_date == '{}'".format(df.loc[i-piece,'trade_date'])).index[0]
        ind_end = df.query("trade_date == '{}'".format(df.loc[i,'trade_date'])).index[0]
        sum = 0
        count = 0
        for j in range(ind_start,ind_end+1):
            refer = front_cp + per_day * count
            sum += abs(df.loc[j,'close_price'] -refer) / refer
            count += 1
        print('凹凸系数：',abs(sum/piece))
        if sum/piece >=0.015: #0.0185：1910
            continue

        ##预留区间内换手（涨幅）大于阀值日期记录
        #选取最近的庄线计算zhuang_grade,因素：1、庄线长度；2、庄线内day_rate大小；3、庄线内异动日数量;4、历史极值与庄线内均线比;
        #最近时间大于60个交易日 zhuang_grade = 100000
        #最近时间之后极大值是庄线均值2倍以上，2000，1.5倍，3000，1.3倍，4000
        #历史最大值 // 均线值，+x00
        # zhuang_long = ind_end - ind_start
        #计算zhuang_grade
        if len(date_dict) == 0:
            zhuang_grade = 1
            ##预留zhuang_grade计算
            # 庄线期间平均值
            avg = df['close_price'][ind_start:ind_end].mean()
            # 历史极大值
            his_max = df['close_price'].max()
            max_avg_rate = his_max // avg * 1000
            # print('历史极值比：',his_max,avg,his_max // avg * 1000)
            if max_avg_rate >= 10000:
                max_avg_rate = 9000
            zhuang_grade += max_avg_rate
            #计算是否已拉升过，标准是百分之三十
            if df['close_price'][ind_end:len(df)-1].max() / avg >= 1.3:
                lasheng_flag = 1
            if len(df) - ind_end >60:
                #计算庄线后60个交易日内最大值
                max_value = df['close_price'][ind_end:ind_end+60].max()
                #计算极值对平均值倍数
                beishu = max_value*10 // avg
                print('倍数：',beishu)
                if beishu >= 100:
                    beishu = 99
                zhuang_grade += beishu * 1000000
                #计算极值距离时间
                max_ind_list = df.query("close_price == '{}'".format(max_value)).index
                max_ind = (max_ind_list & list(range(ind_end,ind_end+61)))[0]
                print('index 列表：',max_ind_list & list(range(ind_end,ind_end+61)))
                print('极值时间差：',max_ind , ind_end,max_ind - ind_end)
                zhuang_grade += (max_ind - ind_end) * 10000

            # ind_start = df.query("trade_date == '{}'".format(end_day)).index[0]
            # ind_end = df.query("trade_date == '{}'".format(start_day)).index[0]
            # print('index:',ind_start,ind_end)
            # max_value = df['close_price'][ind_start:ind_end].max()
            # min_value = df['close_price'][ind_start:ind_end].min()
            # print('value:',max_value,min_value)
        #存储记录
        if df.loc[i,'trade_date'] < end_day:
            start_day = df.loc[i,'trade_date']
        end_day = df.loc[i-piece,'trade_date']
        #计算庄线长度
        date_dict[str(start_day)] = str(end_day)
        if len(date_dict) == 1:
            zhuang_long = (df.query("trade_date == '{}'".format(start_day)).index[0] - df.query("trade_date == '{}'".format(end_day)).index[0]) //10 * 100
            if zhuang_long >= 1000:
                zhuang_long = 900
    zhuang_grade += zhuang_long
    print('date_dict:',date_dict)
    if len(date_dict) != 0:
        for key in date_dict:
            zhuang_date.append((key,date_dict[key]))
    return zhuang_date,zhuang_grade,yidong,zhuang_long,max_avg_rate,lasheng_flag
def save(db, ids, stock_name,zhuang_date,zhuang_grade,yidong,zhuang_long,max_avg_rate,lasheng_flag):

    cursor = db.cursor()
    try:
        print('zhuang_grade:', zhuang_grade)

        sql = "insert into com_zhuang(stock_id,stock_name,zhuang_grade,zhuang_section,yidong,zhuang_long,max_avg_rate,lasheng_flag) \
            values('{0}','{1}','{2}',\"{3}\",\"{4}\",'{5}','{6}','{7}') " \
              "ON DUPLICATE KEY UPDATE stock_id='{0}',stock_name='{1}',zhuang_grade='{2}',zhuang_section=\"{3}\"," \
              "yidong=\"{4}\",zhuang_long = '{5}' ,max_avg_rate = '{6}',lasheng_flag='{7}' \
            ".format(ids, stock_name,zhuang_grade,zhuang_date,yidong,zhuang_long,max_avg_rate,lasheng_flag)
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
    db_config = read_config('db_config')
    db = pymysql.connect(host=db_config["host"], user=db_config["user"], password=db_config["password"], database=db_config["database"])
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    sql = "select distinct  stock_id,stock_name from stock_informations where h_table = '{0}'".format(h_tab)
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
        # zhuang_grade = 1
        # zhuang_json = {}
        ids = ids_tuple[0]
        if start_t != None and end_t != None:
            sql = "SELECT stock_id,trade_date,open_price,close_price,high_price,low_price,increase  FROM stock_trade_data \
                    where  stock_id  = '{2}' and trade_date >= '{0}' and trade_date <= '{1}' ".format( start_t, end_t,ids)
        else:
            sql = "SELECT stock_id,trade_date,open_price,close_price,high_price,low_price,increase  FROM stock_trade_data \
                    where stock_id  = '{0}'".format(ids)
        df = get_df_from_db(sql, db)
        # print('flag1')
        zhuang_date,zhuang_grade,yidong,zhuang_long,max_avg_rate,lasheng_flag = compt_core(df)
        save(db, ids, ids_tuple[1], zhuang_date, zhuang_grade, yidong,zhuang_long,max_avg_rate,lasheng_flag)
def run(start_t, end_t):
    p = Pool(8)
    for i in range(0, 10):
        p.apply_async(main, args=(str(i), start_t, end_t,))
    #    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
if __name__ == '__main__':
    start_t = None#'2020-01-01'
    end_t = None#'2021-01-14'
    start_time = datetime.datetime.now()

    # h_tab = 9
    # main(h_tab, start_t, end_t)
    run(start_t, end_t)
    print('耗时:', datetime.datetime.now() - start_time)
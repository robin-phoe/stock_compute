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
import pub_uti_a
pd.set_option('display.max_columns', None)
logging.basicConfig(level=logging.DEBUG, filename='../log/comp_zhaung.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

def deal_df_data(df):
    df.fillna(0,inplace=True)
    df['trade_date2'] = df['trade_date'].copy()
    df['trade_date2'] = pd.to_datetime(df['trade_date2']).map(date2num)
    df['dates'] = np.arange(0, len(df))
    df['arv_10'] = df['close_price'].rolling(10).mean()
    df['arv_5'] = df['close_price'].rolling(5).mean()
    df['increase_flag'] = 0
    df['increase_abs'] = 0
    for i in range(1,len(df)-1):
        #涨幅绝对值
        df.loc[i, 'increase_abs'] = abs(float(df.loc[i, 'increase']))
        #DB中历史老数据缺失increase
        df.loc[i, 'increase'] = (df.loc[i,'close_price']-df.loc[i-1,'close_price']) / df.loc[i-1,'close_price']*100
        if -2 <= float(df.loc[i,'increase']) <=2:
            df.loc[i, 'increase_flag'] = 1
    print('df:', df[['increase','increase_flag']])
    return df
def compt_core(df,xielv=0.02,day_rate = 0.7,limit_count = 740,piece = 45,lasheng_pice = 100):
    zhuang_grade = 0
    zhuang_long = 0
    max_avg_rate = 0
    lasheng_flag = 0
    yidong = []
    zhuang_date = []
    lastest_target = '1971-01-01'
    if len(df) <= 200:
        print('少于200条记录')
        return zhuang_date,zhuang_grade,yidong,zhuang_long,max_avg_rate,lasheng_flag,lastest_target
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
        # print('斜率：',abs(behind_cp - front_cp) / front_cp)
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
            #计算是否已拉升过，标准是百分之三十   【bug?】拉升区间判断在df结束，历史数据验证时是否不正确
            if df['close_price'][ind_end:len(df)-1].max() / avg >= 1.3:
                lasheng_flag = 1
            if len(df) - ind_end >60:
                #计算庄线后100个交易日内最大值
                max_value = df['close_price'][ind_end:ind_end+lasheng_pice].max()
                #计算极值对平均值倍数
                beishu = max_value*10 // avg
                print('倍数：',beishu)
                if beishu >= 100:
                    beishu = 99
                zhuang_grade += beishu * 1000000
                #计算极值距离时间
                # max_ind_list = df.query("close_price == '{}'".format(max_value)).index
                max_ind_list = df[df.close_price ==max_value].index.to_list()
                ind_max = (ind_end+lasheng_pice+1) if (ind_end+lasheng_pice+1) < len(df) else len(df)
                max_ind = list(set(max_ind_list) & set(range(ind_end,ind_max)))[0]
                # print('index 列表：',max_ind_list & list(range(ind_end,ind_end+lasheng_pice+1)))
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
        lastest_target = zhuang_date[0][0]
    return zhuang_date,zhuang_grade,yidong,zhuang_long,max_avg_rate,lasheng_flag,lastest_target
#臨時功能，附加計算出section最後時間節點
def com_lastest_point():
    sql = "select stock_id,zhuang_section from com_zhuang where zhuang_grade > 0"
    df = pub_uti_a.creat_df(sql)
    s = pub_uti_a.save()
    def map(raw):
        zhuang_section = eval(raw['zhuang_section'])
        if len(zhuang_section) == 0:
            return raw
        sql = "update com_zhuang set lastest_target= '{0}' where stock_id ='{1}'".format(zhuang_section[0][0],raw['stock_id'])
        s.add_sql(sql)
        return raw
    df.apply(map,axis=1)
    s.commit()
'''
【功能】计算庄线放量信号
庄线最后日期(lastest_target)在120日内
换手前10日平均值的2倍
'''
def com_volume_signal(date=None,long = 120,avg_roll = 10,signal_threshold = 2):
    if date == None:
        sql = "select DATE_FORMAT(max(trade_date),'%Y-%m-%d') from stock_trade_data"
        date = pub_uti_a.select_from_db(sql)[0][0]
    print('date:',date)
    start_date = datetime.datetime.strftime((datetime.datetime.strptime(date[0:10],'%Y-%m-%d') - datetime.timedelta(days=long)),'%Y-%m-%d')
    trade_sql = "select T.stock_id,T.trade_date,T.turnover_rate " \
                " from (select stock_id from com_zhuang where lastest_target>= '{0}') Z " \
                "LEFT JOIN stock_trade_data T " \
                "ON Z.stock_id = T.stock_id " \
                "WHERE T.trade_date >= '{0}' and T.trade_date<= '{1}'".format(start_date,date)
    df = pub_uti_a.creat_df(trade_sql,ascending=True)
    id_set = set(df['stock_id'].to_list())
    volume_signal_map = {}
    clean_sql = "delete from zhuang_day_grade where com_date = '{}'".format(date)
    pub_uti_a.commit_to_db(clean_sql)
    s = pub_uti_a.save()
    for id in id_set:
        single_df = df[df.stock_id == id]
        single_df.reset_index(drop=True,inplace=True)
        single_df['avg'] = single_df['turnover_rate'].rolling(avg_roll).mean()
        single_df['avg'] =single_df['avg'].shift(1)
        single_df['avg'].fillna(100,inplace=True)
        single_df['volume_signal'] = single_df['turnover_rate']/single_df['avg']
        # print('single_df', single_df)
        index_list = single_df[single_df['volume_signal'] >= signal_threshold].index.to_list()
        print('index_list:',index_list)
        if len(index_list)!= 0 and index_list[0] >= (len(single_df)-3):
            trade_code = re.sub('-','',date)+id
            grade = 50
            sql = "insert into zhuang_day_grade (trade_code,com_date,stock_id,grade) " \
                  "VALUES ('{0}','{1}','{2}',{3})".format(trade_code,date,id,grade)
            print('sql:',sql)
            s.add_sql(sql)
            volume_signal_map[id] = grade
    s.commit()
    print('volume_signal_map:',volume_signal_map)

def main(num, start_t, end_t):
    num = str(num)
    if start_t != None and end_t != None:
        sql = "SELECT stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price,increase  FROM stock_trade_data \
                where trade_date >= '{0}' and trade_date <= '{1}' and stock_id like '%{2}'".format(start_t, end_t,num)
    else:
        sql = "SELECT stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price,increase  " \
              "FROM stock_trade_data where stock_id like '%{0}' ".format(num)
    df = pub_uti_a.creat_df(sql,ascending=True)
    id_set = set(df['stock_id'].to_list())
    s = pub_uti_a.save()
    for id in id_set:
        single_df = df[df.stock_id == id]
        single_df.reset_index(drop=True,inplace=True)
        print('single_df:',single_df)
        single_df = deal_df_data(single_df)
        zhuang_date,zhuang_grade,yidong,zhuang_long,max_avg_rate,lasheng_flag,lastest_target = compt_core(single_df)
        insert_sql = "insert into com_zhuang(stock_id,stock_name,zhuang_grade,zhuang_section,yidong,zhuang_long,max_avg_rate,lasheng_flag,lastest_target) " \
              "values('{0}','{1}','{2}',\"{3}\",\"{4}\",'{5}','{6}','{7}','{8}') " \
              "ON DUPLICATE KEY UPDATE stock_id='{0}',stock_name='{1}',zhuang_grade='{2}',zhuang_section=\"{3}\"," \
              "yidong=\"{4}\",zhuang_long = '{5}' ,max_avg_rate = '{6}',lasheng_flag='{7}',lastest_target='{8}' " \
              "".format(id, single_df.loc[0,'stock_name'], zhuang_grade, zhuang_date, yidong, zhuang_long, max_avg_rate, lasheng_flag ,lastest_target)
        s.add_sql(insert_sql)
    s.commit()
def run(start_t, end_t):
    p = Pool(10)
    for i in range(0, 10):
        p.apply_async(main, args=(i, start_t, end_t,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
if __name__ == '__main__':
    start_t = None#'2020-01-01'
    end_t = None#'2021-01-14'
    start_time = datetime.datetime.now()

    run(start_t, end_t)
    com_lastest_point()
    com_volume_signal()
    print('耗时:', datetime.datetime.now() - start_time)
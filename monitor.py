import redis
import pymysql
import logging
import time
import datetime
from multiprocessing import Pool
from wxpy import *
import pandas as pd
import numpy as np
import mpl_finance
import matplotlib.pyplot as plt
from matplotlib import ticker
import re

logging.basicConfig(level=logging.DEBUG, filename='monitor.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
#登录微信
bot = Bot(cache_path=True)
monitor_info_dict = {}
redu_5_info_dict = {}
redu_init_info_dict = {}
def write_init_data():
    global r,db
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    sql  = "select max(trade_date) from com_redu"
    cursor.execute(sql)  # 执行SQL语句
    yesterday = cursor.fetchall()[0][0].strftime("%Y-%m-%d")
    print('yesterday:',yesterday)
    # #查询需要庄线监控的信息
    sql ="select stock_id,stock_name,zhuang_grade from com_zhuang where zhuang_grade >= 1000 and zhuang_grade <10000 " \
         "and lasheng_flag = 0"
    cursor.execute(sql)  # 执行SQL语句
    monitor_list = cursor.fetchall()
    ##查询热度五日线监控信息
    sql = "select stock_id,stock_name,redu_5 from com_redu where redu_5 > 0 and trade_date = '{}'".format(yesterday)
    cursor.execute(sql)  # 执行SQL语句
    redu_5_list = cursor.fetchall()
    ##监控热度init
    sql = "select stock_id,stock_name,avg_5 from com_redu where redu_init = 1 and trade_date = '{}'".format(yesterday)
    cursor.execute(sql)  # 执行SQL语句
    redu_init_list = cursor.fetchall()
    cursor.close()
    # print('monitor_list:',monitor_list)
    # 清空monitor_list
    r.ltrim('monitor_list',0,0)
    r.lpop('monitor_list')
    for stock in monitor_list:
        #记录所有需要监控的id
        r.lpush('monitor_list',stock[0])
        # 写入信息字典
        monitor_info_dict[stock[0]] = stock
    # for stock in redu_5_list:
    #     #记录所有需要监控的id
    #     r.lpush('monitor_list',stock[0])
    #     redu_5_info_dict[stock[0]] = stock
    for stock in redu_init_list:
        #记录所有需要监控的id
        r.lpush('monitor_list',stock[0])
        redu_init_info_dict[stock[0]] = stock
    # v = r.lpush('monitor_list', '000002')
    # print('v:',v)
    # print('monitor_info_dict:',monitor_info_dict)
    print('monitor_list_len:',len(monitor_info_dict))
    print('redu_5_list_len:', len(redu_5_info_dict))
    print('redu_init_list_len:', len(redu_init_info_dict))
    # print('monitor_list:',r.lrange('monitor_list',0,r.llen('monitor_list')))

def wx_send_message(message,image_path):
    global bot
    # my_groups = bot.groups().search(u'7个涨停翻一番')[0]
    my_groups = bot.friends().search(u'7个涨停翻一番')[0]
    my_groups.send(message)
    my_groups.send_image(image_path)
    time.sleep(1)
def get_df_from_db(sql):
    global db
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    #df = df.set_index(keys=['trade_date'])
    df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
    df.reset_index(inplace=True)
    df['trade_date2'] = df['trade_date'].copy()
    df['trade_date'] = [x.strftime('%Y-%m-%d') for x in df['trade_date']]
    # df['trade_date'] = pd.to_datetime(df['trade_date']).map(date2num)
    df['dates'] = np.arange(0, len(df))
    cursor.close()
    # print("df:",df)
    # df['trade_date'] = date2num(df['trade_date'])
    return df
def draw_k_line(id,inform_type):
    global db
    cursor = db.cursor()
    def comput_ind(df, time):
        time = time[0:10]
        ind = df.query("trade_date == '{}'".format(time))
        print("comput_ind:", df['trade_date'][0], type(df['trade_date'][0]), time, type(time))
        if len(ind) > 0:
            return ind.index[0]
        else:
            return 0
    sql ="select h_table,stock_name,bk_name from stocK_informations where stock_id = {}".format(id)
    cursor.execute(sql)
    info_res= cursor.fetchall()
    print('info_res:',info_res)
    h_tab  = info_res [0][0]
    stock_name = info_res[0][1]
    stock_name = re.sub('\*','',stock_name)
    bk_name = info_res[0][2]
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    sql ="select zhuang_section,zhuang_grade from com_zhuang where stock_id = {}".format(id)
    cursor.execute(sql)
    zhuang_res= cursor.fetchall()
    zhuang_section =  zhuang_res[0][0]
    zhuang_grade = zhuang_res[0][1]
    chart_title = '{0} {1} {2} {3}'.format(id, stock_name, bk_name,zhuang_grade)
    sql = "SELECT trade_date,open_price,close_price,high_price,low_price  FROM stockdb.stock_history_trade{0} \
            where trade_date >= '{1}' and trade_date <= '{2}' and  stock_id = '{3}'".format(h_tab, '2020-01-01', end_date,
                                                                                            id)
    df = get_df_from_db(sql)
    cursor.close()
    df['5'] = df['close_price'].rolling(5).mean()
    def format_date(x,pos):
        if x<0 or x>len(date_tickers)-1:
            return ''
        return date_tickers[int(x)]

    date_tickers = df.trade_date2.values
    plt.rcParams['font.sans-serif'] = ['KaiTi']
    plt.rcParams['axes.unicode_minus'] = False
    fig, ax = plt.subplots(figsize=(23,5))
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    ax.set_title(chart_title, fontsize=20)
    # 绘制K线图
    mpl_finance.candlestick_ochl(
        ax=ax,
        quotes=df[['dates', 'open_price', 'close_price', 'high_price', 'low_price']].values,
        width=0.7,
        colorup='r',
        colordown='g',
        alpha=0.7)

    plt.plot(df['dates'], df['5'])

    print('zhuang_section:',zhuang_section)
    for zhaung_tup in eval(zhuang_section):
        print('zhaung_tup:',zhaung_tup)
        sta = comput_ind(df, zhaung_tup[1])
        end = comput_ind(df, zhaung_tup[0])
        print('indexs:',sta,end )
        plt.plot(df['dates'][sta:end], df['5'][sta:end] ,color='green')
    plt.legend();
    image_path ='./pic/{0}{1}{2}{3}.jpg'.format(id,stock_name,end_date,inform_type)
    plt.savefig(image_path)
    message = '{0} {1} {2} ! zhuang_grade:{3}。涨幅：'.format(id,stock_name,inform_type,zhuang_grade)
    return message, image_path

def monitor_core_increase(monitor_type,id):
    increase = r.lindex(id+'_increase_list',0)
    if increase == None or increase == '-':
        return
    inform_type = ''
    if monitor_type == 'zhuang':
        grade = monitor_info_dict[id][2]
        stock_name= monitor_info_dict[id][1]
    elif monitor_type == 'redu_5':
        grade = redu_5_info_dict[id][2]
        stock_name = redu_5_info_dict[id][1]
    if monitor_type in ('zhuang','redu_5') :
        if increase != '-' and increase != None and float(increase) >= 5:
            if r.get(id + monitor_type + '_flag_5') != '1' :
                print('flag_3:',r.get(id + monitor_type + '_flag_5'))
                # print(id,'增长超过%3!：',increase)
                print('time:', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                print('{0}:{1} {2} 增长超过%5!：{3}  grade:{4}'.format(monitor_type,id,stock_name,increase,grade))
                r.set(id + monitor_type + '_flag_5','1')
                inform_type = monitor_type + '_inc_5'
            else:
                return
        elif increase != '-' and increase != None and float(increase) >= 3:
            if r.get(id + monitor_type + '_flag_2') != '1' :
                print('flag_2:',r.get(id + monitor_type + '_flag_2'))
                # print(id,'增长超过%3!：',increase)
                print('time:',datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                print('{0}:{1} {2} 增长超过%3!：{3}  grade:{4}'.format(monitor_type,id,stock_name,increase,grade))
                r.set(id + monitor_type + '_flag_2','1')
                inform_type = monitor_type + '_inc_3'
            else:
                return
        else:
            return
    # elif monitor_type == 'redu_init' :
    #     # print('redu_init_info_dict:',redu_init_info_dict)
    #     avg_5 = redu_init_info_dict[id][2]
    #     last_price = r.lindex(id+'_price_list',0)
    #     if  last_price != '-' and last_price != None and float(last_price) <= avg_5:
    #         if r.get(id + monitor_type + '_flag_y5') != '1' :
    #             print('flag_y5:',r.get(id + monitor_type + '_flag_y5'))
    #             # print(id,'增长超过%3!：',increase)
    #             print('time:',datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    #             print('{0},{1} redu_init flag_y5! increase:{2} '.format(id,redu_init_info_dict[id][1],increase))
    #             r.set(id + monitor_type + '_flag_y5','1')
    #             inform_type = monitor_type + '_inc_y5'
    #         else:
    #             return
    #     # elif increase != '-' and increase != None and float(increase) <= 0:
    #     #     if r.get(id + monitor_type + '_flag_0') != '1' :
    #     #         print('flag_0:',r.get(id + monitor_type + '_flag_0'))
    #     #         # print(id,'增长超过%3!：',increase)
    #     #         print('time:',datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    #     #         print('{0},{1} redu_init 涨幅为0!：{2} '.format(id,redu_init_info_dict[id][1],increase))
    #     #         r.set(id + monitor_type + '_flag_0','1')
    #     #         inform_type = monitor_type + '_inc_0'
    #     #     else:
    #     #         return
    #     else:
    #         return

    else:
        return
    #wx send
    message, image_path = draw_k_line(id,inform_type)
    message = monitor_type +':'+ message + increase
    wx_send_message(message, image_path)
def monitor_main():
    monitor_list = r.lrange('monitor_list',0,-1)
    for id in monitor_info_dict:
        monitor_core_increase('zhuang',id)
    for id in redu_5_info_dict:
        monitor_core_increase('redu_5',id)
    # for id in redu_init_info_dict:
    #     monitor_core_increase('redu_init', id)
def test_redis():
    # r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
    # v = r.lpush('Zarten1', 1, 2, 3, 4, 5)
    # print('v:',v)
    # v = r.lpush('12601', [1,2])
    # print('v:',v)
    # print('list',r.lrange('12601',0,4))
    # print(r.get('300457_price'))
    # r.ltrim('Zarten1',0,0)
    # r.lpop('Zarten1')
    # print('len:',r.llen('Zarten1'))
    # print('Zarten1:',r.lrange('Zarten1',0,0))
    print('Zarten1:', r.lindex('Zarten1', 0))
if __name__ == '__main__':
    # test_redis()
    write_init_data()
    while True:
        monitor_main()
        time.sleep(1)

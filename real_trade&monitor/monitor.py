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

logging.basicConfig(level=logging.DEBUG, filename='../log/monitor.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
db = pymysql.connect(host="192.168.1.6", user="user1", password="Zzl08382020", database="stockdb")

monitor_info_dict = {}
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
class creat_df_from_db:
    def __init__(self):
        pass
    def creat_df(self,sql):
        global db
        cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
        cursor.execute(sql)  # 执行SQL语句
        data = cursor.fetchall()
        # 下面为将获取的数据转化为dataframe格式
        columnDes = cursor.description  # 获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
        df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
        df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
        df.reset_index(inplace=True)
        cursor.close()
        return df
class stock:
    in_bk_rank = 0
    bk_increase = 0
    increase = 0
    new_price = 0
    monitor_inc3_flag = False
    monitor_inc5_flag = False
    monitor_fast_flag = False
    def __init__(self,stock_name,stock_id,bk_name,increase,new_price,monitor_type):
        self.stock_name = stock_name
        self.stock_id = stock_id
        self.bk_name = bk_name
        self.monitor_type = monitor_type
class bk:
    increase = 0 #板块增长
    amount = 0 #板块成交量
    member_trade_info = {} #成员实时交易信息
    member_rank = {} #板块内成员排序
    def __init__(self,name,id,member):
        self.name = name
        self.id = id
        self.member = member
    def __get_member_trade_info(self):
        pass
    def __sort_member(self):
        pass
    def get_bk_info(self):
        return (self.name,self.id,self.member,self.increase,self.amount)
    def get_stock_for_bk(self,stock_id):
        self.__sort_member()
        rank = self.member_rank[stock_id]
        return (increase,amount,rank)
class stock_buffer:
    stock_dict = {}  # {stock_id:instance}
    def __init__(self):
        pass
    def __select_monitor(self):
        global r, db
        cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
        sql = "select max(trade_date) from monitor"
        cursor.execute(sql)  # 执行SQL语句
        yesterday = cursor.fetchall()[0][0].strftime("%Y-%m-%d")
        cursor.close()
        logging.info('monitor date:{}'.format(yesterday))
        print('monitor date:{}'.format(yesterday))
        sql = "select M.stock_id,M.stock_name,M.grade,M.monitor_type,I.bk_name from monitor M " \
              " LEFT JOIN stock_informations I " \
              "ON M.stock_id = I.stock_id " \
              " where trade_date = '{}' and monitor=1 ".format(
            yesterday)
        made_df = creat_df_from_db()
        df = made_df.creat_df(sql)
        return df
    def fill_stock_buffer(self):
        df = self.__select_monitor()
        def write_instance(raw):
            stoc = stcok(stock_name = raw['stock_name'],stock_id = raw['stock_id'],bk_name = raw['bk_name'],
                         monitor_type = raw['monitor_type'])
            stock_dict[raw['stock_id']] = stoc
        df.apply(write_instance,axis=1)


class bk_buffer:
    bk_dict = {} #{bk_id:instance}
    def __init__(self):
        pass
    def __select_bk_info(self):
        #待优化，stock_information 表中没有bankuai_code ，须补上
        sql = "select I.stock_id,I.bk_name,B.bankuai_code from stock_informations I " \
              " LEFT JOIN bankuai_day_data B " \
              " ON I.bk_name = B.bk_name "
        made_df = creat_df_from_db()
        df = made_df.creat_df(sql)
        return df
    def fill_bk_buffer(self):
        df = self.__select_bk_info()
        bk_set = set(df['bankuai_name'].tolist())
        def write_instance(raw,bk_instance,bk_name,axis=1):
            if raw['bankuai_name'] == bk_name:
                bk_instance.name = raw['bankuai_code']
                bk_instance.member.append(raw['stock_id'])
        for bk_name in bk_set:
            bk_instance = bk(name = bk_name,id = 'fill',member = [])
            df.apply(write_instance,bk_instance,bk_name)
            self.bk_dict[bk_name] = bk_instance
    def set_bk_instance(self,bk_id,instance):
        self.bk_dict[bk_id] = instance
    def get_bk_instance(self,stock_id):
        if stock_id not in self.bk_dict:
            return false
        else:
            return self.bk_dict[stock_id]
    def get_buffer_all_key(self):
        return self.bk_dict.keys()
class wx_send_message:
    def __init__(self):
        self.bot = Bot(cache_path=True)
    def send_message(self,message,image_path):
        # print('group:',bot.groups(),bot.groups().search(u'有赚就行'))
        my_groups = self.bot.groups().search(u'有赚就行')[0]
        # my_groups = bot.friends().search(u'7个涨停翻一番')[0]
        my_groups.send(message)
        my_groups.send_image(image_path)
        time.sleep(1)
class draw_k_line:
    def __init__(self):
        pass

def write_init_data():
    global r,db
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    sql  = "select max(trade_date) from monitor"
    cursor.execute(sql)  # 执行SQL语句
    yesterday = cursor.fetchall()[0][0].strftime("%Y-%m-%d")
    print('yesterday:',yesterday)
    # #查询需要庄线监控的信息
    sql ="select stock_id,stock_name,grade,monitor_type from monitor where trade_date = '{}' and monitor=1 ".format(yesterday)
    cursor.execute(sql)  # 执行SQL语句
    monitor_list = cursor.fetchall()
    # 清空redis monitor_list
    r.ltrim('monitor_list',0,0)
    r.lpop('monitor_list')
    for stock in monitor_list:
        #记录所有需要监控的id
        r.lpush('monitor_list',stock[0])
        # 写入信息字典
        monitor_info_dict[stock[0]] = stock
    print('monitor_list_len:',len(monitor_info_dict))
    cursor.close()
def wx_send_message(message,image_path):
    global bot
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
    if len(zhuang_res) == 0:
        return ''
    zhuang_section =  zhuang_res[0][0]
    zhuang_grade = zhuang_res[0][1]
    chart_title = '{0} {1} {2} {3}'.format(id, stock_name, bk_name,zhuang_grade)
    sql = "SELECT trade_date,open_price,close_price,high_price,low_price  FROM stock_trade_data \
            where trade_date >= '{0}' and  stock_id = '{1}' and trade_date <= '{2}' ".format('2020-01-01',id, end_date)
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
    image_path ='../pic/{0}{1}{2}{3}.jpg'.format(id,stock_name,end_date,inform_type)
    plt.savefig(image_path)
    message = '{0} {1} {2} ! zhuang_grade:{3}。涨幅：'.format(id,stock_name,inform_type,zhuang_grade)
    return message, image_path
def monitor_core_increase(id):
    increase = r.lindex(id+'_increase_list',0)
    if increase == None or increase == '-':
        return
    inform_type = ''
    grade = monitor_info_dict[id][2]
    stock_name= monitor_info_dict[id][1]
    monitor_type = monitor_info_dict[id][3]
    if monitor_type in ('zhuang','remen_xiaoboxin') :
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
        elif increase != '-' and increase != None and float(increase) >= 2.5:
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

    else:
        return
    #wx send
    if draw_k_line(id,inform_type) == '':
        return
    message, image_path = draw_k_line(id,inform_type)
    message = monitor_type +':'+ message + increase
    wx_send_message(message, image_path)
def monitor_main():
    monitor_list = r.lrange('monitor_list',0,-1)
    for id in monitor_info_dict:
        monitor_core_increase(id)
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

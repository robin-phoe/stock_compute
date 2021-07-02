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
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config

logging.basicConfig(level=logging.CRITICAL, filename='../log/monitor.log', filemode='a',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
db_config = read_config('db_config')
db = pymysql.connect(host=db_config["host"], user=db_config["user"], password=db_config["password"],
                     database=db_config["database"])

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
        if 'trad_date' in df.columns:
            df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
            df.reset_index(inplace=True)
        cursor.close()
        return df
class stock:
    def __init__(self,stock_name,stock_id,bk_name,monitor_type,grade,monitor):
        self.stock_name = stock_name
        self.stock_id = stock_id
        self.bk_name = bk_name
        self.monitor_type = monitor_type
        self.grade = grade
        self.monitor = monitor #1为监控
        self.in_bk_rank = 0
        self.bk_increase = 0
        self.new_increase = 0
        self.increase = 0
        self.new_price = 0
        self.price = 0
        self.high_price = 0
        self.low_price = 0
        self.open_price = 0
        self.price_list = []  # 从左边填入
        self.monitor_inc3_flag = 'True'
        self.monitor_inc5_flag = 'True'
        self.monitor_fast_flag = 'True'
        self.message = None
        self.inform_type = None
        self.inform_flag = False  # 是否触发了通告
        self.modify_flag = False  # 修改数据时间改为True
        self.chart_title = None
        self.get_status_from_redis()  # 从redis验证monitor_flag
    def get_real_data(self):
        # logging.info('stock_id:{}'.format(self.stock_id))
        len_pre = r.llen('{}_price_list'.format(self.stock_id))
        """判断redis中是否开始有行情"""
        if len_pre == 0:
            return False
        """判断class中price_list是否完整"""
        if len(self.price_list) < len_pre:
            self.price_list = r.lrange('{}_price_list'.format(self.stock_id),0,-1)
        else:
            self.price_list.append(self.new_price)
        self.new_price = float(r.lindex('{}_price_list'.format(self.stock_id),0))
        self.new_increase = float(r.lindex('{}_increase_list'.format(self.stock_id),0))
        self.open_price = float(r.hget('open_price',self.stock_id))
        self.high_price = float(r.hget('high_price', self.stock_id))
        self.low_price = float(r.hget('low_price', self.stock_id))
        return True
    def refresh_data(self):
        if not self.get_real_data():
            return False
        if self.price == self.new_price:
            return
        self.increase = self.new_increase
        self.price = self.new_price
        self.modify_flag = True
        # self.compute_monitor()
    """当modify_flag为True，进行计算"""
    def compute_monitor(self):
        if self.monitor != 1:
            return
        if self.increase >= 5:
            if self.monitor_inc5_flag == 'True':
                self.inform_type = '涨幅超过5%'
                self.monitor_inc5_flag = False
                r.hset('monitor_flag', '{}_inc5_flag'.format(self.stock_id),'False')
            else:
                return
        elif self.increase >= 2.5:
            if self.monitor_inc3_flag == 'True':
                self.inform_type = '涨幅超过2.5%'
                self.monitor_inc3_flag = False
                r.hset('monitor_flag', '{}_inc3_flag'.format(self.stock_id), 'False')
            else:
                return
        else:
            self.modify_flag = False
            return
        """获取板块信息"""
        bk = bk_buffer.get_bk_instance(self.bk_name)
        if bk:
            self.bk_increase = bk.increase
            self.in_bk_rank = bk.get_rank_in_bk(self.stock_id)
            logging.debug('bk name:{},mem_len:{},mem_set_len:{},bk_members:{}'.format(bk.name,len(bk.members),len(set(bk.members)),bk.members))
        else:
            print('{0} {1} Not exist!'.format(self.stock_name,self.bk_name))
            logging.error('{0} {1} Not exist!'.format(self.stock_name,self.bk_name))
        self.message = "【{0}】{1} 通知原因：{2} ! 涨幅：{3}。监控类型：{4}。分数：{5}。板块：{6}。板块涨幅：{7}。板块内排名：{8}。".format(
            self.stock_name,self.stock_id,self.inform_type,self.increase,self.monitor_type,self.grade,self.bk_name,self.bk_increase,
            self.in_bk_rank
        )
        self.chart_title = '{0} {1} {2} {3}'.format(self.stock_id, self.stock_name, self.bk_name,self.grade)
        self.inform_flag = True
        self.modify_flag = False
    """从redis验证monitor_flag"""
    def get_status_from_redis(self):
        inc5_flag = r.hget('monitor_flag','{}_inc5_flag'.format(self.stock_id))
        if inc5_flag != None:
            self.monitor_inc5_flag = inc5_flag
        inc3_flag = r.hget('monitor_flag', '{}_inc3_flag'.format(self.stock_id))
        if inc3_flag != None:
            self.monitor_inc3_flag = inc3_flag
class bk:
    def __init__(self,name,id,members):
        self.name = name
        self.id = id
        self.members = members #列表
        self.increase = 0  # 板块增长
        self.amount = 0  # 板块成交量
        self.member_real_info = {}  # 成员实时交易信息
        self.member_rank = {}  # 板块内成员排序
        self.mem_count = 0
    def __get_member_real_info(self):
        for mem in self.members:
            if mem in stock_buffer.stock_dict:
                stock = stock_buffer.stock_dict[mem]
                self.member_real_info[mem] = float(stock.increase)
            else:
                logging.error('bk:{} member:{}  not in stock_buffer.stock_dict:{}'.format(self.name,mem,stock_buffer.stock_dict))
    def __get_bk_real_info(self):
        inc = r.hget('bk_increase',self.id)
        if inc == None:
            self.increase = 0
        else:
            self.increase = inc
    def __sort_member(self):
        mem_list = sorted(self.member_real_info.items(), key=lambda d: d[1], reverse=True) #倒序
        # logging.info('mem_list:{}'.format(mem_list))
        for i in range(len(mem_list)):
            self.member_rank[mem_list[i][0]] = i+1
        # logging.info('member_rank:{}'.format(self.member_rank))
    def refresh_bk_info(self):
        self.__get_member_real_info()
        self.__get_bk_real_info()
        self.__sort_member()
    def get_bk_info(self):
        return (self.name,self.id,self.member,self.increase,self.amount)
    def get_rank_in_bk(self,stock_id):
        self.__sort_member()
        rank = "{}/{}".format(self.member_rank[stock_id],self.mem_count)
        return rank
class stock_buffer:
    def __init__(self):
        self.stock_dict = {}  # {stock_id:instance}
    def __select_monitor(self):
        global r, db
        cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
        sql = "select max(trade_date) from monitor"
        cursor.execute(sql)  # 执行SQL语句
        yesterday = cursor.fetchall()[0][0].strftime("%Y-%m-%d")
        cursor.close()
        logging.info('监控日期:{}'.format(yesterday))
        print('监控日期:{}'.format(yesterday))
        sql = "select M.monitor,I.stock_id,I.stock_name,M.grade,M.monitor_type,I.bk_name from stock_informations I " \
              " INNER JOIN (select * from monitor  where trade_date = '{}') M" \
              " ON M.stock_id = I.stock_id " .format(yesterday)
        made_df = creat_df_from_db()
        df = made_df.creat_df(sql)
        print('监控数量：{}'.format(len(df)))
        return df
    def init_stock_buffer(self):
        df = self.__select_monitor()
        def write_instance(raw):
            stoc = stock(stock_name = raw['stock_name'],stock_id = raw['stock_id'],bk_name = raw['bk_name'],
                         monitor_type = raw['monitor_type'],grade = raw['grade'],monitor = raw['monitor'])
            self.stock_dict[raw['stock_id']] = stoc
        df.apply(write_instance,axis=1)
    def put_stock_instance(self,id):
        if id not in self.stock_dict:
            return False
        else:
            return self.stock_dict[id]
class bk_buffer:
    def __init__(self):
        self.bk_dict = {} #{bk_id:instance}
    def __select_bk_info(self):
        #待优化，stock_information 表中没有bk_code ，须补上
        sql = "select I.stock_id,I.bk_name,B.bk_code from stock_informations I " \
              " LEFT JOIN (select distinct bk_name,bk_code from bankuai_day_data) B " \
              " ON I.bk_name = B.bk_name "
        made_df = creat_df_from_db()
        df = made_df.creat_df(sql)
        print('df:',df)
        return df
    def init_bk_buffer(self):
        df = self.__select_bk_info()
        bk_set = set(df['bk_name'].tolist())
        def write_instance(raw,bk_instance,bk_name):
            if raw['bk_name'] == bk_name:
                bk_instance.name = raw['bk_name']
                if raw['bk_code'] != None:
                    bk_instance.id = raw['bk_code']
                else:
                    bk_instance.id = 'B000'
                if raw['stock_id'] not in bk_instance.members:
                    bk_instance.members.append(raw['stock_id'])
        for bk_name in bk_set:
            bk_instance = bk(name = bk_name,id = 'fill',members = [])
            df.apply(write_instance,args=(bk_instance,bk_name),axis=1)
            bk_instance.mem_count = len(bk_instance.members)
            self.bk_dict[bk_name] = bk_instance
    def set_bk_instance(self,bk_name,instance):
        self.bk_dict[bk_name] = instance
    def get_bk_instance(self,bk_name):
        if bk_name not in self.bk_dict:
            return False
        else:
            return self.bk_dict[bk_name]
    def get_buffer_all_key(self):
        return self.bk_dict.keys()
class wx_send_message:
    def __init__(self):
        self.bot = Bot(cache_path=True)
        self.receiver_dic = {'remen_xiaoboxin':u'热门小波形','zhuang':u'庄线','single_limit_retra':u'单涨停回撤','remen_boxin':u'热门波形','remen_retra':u'热门回撤',}
    def send_message(self,message,image_path,monitor_type):
        # print('group:',bot.groups(),bot.groups().search(u'有赚就行'))
        if monitor_type in self.receiver_dic:
            my_groups = self.bot.friends().search(self.receiver_dic[monitor_type])[0]
            # my_groups = self.bot.groups().search(u'有赚就行')[0]
        else:
            # my_groups = self.bot.friends().search(u'7个涨停翻一番')[0]
            my_groups = self.bot.groups().search(u'有赚就行')[0]
        my_groups.send(message)
        my_groups.send_image(image_path)
        time.sleep(1)
class draw_k_line:
    def __init__(self):
        self.id = None
        self.df = None
        self.info_df = None
        self.image_path = None
        self.to_day = None
    def select_df(self):
        cf = creat_df_from_db()
        self.to_day = datetime.datetime.now().strftime('%Y%m%d')
        data_sql = "SELECT trade_date,open_price,close_price,high_price,low_price  " \
              " FROM stock_trade_data " \
              " where trade_date >= '{0}' and  stock_id = '{1}' and trade_date <= '{2}' ".format('2020-01-01',self.id, self.to_day)
        self.df = cf.creat_df(data_sql)
        info_sql = "SELECT I.stock_name,I.bk_name,Z.zhuang_section,Z.zhuang_grade " \
                   " FROM stock_informations I " \
                   " LEFT JOIN com_zhuang Z " \
                   " ON I.stock_id = Z.stock_id " \
                   " WHERE I.stock_id = '{}'".format(self.id)
        self.info_df = cf.creat_df(info_sql)
    def insert_today_trade(self,open,high,low,new_price):
        today_date = datetime.datetime.now()
        self.df.loc[len(self.df)] = [today_date,open,new_price,high,low]
    def __comput_ind(self,time):
        time = time[0:10]
        ind = self.df.query("trade_date == '{}'".format(time))
        print("comput_ind:", self.df['trade_date'][0], type(self.df['trade_date'][0]), time, type(time))
        if len(ind) > 0:
            return ind.index[0]
        else:
            return 0
    def draw_image(self,stock_id,chart_title,open,high,low,new_price):
        self.id = stock_id
        self.select_df()
        self.insert_today_trade(open,high,low,new_price)
        stock = stock_buffer.put_stock_instance(self.id)
        self.df['dates'] = np.arange(0, len(self.df))
        self.df['5'] = self.df['close_price'].rolling(5).mean()
        def format_date(x,pos):
            if x<0 or x>len(date_tickers)-1:
                return ''
            return date_tickers[int(x)]
        date_tickers = self.df.trade_date.values
        plt.rcParams['font.sans-serif'] = ['KaiTi']
        plt.rcParams['axes.unicode_minus'] = False
        fig, ax = plt.subplots(figsize=(23,5))
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
        ax.set_title(chart_title, fontsize=20)
        # 绘制K线图
        mpl_finance.candlestick_ochl(
            ax=ax,
            quotes=self.df[['dates', 'open_price', 'close_price', 'high_price', 'low_price']].values,
            width=0.7,
            colorup='r',
            colordown='g',
            alpha=0.7)
        plt.plot(self.df['dates'], self.df['5'])
        zhuang_section = self.info_df.loc[0,'zhuang_section']
        # print('zhuang_section:', zhuang_section)
        try:
            zhuang_section = eval(zhuang_section)
        except Exception as err:
            if zhuang_section == None:
                zhuang_section = []
            logging.error('ERR:{} zhuang_section:{},df:{}'.format(err,zhuang_section,self.info_df))
            print('ERR:{} zhuang_section:{},df:{}'.format(err,zhuang_section,self.info_df))
        for zhaung_tup in zhuang_section:
            sta = self.__comput_ind(zhaung_tup[1])
            end = self.__comput_ind(zhaung_tup[0])
            print('indexs:', sta, end)
            plt.plot(self.df['dates'][sta:end], self.df['5'][sta:end], color='green')
        plt.legend();
        # self.image_path = '../pic/{0}{1}{2}{3}.jpg'.format(stock.stock_id, stock.stock_name, self.to_day, stock.inform_type)
        self.image_path = '../pic/{0}{1}{2}.jpg'.format(stock.stock_id,self.to_day,
                                                           stock.inform_type)
        plt.savefig(self.image_path)
class main:
    # 实例化wx
    wx_send = wx_send_message()
    def __init__(self):
        pass
    def run_once(self):
        start_time = datetime.datetime.now()
        for stock_id in stock_buffer.stock_dict:
            stock_buffer.stock_dict[stock_id].refresh_data()
        for bk_name in bk_buffer.bk_dict:
            bk_buffer.bk_dict[bk_name].refresh_bk_info()
        """计算触发通知"""
        for stock_id in stock_buffer.stock_dict:
            stock = stock_buffer.stock_dict[stock_id]
            stock.compute_monitor()
            # print(stock.stock_name,stock.increase,stock.inform_flag)
            if stock.inform_flag:
                dk = draw_k_line()
                dk.draw_image(stock.stock_id,stock.chart_title,stock.open_price,stock.high_price,stock.low_price,stock.new_price)
                self.wx_send.send_message(stock.message,dk.image_path,stock.monitor_type)
                logging.debug('message:{}'.format(stock.message))
                print('message:{}'.format(stock.message))
                stock.inform_flag = False
                del dk
        print('耗时：',datetime.datetime.now() - start_time)
    def run(self):
        while True:
            self.run_once()
            time.sleep(1)

#buffer 实例化及数据初始化
stock_buffer = stock_buffer()
print('flag1')
bk_buffer = bk_buffer()
print('flag2')
stock_buffer.init_stock_buffer()
print('flag3')
bk_buffer.init_bk_buffer()
m = main()
print('flag4')
m.run()

if __name__ == '__main__':
    pass

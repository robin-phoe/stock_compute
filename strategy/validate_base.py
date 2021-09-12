#后一日hight price >=3% 触发监控，计触发数1，open price + 3% 计算为call_price
#触发后一日open price 计为 put_open_price, close price 计为 put_close_price
#pl_layer1: (,0] ;  pl_layer2: (0,2.5) ; pl_layer3:[2.5,6),pl_layer4:[6,)
import logging
import pymysql
import pandas as pd
import datetime
import re
import pub_uti_a
import mpl_finance
import matplotlib.pyplot as plt
from matplotlib import ticker
import numpy as np

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)

logging.basicConfig(level=logging.INFO, filename='../log/validate_remen_xiaoboxin.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

class validate_buffer:
    def __init__(self,vali_start,vali_end):
        self.trade_df = None
        self.vali_df = None
        self.result_df = None
        self.stcoK_set = set()
        self.vali_start = vali_start
        self.vali_end = vali_end
        self.vali_range_day = 10
        self.trade_date_start = self.vali_start
        self.trade_date_end = datetime.datetime.strptime(self.vali_end , '%Y-%m-%d') + datetime.timedelta(days = self.vali_range_day+1)
        self.report_file_name = './validate_report/validate_retacement.xlsx'
        self.vali_table = 'remen_retracement'
        self.grade = 10001
    def com(self):
        self.get_trade_date()
        self.get_vali_stock()
        self.create_reslut_df()
        self.init_stock()
    def get_trade_date(self):
        sql = "select * from stock_trade_data where trade_date >= '{0}' and trade_date <= '{1}'".format(
            self.trade_date_start,self.trade_date_end)
        self.trade_df = pub_uti_a.creat_df(sql)
        self.trade_df['mod_price'] = (self.trade_df['high_price'] + self.trade_df['low_price'])/2
    def get_vali_stock(self):
        sql = "select * from {0} where trade_date >='{1}' and trade_date <='{2}' and grade >= '{3}'".format(
            self.vali_table,self.vali_start,self.vali_end,self.grade)
        # sql = "select * from {0} where trade_date >='{1}' and trade_date <='{2}' and stock_id = '600844' and grade >= '{3}'".format(
        #     self.vali_table,self.vali_start,self.vali_end,self.grade)
        self.vali_df = pub_uti_a.creat_df(sql)
        self.stcoK_set = set(self.vali_df['stock_id'].to_list())
    def init_stock(self):
        for stock_id in self.stcoK_set:
            vali_single_df = self.vali_df.loc[self.vali_df.stock_id == stock_id]
            vali_single_df.reset_index(inplace=True)
            trade_single_df = self.trade_df.loc[self.trade_df.stock_id == stock_id]
            trade_single_df.reset_index(inplace=True)
            st_b = stock_buffer(vali_single_df,trade_single_df)
            self.result_df = st_b.commput(self.result_df)
        # print('result_df:',self.result_df)
        # self.result_df.to_csv(self.report_file_name,encoding='utf_8_sig',index=False)
        writer = pd.ExcelWriter(self.report_file_name)
        self.result_df.to_excel(writer, encoding='utf_8_sig', index=False)
        writer.save()
    def create_reslut_df(self):
        data = {'trade_code': [],'trade_date': [],'stock_id': [],'stock_name': [],'grade': [],'call_price': [],'low_inc_1': []
            , 'high_inc_1': [],'mod_inc_1': [],'low_inc_2': [],'high_inc_2': [],'mod_inc_2': [],'low_inc_3': [],
                'high_inc_3': [],'mod_inc_3': []
                }
        self.result_df = pd.DataFrame(data)
class stock_buffer:
    def __init__(self,vali_single_df,trade_single_df):
        self.stock_id = vali_single_df.loc[0,'stock_id']
        self.stock_name = vali_single_df.loc[0,'stock_name']
        self.vali_single_df = vali_single_df
        self.vali_date_list = []
        self.trade_single_df = trade_single_df
    def commput(self,result_df):
        for index,raw in self.vali_single_df.iterrows():
            st = stock(raw,self.trade_single_df)
            if not st.compute():
                continue
            raw_list = [raw['trade_code'],st.trade_date,self.stock_id,self.stock_name,st.grade,st.call_price,
                        st.low_inc_1,st.high_inc_1,st.mod_inc_1,
                        st.low_inc_2,st.high_inc_2,st.mod_inc_2,
                        st.low_inc_3,st.high_inc_3,st.mod_inc_3]
            print('raw_list:',raw_list)
            result_df.loc[len(result_df)] = raw_list
        return result_df
class stock:
    def __init__(self,raw,trade_single_df):
        self.trade_single_df = trade_single_df
        self.raw = raw
        self.trade_date = raw['trade_date']
        self.grade = raw['grade']
        self.call_price = None
        self.low_inc_1 = None
        self.high_inc_1 = None
        self.mod_inc_1 = None
        self.low_inc_2 = None
        self.high_inc_2 = None
        self.mod_inc_2 = None
        self.low_inc_3 = None
        self.high_inc_3 = None
        self.mod_inc_3 = None
    def compute(self,):
        print('日期：', self.trade_date)
        if self.raw['grade'] >= 20000:
            if not self.com_close():
                return False
        elif self.raw['grade'] >= 10000:
            if not self.com_in():
                return False
        else:
            return False
        self.low_inc_1 = self.trade_single_df.loc[self.index-1,'low_price']/self.call_price -1
        self.high_inc_1 = self.trade_single_df.loc[self.index-1, 'high_price']/self.call_price -1
        self.mod_inc_1 = self.trade_single_df.loc[self.index-1, 'mod_price']/self.call_price -1
        self.low_inc_2 = self.trade_single_df.loc[self.index-2, 'low_price']/self.call_price -1
        self.high_inc_2 = self.trade_single_df.loc[self.index-2, 'high_price']/self.call_price -1
        self.mod_inc_2 = self.trade_single_df.loc[self.index-2, 'mod_price']/self.call_price -1
        self.low_inc_3 = self.trade_single_df.loc[self.index-3, 'low_price']/self.call_price -1
        self.high_inc_3 = self.trade_single_df.loc[self.index-3, 'high_price']/self.call_price -1
        self.mod_inc_3 = self.trade_single_df.loc[self.index-3, 'mod_price']/self.call_price -1
        return True
    def com_close(self):
        index_list = self.trade_single_df[self.trade_single_df.trade_date == self.trade_date].index.to_list()
        if len(index_list) != 1:
            logging.error('{} 日期未找到或者多个:{},{}'.format(self.raw['stock_id'],self.trade_date,index_list))
            print('{} 日期未找到或者多个:{}'.format(self.raw['stock_id'],self.trade_date))
            return False
        self.index = index_list[0]
        if len(self.trade_single_df) <= self.index + 4:
            logging.error('{} 交易记录长度不够:{}'.format(self.raw['stock_id'],self.trade_date))
            print('{} 交易记录长度不够:{}'.format(self.raw['stock_id'],self.trade_date))
            return False
        self.call_price = self.trade_single_df.loc[self.index, 'close_price']
        return True
    def com_in(self):
        index_list = self.trade_single_df[self.trade_single_df.trade_date == self.trade_date].index.to_list()
        if len(index_list) != 1:
            logging.error('{} 日期未找到或者多个:{},{}'.format(self.raw['stock_id'], self.trade_date,index_list))
            print('{} 日期未找到或者多个:{}'.format(self.raw['stock_id'], self.trade_date))
            return False
        # print('index:',self.raw['stock_id'],self.trade_date,index_list)
        self.index = index_list[0] -1
        if self.index < 4:
            logging.error('{} 索引超过下限:{},{}'.format(self.raw['stock_id'], self.trade_date,index_list))
            print('{} 索引超过下限:{}'.format(self.raw['stock_id'], self.trade_date))
            return False
        print('计算日期：',self.trade_single_df.loc[self.index , 'trade_date'])
        if len(self.trade_single_df) <= self.index + 4:
            logging.error('{} 交易记录长度不够:{}'.format(self.raw['stock_id'], self.trade_date))
            print('{} 交易记录长度不够:{}'.format(self.raw['stock_id'], self.trade_date))
            return False
        self.call_price = self.trade_single_df.loc[self.index + 1, 'close_price'] * 1.025
        if self.trade_single_df.loc[self.index, 'high_price'] < self.call_price:
            print('未达到买入标准：日期：{0} ，call_price:{1} ,high_price:{2}'.format(
                self.trade_single_df.loc[self.index + 1, 'trade_date'],self.call_price,self.trade_single_df.loc[self.index, 'high_price']))
            return False
        return True

"""
改用web页面显示，图片绘制封存（未调通）
"""
# class draw_pic:
#     def __init__(self):
#         self.id = None
#         self.name = None
#         self.df = None
#         # self.info_df = None
#         self.image_path = None
#         self.trade_date = None
#         self.chart_title = None
#         self.date_df = None
#         self.select_df()
#         self.single_df = None
#
#     def select_df(self):
#         data_sql = "SELECT trade_date,open_price,close_price,high_price,low_price  " \
#                    " FROM stock_trade_data " \
#                    " where trade_date >= '2020-10-01'"
#         self.df = pub_uti.creat_df(data_sql)
#
#     def create_single_df(self):
#         self.single_df = self.df.loc[self.df.stock_id == self.id]
#         self.single_df.reset_index(inplace=True)
#         index_list = self.single_df[self.single_df.trade_date == self.trade_date].index.to_list()
#         if len(index_list) == 1:
#             end_index = index_list[0] - 3
#         else:
#             print('ERROR:index lengh has error.', index_list)
#         if end_index < 0:
#             end_index = 0
#         self.single_df = self.single_df.iloc[end_index:]
#         self.single_df.reset_index(inplace=True)
#
#     def draw_image(self, date, stock_id, name, chart_title,folder_name):
#         self.id = stock_id
#         self.name = name
#         self.folder_name = folder_name
#         self.chart_title = chart_title
#         self.trade_date = date
#         self.create_single_df()
#         self.df['dates'] = np.arange(0, len(self.df))
#         self.df['5'] = self.df['close_price'].rolling(5).mean()
#
#         def format_date(x, pos):
#             if x < 0 or x > len(date_tickers) - 1:
#                 return ''
#             return date_tickers[int(x)]
#
#         date_tickers = self.df.trade_date.values
#         plt.rcParams['font.sans-serif'] = ['KaiTi']
#         plt.rcParams['axes.unicode_minus'] = False
#         fig, ax = plt.subplots(figsize=(23, 5))
#         ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
#         ax.set_title(chart_title, fontsize=20)
#         # 绘制K线图
#         mpl_finance.candlestick_ochl(
#             ax=ax,
#             quotes=self.df[['dates', 'open_price', 'close_price', 'high_price', 'low_price']].values,
#             width=0.7,
#             colorup='r',
#             colordown='g',
#             alpha=0.7)
#         # plt.plot(self.df['dates'], self.df['5'])
#         # zhuang_section = self.info_df.loc[0,'zhuang_section']
#         # # print('zhuang_section:', zhuang_section)
#         # try:
#         #     zhuang_section = eval(zhuang_section)
#         # except Exception as err:
#         #     if zhuang_section == None:
#         #         zhuang_section = []
#         #     logging.error('ERR:{} zhuang_section:{},df:{}'.format(err,zhuang_section,self.info_df))
#         #     print('ERR:{} zhuang_section:{},df:{}'.format(err,zhuang_section,self.info_df))
#         # for zhaung_tup in zhuang_section:
#         #     sta = self.__comput_ind(zhaung_tup[1])
#         #     end = self.__comput_ind(zhaung_tup[0])
#         #     print('indexs:', sta, end)
#         #     plt.plot(self.df['dates'][sta:end], self.df['5'][sta:end], color='green')
#         plt.legend();
#
#         # self.image_path = '../pic/{0}{1}{2}{3}.jpg'.format(stock.stock_id, stock.stock_name, self.to_day, stock.inform_type)
#         self.image_path = './validate_report/{0}/{1}.jpg'.format(self.folder_name,self.chart_title)
#         plt.savefig(self.image_path)
# class pic_buffer:
#     def __init__(self):
#         self.csv_name = './validate_report/validate_retacement.csv'
#         self.df = None
#         self.grade_start = -0.05
#         self.grade_end = 0
#         self.df = pd.read_csv(self.csv_name)
#         self.folder_name = 'negative_5'
#     def target_df(self):
#         target_df = self.df.loc[self.grade_start < self.df.mod_inc_1 ]
#         target_df = target_df.loc[self.df.mod_inc_1<= self.grade_end]
#         dp = draw_pic()
#         for inx,raw in target_df.iterrows():
#             chart_title = "{0}_{1}_{2}_{3}".format(raw['stock_id'],raw['stock_name'],raw['grade'],raw['trade_date'])
#             dp.draw_image(date, raw['stock_id'], raw['stock_name'], chart_title, self.folder_name)
"""
图片绘制end
"""
class save_result:
    def __init__(self):
        self.csv_name = './validate_report/validate_retacement.xlsx'
        self.df = None
        self.df = pd.read_excel(self.csv_name,dtype={'stock_id':str})
        print('df:',self.df.head(100))
    def save(self):
        pub_uti_a.df_to_mysql('validate_retracement', self.df)


if __name__ == '__main__':
    date = '2021-04-14'
    # main(date)
    # vali = validate_buffer(vali_start = '2021-01-01',vali_end='2021-06-20')
    # vali.com()
    sr = save_result()
    sr.save()
    print('completed.')
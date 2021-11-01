#使用散点图展示计算结果
import pub_uti_a
import  pandas as pd
import matplotlib.pyplot as plt
import copy
class pic:
    def __init__(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.sel_df = None
        self.monitor_df = None
        self.n_monitor_df = None
    def select(self):
        sql = "select s.stock_id,s.high_price,s.low_price,s.increase,l.grade,l.monitor from stock_trade_data s " \
              " LEFT JOIN limit_up_single l " \
              " ON s.trade_code = l.trade_code " \
              " where s.trade_date >='{0}' and s.trade_date <='{1}'".format(self.start_date,self.end_date)
        self.sel_df = pub_uti_a.creat_df(sql)
    def deal_data(self):
        df_group =self.sel_df.groupby('stock_id',as_index =False)
        self.sel_df['inc_1'] = df_group['increase'].shift(-1)
        self.sel_df['inc_2'] = df_group['increase'].shift(-2)
        # self.sel_df['after_1_high'] = df_group['high_price'].shift(-1)
        # self.sel_df['after_2_high'] = df_group['high_price'].shift(-2)
        # self.sel_df['after_2_mid'] = (df_group['high_price'].shift(-2) + df_group['low_price'].shift(-2))/2
        self.sel_df.fillna(0,inplace = True)
        # print('self.sel_df:', self.sel_df)
        self.monitor_df = self.sel_df[self.sel_df.monitor == 1]
        self.n_monitor_df = self.sel_df[self.sel_df.monitor != 1]
    def show_pic(self):
        self.select()
        self.deal_data()
        ax = self.sel_df.plot.scatter(x='grade', y='inc_2',color = 'Green',label = 'g1')
        self.n_monitor_df.plot.scatter(x='grade', y='inc_2', color='Blue', label='g2',ax = ax)
        plt.show()
if __name__ == '__main__':
    p =pic('2020-10-11','2021-10-21')
    p.show_pic()


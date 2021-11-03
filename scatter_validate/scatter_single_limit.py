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
        #standard  v_rebound   double_limit   wave
        sql = "select s.trade_date,s.stock_id,s.high_price,s.low_price,s.close_price,s.increase,l.grade,l.monitor,l.type from stock_trade_data s " \
              " LEFT JOIN (select * from limit_up_single where type = 'v_rebound' and trade_date >='{0}' and trade_date <='{1}') l " \
              " ON s.trade_code = l.trade_code " \
              " where s.trade_date >='{0}' and s.trade_date <='{1}' ".format(self.start_date,self.end_date)
        # sql = "select s.trade_date,s.stock_id,s.high_price,s.low_price,s.close_price,s.increase,l.grade,l.monitor,l.type from stock_trade_data s " \
        #       " LEFT JOIN (select * from limit_up_single where  trade_date >='{0}' and trade_date <='{1}') l " \
        #       " ON s.trade_code = l.trade_code " \
        #       " where s.trade_date >='{0}' and s.trade_date <='{1}' ".format(self.start_date,self.end_date)
        self.sel_df = pub_uti_a.creat_df(sql)
        self.sel_df.to_csv('record_sel.csv')
    def deal_data(self):
        df_group =self.sel_df.groupby('stock_id',as_index =False)
        self.sel_df['grade'] = self.sel_df['grade'].apply(lambda x: 0 if (x<0 or x>20000) else x)#消除异常分
        self.sel_df['h_price_1'] =df_group['high_price'].shift(1) #triger
        self.sel_df['close_price_2'] = df_group['close_price'].shift(2)
        self.sel_df.dropna(subset= ['h_price_1','close_price_2'],inplace = True)
        self.sel_df['inc_1'] = (self.sel_df['h_price_1']/self.sel_df['close_price']-1)*100
        self.sel_df['inc_2'] = (self.sel_df['close_price_2']/self.sel_df['close_price']-1.025)*100
        # self.sel_df['after_1_high'] = df_group['high_price'].shift(-1)
        # self.sel_df['after_2_high'] = df_group['high_price'].shift(-2)
        # self.sel_df['after_2_mid'] = (df_group['high_price'].shift(-2) + df_group['low_price'].shift(-2))/2
        # print('self.sel_df:', self.sel_df)
        self.triger_monitor_df = self.sel_df[(self.sel_df.monitor == 1) & (self.sel_df.inc_1 >=2.5) & (self.sel_df.inc_2 != -30)]
        self.monitor_df = self.sel_df[(self.sel_df.monitor == 1) & (self.sel_df.inc_2 != -30)]
        self.monitor_df.to_csv('monitor_df.csv')
        self.n_monitor_df = self.sel_df[self.sel_df.monitor != 1 & (self.sel_df.inc_1 >=2.5)]
    def show_pic(self):
        self.select()
        self.deal_data()
        # ax = self.sel_df.plot.scatter(x='grade', y='inc_2',color = 'Green',label = 'g1')
        df =self.triger_monitor_df
        df.plot.scatter(x='grade', y='inc_2',s=5,c='Green',figsize=(10,10),)
        lenth = len(df)
        mean = df['inc_2'].mean()
        print('总数：{}，平均数：{}'.format(lenth,mean))
        w_df = df[df.grade >= 10000]
        lenth = len(w_df)
        mean = w_df['inc_2'].mean()
        print('10000以上总数：{}，平均数：{}'.format(lenth, mean))
        plt.show()
        # self.triger_monitor_df.plot.scatter(x='grade', y='inc_2')
        # plt.show()
if __name__ == '__main__':
    p =pic('2021-01-01','2021-03-26')
    p.show_pic()


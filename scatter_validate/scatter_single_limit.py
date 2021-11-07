#使用散点图展示计算结果
import json
import logging

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
        # sql = "select s.trade_date,s.stock_id,s.high_price,s.low_price,s.close_price,s.increase,l.grade,l.monitor,l.type,l.factor " \
        #       " from stock_trade_data s " \
        #       " LEFT JOIN (select * from limit_up_single_validate where type = 'wave' and trade_date >='{0}' and trade_date <='{1}') l " \
        #       " ON s.trade_code = l.trade_code " \
        #       " where s.trade_date >='{0}' and s.trade_date <='{1}' ".format(self.start_date,self.end_date)
        sql = "select s.trade_date,s.stock_id,s.high_price,s.low_price,s.close_price,s.increase,l.grade,l.monitor,l.type from stock_trade_data s " \
              " LEFT JOIN (select * from limit_up_single_validate where  trade_date >='{0}' and trade_date <='{1}') l " \
              " ON s.trade_code = l.trade_code " \
              " where s.trade_date >='{0}' and s.trade_date <='{1}' ".format(self.start_date,self.end_date)
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
        self.all_monitor_df = self.sel_df[self.sel_df.inc_1 >=2.5]
    def deal_factor_data(self):
        self.factor_list = []
        self.triger_monitor_df.reset_index(drop=True,inplace=True)
        print('self.triger_monitor_df:', self.triger_monitor_df.columns,self.triger_monitor_df)
        for i in range(0,100):
            factor = self.triger_monitor_df.loc[i,'factor']
            if factor!= '{}':
                factor_dict = json.loads(factor)
                for n in factor_dict:
                    self.triger_monitor_df[n] = 0
                    self.factor_list.append(n)
                break
        def deal_fun(raw):
            factor_dict = json.loads(raw['factor'])
            if len(factor_dict) == 0:
                return raw
            for fac in factor_dict:
                if fac not in raw:
                    logging.ERROR('factor not in raw!factor:{},raw:{}'.format(fac,raw))
                    print('Error:factor not in raw!factor:{},raw:{}'.format(fac,raw))
                    continue
                raw[fac] = factor_dict[fac]

            return raw
        self.triger_monitor_df = self.triger_monitor_df.apply(deal_fun,axis=1)
        self.triger_monitor_df.to_csv('record_factor.csv')
    def show_pic(self):
        self.select()
        self.deal_data()
        # ax = self.sel_df.plot.scatter(x='grade', y='inc_2',color = 'Green',label = 'g1')
        df =self.all_monitor_df
        df.plot.scatter(x='grade', y='inc_2',s=2,c='Green',figsize=(15,15),)
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
    def show_factor_pic(self):
        self.select()
        self.deal_data()
        self.deal_factor_data()
        color_list = ['b','g','r','y']
        color_dict = {'b':'蓝色','g':'绿色','r':'红色','y':'黄色'}
        mark_list = ['.','+','x','*','D','1','2','3','4','_',2,4,5,6,7,'s']
        mark_dict = {'.':'点','1':'下Y','2':'上Y','3':'左Y','4':'右Y','+':'+','x':'X','_':'-',2:'|',4:'左三角',5:'右三角',
                     6:'上三角',7:'下三角','s':'方块','D':'菱形','*':'五角星'}
        col_len = len(self.factor_list)
        pick_tup = (1,0)
        pick_len = col_len//pick_tup[0]
        start = pick_len * pick_tup[1]
        end = pick_len * (pick_tup[1] +1) if  pick_len * (pick_tup[1] +1) < col_len else col_len
        i = start
        record_dict = {}
        for mark in mark_list:
            if i == end:
                break
            for color in color_list:
                factor_name = self.factor_list[i]
                print('factor_name:',factor_name)
                plt.scatter(self.triger_monitor_df[factor_name], self.triger_monitor_df['inc_2'], color=color, marker=mark)
                record_dict[factor_name] = (color_dict[color],mark_dict[mark])
                i += 1
                if i == end:
                    break
        print(record_dict)
        plt.show()

if __name__ == '__main__':
    p =pic('2021-01-01','2021-10-31')
    p.show_pic()
    # p.show_factor_pic()

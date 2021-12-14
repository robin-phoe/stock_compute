#使用散点图展示计算结果
import datetime
import json
import logging

import pub_uti_a
import  pandas as pd
import matplotlib.pyplot as plt
import copy
import re
pd.set_option('display.max_columns', None)
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
        #       " LEFT JOIN (select * from limit_up_single_validate where type = 'v_rebound' and trade_date >='{0}' and trade_date <='{1}') l " \
        #       " ON s.trade_code = l.trade_code " \
        #       " where s.trade_date >='{0}' and s.trade_date <='{1}' ".format(self.start_date,self.end_date)
        sql = "select s.trade_date,s.stock_id,s.high_price,s.low_price,s.close_price,s.increase,l.zhuang_grade,l.monitor,l.zhuang_section from stock_trade_data s " \
              " INNER JOIN (select * from com_zhuang where  zhuang_grade>=1000  ) l " \
              " ON s.stock_id = l.stock_id " \
              " where s.trade_date >='{0}' and s.trade_date <='{1}' ".format(self.start_date,self.end_date)
        self.sel_df = pub_uti_a.creat_df(sql)
        self.sel_df.to_csv('record_sel.csv')
    def deal_data(self):
        day_lenth = 120
        id_set = set(self.sel_df['stock_id'].to_list())
        res_df = pd.DataFrame(columns=('id','up_rate','grade','long'))
        for id in id_set:
            print('id:',id)
            single_df = self.sel_df[self.sel_df.stock_id == id]
            single_df.reset_index(inplace=True, drop=True)
            grade = single_df.loc[0,'zhuang_grade']
            section_lastest_day = eval(single_df.loc[0,'zhuang_section'])[0][0][0:10]
            print('section_lastest_day:',section_lastest_day)
            pice_start = single_df[single_df.trade_date == section_lastest_day].index[0]
            pice_end = 0
            if pice_start >day_lenth:
                pice_end = pice_start -day_lenth
            price_start = single_df.loc[pice_start,'close_price']
            print('pice_end:',pice_end,'pice_start:',pice_start)
            price_end = max(single_df['close_price'][pice_end:pice_start-1])
            up_rate = (price_end/price_start-1)*100
            max_price_index_list = single_df[single_df.close_price == price_end].index
            print('max_price_index_list:',max_price_index_list)
            max_price_index_l = []
            for index in max_price_index_list:
                if pice_end <=index <= pice_start :
                    max_price_index_l.append(index)
            # max_price_index =
            long = pice_start - max_price_index_l[-1]
            res_df.loc[len(res_df)] = [id,up_rate,grade,long]
        print('均值：',res_df['up_rate'].mean())


        self.all_monitor_df = res_df
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
        #计算相关性
        df = self.triger_monitor_df
        #清洗t_r_mul过大值
        # df['t_r_mul'] = df['t_r_mul'].apply(lambda x:x if x<1000 else 100)
        df2= df[self.factor_list]
        self.factor_list.append('inc_2')
        df1 = df[self.factor_list]
        print('相关性：',df1.corr(method='kendall', min_periods=1))
        pd.plotting.scatter_matrix(df1 , figsize=(15, 15), marker='0',
                                   hist_kwds={'bins': 20}, s=2, alpha=.8)
        plt.show()
    def show_pic(self):
        self.select()
        self.deal_data()
        # ax = self.sel_df.plot.scatter(x='grade', y='inc_2',color = 'Green',label = 'g1')
        df =self.all_monitor_df
        df.plot.scatter(x='long', y='up_rate',s=2,c='Green',figsize=(15,15),)
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
    p =pic('2018-01-01','2021-12-30')
    p.show_pic()
    # p.show_factor_pic()

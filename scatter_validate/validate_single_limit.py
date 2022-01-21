"""
验证单个涨停后间隔再涨停的概率，记录间隔天数
标准：
1、单个涨停
2、间隔1日以上，10日以下
"""
import pandas as pd
import pub_uti_a


class stock:
    def __init__(self,df):
        self.single_df = df
        self.res_df = pd.DataFrame()
        self.delta_day = 10
        self.first_flag = False
        self.bad_limt = False
        self.count =0
        self.deal_data()
    def deal_data(self):
        self.single_df.reset_index(drop=True,inplace=True)
        self.single_df['count'] = 0
        def fun(raw):
            if self.first_flag == False:
                if raw['limit_flag']:
                    self.first_flag = True
                    raw['count'] = -1
            else:
                if self.bad_limt ==False:
                    if raw['limit_flag']:
                        if self.count == 0:
                            self.bad_limt = True
                        #bingo
                        else:
                            raw['count'] = self.count
                            self.count = 0
                            self.bad_limt = True
                    else:
                        self.count +=1
                else:
                    if raw['limit_flag'] ==False:
                        self.bad_limt = False
                        self.first_flag =False
            if self.count > self.delta_day:
                self.first_flag = False
            return raw
        self.res_df =self.single_df.apply(fun,axis=1)
        self.res_df = self.res_df[self.res_df['count'] != 0]
class sock_buffer:
    def __init__(self,start_date,end_date):
        self.df = pd.DataFrame()
        self.start_date =start_date
        self.end_date = end_date
        self.stock_id_set = set()
        self.result_df = pd.DataFrame()
        self.select_data()
        self.deal_limit()
        self.deal_single()
    def select_data(self):
        sql = "select trade_date,stock_id,stock_name,close_price,increase " \
              "FROM stock_trade_data " \
              "WHERE trade_date >='{0}' AND trade_date <='{1}'".format(self.start_date,self.end_date)
        self.df = pub_uti_a.creat_df(sql,ascending=True)
        self.stock_id_set = set(self.df['stock_id'].to_list())
    def deal_limit(self):
        self.df['limit_flag'] = False
        def fun(raw):
            if raw['increase'] >= 9.75:
                raw['limit_flag'] = True
            return raw
        self.df = self.df.apply(fun,axis=1)
    def deal_single(self):
        for id in self.stock_id_set:
            if (id[0:3]=='600' or id[0:2]=='00'):
                res_df = stock(self.df[self.df['stock_id'] == id]).res_df
                if len(self.result_df) == 0:
                    self.result_df = res_df
                else:
                    self.result_df = pd.concat([self.result_df,res_df])
        self.result_df.reset_index(drop= True,inplace=True)
        self.result_df.to_csv('./validate_single_limit.csv')
if __name__ == '__main__':
    sock_buffer('2020-01-01','2021-12-31')
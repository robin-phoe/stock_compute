'''
验证 庄线信号 下收益结果
'''
import pandas as pd
import pub_uti_a
import datetime

class single_stock:
    def __init__(self,single_df,zhuang_section):
        self.id = single_df.loc[0,'stock_id']
        self.name = single_df.loc[0,'stock_name']
        self.single_df = single_df
        self.zhuang_section = eval(zhuang_section)
        self.zhuang_section_time =[]
        self.section_len = len(zhuang_section)
        self.section_index_now = 0
        self.start_add = 30
        self.end_add = 120
        self.signal_threshold =2 #换手倍数
        self.avg_roll = 10
        self.interval = 0 #信号触发间隔天数
        self.result_df = pd.DataFrame(columns=('stock_id','stock_name','trade_date','delta_5','delta_14','delta_20','delta_40'))
    #运行函数
    def run(self):
        self.comp_section()
        self.deal_single()
        for i,raw in self.single_df.iterrows():
            for date_tup in self.zhuang_section_time:
                # print('time type:',raw['trade_date'])
                if date_tup[0] <= datetime.datetime.strptime(raw['trade_date'],'%Y-%m-%d') <= date_tup[1]:
                    if self.interval > 0:
                        self.interval -= 1
                        continue
                    self.validate_single(i)
                    break
        return self.result_df
    #计算庄线区间
    def comp_section(self):
        for tup in self.zhuang_section:
            self.zhuang_section_time.append((datetime.datetime.strptime(tup[1],'%Y-%m-%d') + datetime.timedelta(days=self.start_add),
                                          datetime.datetime.strptime(tup[0],'%Y-%m-%d') + datetime.timedelta(days=self.end_add)))
    #【核心算法】计算判断信号
    def deal_single(self):
        self.single_df['avg'] = self.single_df['turnover_rate'].rolling(self.avg_roll).mean()
        self.single_df['avg'] = self.single_df['avg'].shift(1)
        self.single_df['avg'].fillna(100, inplace=True)
        self.single_df['volume_signal'] = self.single_df['turnover_rate'] / self.single_df['avg']
        # self.single_df.to_excel('./validate_report/single_df.xlsx',index=False,encoding='utf-8')
        # print('single_df', single_df)
    def validate_single(self,index):
        if self.single_df.loc[index,'volume_signal'] < self.signal_threshold:
            return 0
        self.interval = 5
        len_df = len(self.single_df)
        buy_price = self.single_df.loc[index,'close_price']
        def delta_fun(interval):
            close_list = self.single_df['close_price'].to_list()[index:index + interval]
            min_delta = (min(close_list) / buy_price - 1) * 100
            max_delta = (max(close_list) / buy_price - 1) * 100
            delta = max_delta if max_delta>abs(min_delta) else min_delta
            return delta
        #信号后5日极值价差
        interval = 5
        delta_5 = 0
        if index+interval <= len_df:
            delta_5 =delta_fun(interval)
        #信号后14日极值价差
        interval = 14
        delta_14 = 0
        if index+interval <= len_df:
            delta_14 =delta_fun(interval)
        #信号后20日极值价差
        interval = 20
        delta_20 =0
        if index+interval <= len_df:
            delta_20 =delta_fun(interval)
        #信号后40日极值价差
        interval = 40
        delta_40 =0
        if index+interval <= len_df:
            delta_40 =delta_fun(interval)
        print('result:',delta_5,delta_14,delta_20,delta_40)
        self.result_df.loc[len(self.result_df)] = [self.id,self.name,self.single_df.loc[index,'trade_date'],
                                                   delta_5,delta_14,delta_20,delta_40]
class stock_buffer:
    def __init__(self):
        self.zhuang_df = pd.DataFrame()
        self.data_df = pd.DataFrame()
        self.result_df = pd.DataFrame(columns=('stock_id','stock_name', 'trade_date', 'delta_5',
                                               'delta_14', 'delta_20', 'delta_40'))
        self.s_buffer = {}
        self.section_dict = {}
        self.id_set = ()
    def run(self):
        self.select_data()
        self.create_section()
        self.create_single()
    def select_data(self):
        zhuang_sql = "SELECT stock_id,stock_name,zhuang_section " \
              " FROM com_zhuang " \
              " WHERE zhuang_long > 0 "
        self.zhuang_df = pub_uti_a.creat_df(zhuang_sql)
        self.id_set = set(self.zhuang_df['stock_id'].to_list())
        data_sql = "SELECT * " \
                   " FROM stock_trade_data " \
                   " WHERE stock_id "
        self.data_df = pub_uti_a.creat_df(data_sql,ascending=True)
        self.create_section()
    def create_section(self):
        for i,raw in self.zhuang_df.iterrows():
            self.section_dict[raw['stock_id']] = raw['zhuang_section']
    def create_single(self):
        for id in self.id_set:
            single_df = self.data_df[self.data_df['stock_id'] == id]
            single_df.reset_index(inplace=True,drop=True)
            # print('single_df:',single_df['trade_date']) #验证时间顺序
            single_result = single_stock(single_df,self.section_dict[id]).run()
            self.result_df = pd.concat([self.result_df,single_result],axis=0)
        print('self.result_df:',self.result_df)
        self.result_df.to_excel('./validate_result/zhuang_shouyi_validate.xlsx',index=False,encoding='utf-8')

if __name__ == '__main__':
    stock_buffer().run()
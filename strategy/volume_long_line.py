"""
换手量对长线的影响猜测验证
1、长时间换手量级不变，突然量级提升，呈长阶梯状态，拉升时量明显增高
2、长时间换手量级不变，突然量级下沉，呈下降长阶梯状，拉升时量再次暴露
3、长时间换手量级不变，量短区间明显升高，再回落形成平静台地，呈现一次或者多次丘包状态，再次量提升时拉升
"""
import logging
import datetime
import pandas as pd
import pub_uti_a
class stock:
    def __init__(self,single_df):
        self.df = single_df
        self.trade_code = None
        self.info = None
        self.info_tup = ('single','single2','hat','highland')
        self.hat_num = 2
        self.highland_num = 5
        self.count_matching = 0
        self.short_piece = 3
        self.long_piece = 20
        self.before_section = 30
        self.start = len(self.df)-1
        self.begin = 0
    def run(self):
        self.deal_data()
        self.core()
    def deal_data(self):
        self.df.reset_index(inplace=True,drop=True)
        self.trade_code = self.df.loc[len(self.df) - 1, 'trade_code']
        #换手量抽象化 防止细微量的倍数影响，比如0.1，0.2，形成了翻倍，但是绝对量并不明显
        self.df['value_a'] = 0
        def abstract(raw):
            section_tup = [(-1,0.5),(0.5,1),(1,2),(2,4),(4,6),(6,8),(8,100)]
            for tup in section_tup:
                if tup[0]<=raw['turnover_rate']<tup[1]:
                    if raw['turnover_rate'] < (tup[0]+tup[1])/2:
                        raw['value_a'] = tup[0]
                    else:
                        raw['value_a'] = tup[1]
            return raw
        self.df = self.df.apply(abstract,axis=1)
    def core(self):
        self.info = 'N'
        self.count_matching = 0
        #判断之前区间中是否有single、hat信号
        self.begin = self.start - self.before_section
        section_flag = self.df['value_abnormal'][self.begin:self.start]
        count_info = ((section_flag=='single')|(section_flag=='hat')|(section_flag=='single2')).sum()
        target_value = self.df.loc[self.start,'value_a']
        section_value = self.df['value_a'][0:self.start]
        section_flag = self.df['value_abnormal'][0:self.start]
        continuous_flag = True
         #记录连续符合1/2值的天数
        count_hat = 0 #记录满足hat的天数
        last_info = None
        for i in range(len(section_value)-1,-1,-1):
            if section_flag[i] in self.info_tup:
                if continuous_flag:
                    count_hat +=1
                else:
                    last_info = section_flag[i-1]
                    break
            else:
                continuous_flag = False
                if section_value[i] <= target_value/2:
                    self.count_matching += 1
                else:
                    break
        if self.count_matching>3:
            if count_hat >= self.highland_num:
                self.info = "highland"
            elif count_hat >= self.hat_num:
                self.info = "hat"
            elif count_hat >=1 :  # 总统计大于连续计数，表示有间隔
                self.info = 'single2'
            else:
                self.info = "single"
        else:
            self.info = 'N'
        self.df.loc[self.start,['value_abnormal','count_matching']]= [self.info,self.count_matching]
class stock_buffer:
    def __init__(self,s_date=None,e_date=None):
        self.df = None
        self.save = pub_uti_a.save()
        self.id_set = set()
        self.s_date = s_date
        self.e_date = e_date
        self.sql_range_day= 40
        self.date = None
    def select_data(self):
        sql = "SELECT trade_code,trade_date,stock_id,stock_name,turnover_rate," \
              " value_abnormal,count_matching " \
              " from stock_trade_data " \
              "WHERE trade_date >='{0}' AND trade_date <='{1}' ".format(self.sql_start_date,self.e_date)
        self.df = pub_uti_a.creat_df(sql,ascending=True)
        self.id_set = set(self.df['stock_id'].to_list())

    def creat_time(self):
        if self.s_date == None or self.e_date == None:
            sql = "select DATE_FORMAT(max(trade_date),'%Y-%m-%d') as last_date from stock_trade_data "
            self.date = pub_uti_a.select_from_db(sql=sql)[0][0]
            self.s_date = self.date
            self.e_date = self.date
        self.sql_start_date = (datetime.datetime.strptime(self.s_date, '%Y-%m-%d') -
                               datetime.timedelta(days=self.sql_range_day)).strftime('%Y-%m-%d')
    def run(self):
        self.creat_time()
        self.select_data()
        for id in self.id_set:
            single_df = self.df[self.df['stock_id'] == id]
            stock_ins = stock(single_df)
            save_sql = "UPDATE stock_trade_data " \
                       "SET value_abnormal='{0}',count_matching='{1}' " \
                       "WHERE trade_code = '{2}'".format(
                stock_ins.info,stock_ins.count_matching,stock_ins.trade_code)
            stock_ins.run()
            print(stock_ins.info,stock_ins.count_matching,stock_ins.trade_code)
            self.save.add_sql(save_sql)
        self.save.commit()
"""
历史数据计算
"""
class stock_buffer_history(stock_buffer):
    def select_trade_date(self):
        if self.s_date == None or self.e_date == None:
            logging.ERROR('The date section is None when make history compute!')
            print('The date section is None when make history compute!')
            self.s_date = '2020-01-01'
            self.e_date = '2021-12-31'
        sql = "select distinct(trade_date) from stock_trade_data " \
              " where trade_date >= '{}' and trade_date<= '{}' ".format(self.s_date,self.e_date)
        self.trade_date_list = pub_uti_a.creat_df(sql,ascending=True)['trade_date'].to_list()
    def clean_data(self):
        sql = "update stock_trade_data " \
              "set value_abnormal='N',count_matching='0' " \
              " where trade_date >= '{}' and trade_date<= '{}'".format(self.s_date,self.e_date)
        pub_uti_a.commit_to_db(sql)
    def run(self):
        self.creat_time()
        self.clean_data()
        self.select_trade_date()
        self.select_data()
        for id in self.id_set:
            self.save = pub_uti_a.save()
            single_df = self.df[self.df['stock_id'] == id]
            print(id, single_df['stock_name'].to_list()[0])
            stock_ins = stock_history(single_df)
            stock_ins.run(self.trade_date_list)
            for save_sql in stock_ins.sql_list:
                self.save.add_sql(save_sql)
            # stock_ins.df.to_csv('./document/test.csv')
            self.save.commit()

class stock_history(stock):
    def make_sql(self):
        sql = "UPDATE stock_trade_data " \
                       "SET value_abnormal='{0}',count_matching='{1}' " \
                       "WHERE trade_code = '{2}'".format(
                self.info,self.count_matching,self.trade_code)
        # print(self.info,self.count_matching,self.trade_code[0:8])
        self.sql_list.append(sql)
    def run(self,trade_date_list):
        self.sql_list = []
        self.deal_data()
        for t_date in trade_date_list:
            index_list = self.df[self.df['trade_date'] == t_date].index.to_list()
            if len(index_list)== 0:
                continue
            self.start = index_list[0]
            self.trade_code = self.df.loc[self.start,'trade_code']
            self.core()
            self.make_sql()


if __name__ == '__main__':
    # stock_buffer().run()
    stock_buffer_history('2018-04-01','2022-02-09').run()
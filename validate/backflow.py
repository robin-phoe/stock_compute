#设定初始资金，设定建仓规则，平仓规则。计算策略信号在指定时间段内收益
#做出通用回测工具，支持策略、因子回测
#设定卖出标准，止盈，止损，超时
#支持多种模式，1、额定初始资金调仓模式，2、无限资金算收益比模式
#远期：支持策略、因子信号计算缓存，现计算校验已有策略已计算数据
import pub_uti_a
import pandas as pd

class base_config:
    mode = 'ratio'#limit_cash:限定资金；ratio:比值模式
    timeout = 3
    stop_profit = 0.05 * 100#
    stop_loss = 0.05 * 100

init_capital = 1000000
start_time = ""
end_time = ""

#信号
class single:
    code = None
    name = None
    grade = None
    conf = base_config()
#创建信号集合
class signal_hub:
    def __init__(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.df = None

    def select_single(self):
        sql = "select trade_code,grade" \
              " from limit_up_single " \
              " where trade_date >= '{}' and trade_date <='{}' and grade > 0".format(self.start_date,self.end_date)
        self.df = pub_uti_a.creat_df(sql, ascending=True)
    '''
    def create_info_df(self):
        self.select_signal()
        self.select_market()
        self.info_df = pd.merge(self.market_df,self.signal_df,on='trade_code')
        self.info_df['grade'].fillna(-9999,inplace=True)
    #创建当日信号集（按分数倒序）
    def create_today_signal(self,date):
        today_signal = self.info_df[self.info_df['trade_date'] == date & self.info_df['grade'] >= self.start_grade]
        return today_signal
    '''

class position:
    def __init__(self,stock_id,stock_name,qty,buy_price):
        self.stock_id = stock_id
        self.stock_name = stock_name
        self.qty = qty
        self.buy_price = buy_price
        self.hold_days = 0

class main:
    def __init__(self,init_capital,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.position_buffer = {}
        self.init_capital = init_capital
        self.available_capital = init_capital
        self.position_value = 0
        self.all_capital = init_capital
        self.single_day_trade = 0
        self.create_info_df()
        self.start_grade = 0
        self.trade_date_list = []

    #创建时段内时序信号集
    def select_signal(self):
        self.signal_df = signal_hub(self,self.start_date,self.end_date)
    #创建时段内行情信息
    def select_market(self):
        sql = "select trade_code,trade_date,stock_id,stock_name,open_price,close_price,high_price,low_price,increase " \
              " from stock_trade_data " \
              "where trade_date >= '{}' and trade_date <= '{}' ".format(self.start_date,self.end_date)
        self.market_df = pub_uti_a.creat_df(sql)

    #建仓操作
    def buy_operate(self,date):
        today_signal = self.create_today_signal(date)
        #资金分配
        #per_s_capital =
        for stock in today_signal:
            if stock in self.position_buffer:
                continue

    #平仓操作
    def sell_operate(self):
        pass
    #创建交易日列表
    def create_tradedate_list(self):
        sql = "select distinct trade_date from stock_trade_data where trade_date >= '{}' and trade_date <= '{}'".format(
            self.start_date,self.end_date
        )
        trade_data_df = pub_uti_a.creat_df(sql,ascending=True)
        self.trade_date_list = trade_data_df['trade_date'].to_list()
    #遍历日期，触发建仓平仓
    def run(self):
        self.create_tradedate_list()
        for date in self.trade_date_list:
            self.single_day_trade = 0
            #处理买信号
            self.buy_operate(date)
            #处理平仓
            for id,instance in self.position_buffer.items():
                if instance.hold_days == 0:
                    continue
                #止盈卖出
                #止损失卖出
                #持仓超时卖出

if __name__ == '__main__':
    pass
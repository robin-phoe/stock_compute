#设定初始资金，设定建仓规则，平仓规则。计算策略信号在指定时间段内收益
#做出通用回测工具，支持策略、因子回测
#设定卖出标准，止盈，止损，超时
#支持多种模式，1、额定初始资金调仓模式，2、无限资金算收益比模式
#远期：支持策略、因子信号计算缓存，现计算校验已有策略已计算数据
import pub_uti_a
import pandas as pd
import enum

class call_type(enum):
    open = 1
    filter =2 #2.5%
    middle = 3
    close = 4
class base_config:
    mode = 'ratio'#limit_cash:限定资金；ratio:比值模式
    timeout = 3
    stop_profit = 0.05 * 100#
    stop_loss = 0.05 * 100
    trade_delta = 1#T
    call = call_type.filter
init_capital = 1000000
start_time = ""
end_time = ""

#信号
class signal:
    def __init__(self,id,name,grade,signal_date):
        self.id = None
        self.name = None
        self.grade = None
        self.signal_date = None
        self.conf = base_config()
#信号buffer
class signal_hub:
    def __init__(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.df = None
        self.select_single()
        self.signal_buffer = {}
        self.create_signal_buffer()
    def select_signal(self):
        sql = "select trade_code,trade_date,stock_id,stock_name,grade" \
              " from limit_up_single " \
              " where trade_date >= '{}' and trade_date <='{}' and grade > 0".format(self.start_date,self.end_date)
        self.df = pub_uti_a.creat_df(sql, ascending=True)
    def create_signal_buffer(self):
        for index,row in self.df.iterrows():
            self.signal_buffer.setdefault(row['run_date'],[])
            self.signal_buffer[row['run_date']].append(signal(id = row['stock_id'],name = row['stock_name'],grade=row['grade'],signal_date=row['run_date']))
    def get_signals_by_date(self,date):
        return self.signal_buffer.get(date)
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
class market:
    def __init__(self):
        trade_date = None
        stock_id = None
        stock_name = None
        open_price= None
        close_price= None
        high_price= None
        low_price= None
        increase= None
class market_hub:
    def __init__(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.market_df = None
        self.select_market()
        self.market_buffer = {}
    #创建时段内行情信息
    def select_market(self):
        sql = "select trade_code,trade_date,stock_id,stock_name,open_price,close_price,high_price,low_price,increase " \
              " from stock_trade_data " \
              "where trade_date >= '{}' and trade_date <= '{}' ".format(self.start_date,self.end_date)
        self.market_df = pub_uti_a.creat_df(sql)
    #create buffer
    def create_market_buffer(self):
        for index,row in self.market_df.iterrows():
            self.market_buffer.setdefault(row['trade_date'],{})
            self.market_buffer['trade_date'][row['stock_id']] = market(trade_date=row['trade_date'],stock_id = row['stock_id'],
                                                                       stock_name = row['stock_name'],open_price= row['open_price'],close_price= row['close_price'],
                                                                       high_price= row['high_price'],low_price= row['low_price'],increase= row['increase'])
    def get_market_by_day(self,date,stock_id):
        return self.market_buffer.get(date).get(stock_id)

class position:
    def __init__(self,trade_code,stock_id,stock_name,qty,buy_price,conf):
        self.trade_code = trade_code
        self.stock_id = stock_id
        self.stock_name = stock_name
        self.qty = qty
        self.buy_price = buy_price
        self.hold_days = 0
        self.conf = conf

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
        self.sig_hub = signal_hub(self.start_date,self.end_date)
        self.market_hub = market_hub(self.start_date,self.end_date)


    #建仓操作
    def buy_operate(self,index):
        #资金分配
        #per_s_capital =
        if index == 0:
            return
        single_date = self.trade_date_list[index-1]
        date = self.trade_date_list[index]
        signals = self.sig_hub.get_signals_by_date(single_date)
        for signal in signals:
            market = self.market_hub.get_market_by_day(date,signal.stock_id)
            #开盘价等于收盘价格，且涨幅大于9.75，筛选一字板（未纳入300、688）
            if market.high_price == market.low_price and market.increase >= 9.75:
                continue
            if signal.conf.call == call_type.filter:
                if market.increase >= 2.5:
                    #持仓唯一判断，是否已有持仓则不再买入
                    #todo
                    self.position_buffer[signal.trade_code] = position(signal.trade_code, signal.stock_id,
                                                                       signal.stock_name, qty = 100,
                                                                       buy_price = market.close_price,conf=signal.conf)

    #平仓操作
    def sell_operate(self,index):
        for id, position in self.position_buffer.items():
            position.hold_days += 1
            if position.hold_days == 1:
                continue
            #行情
            stock_id = position.stcok_id
            date = self.trade_date_list[index]
            market = self.market_hub.get_market_by_day(date, stock_id)
            # 止盈卖出 按收盘价卖出（涨停不卖）
            if market.close_price >= position.conf.stop_profit:
                if market.increase >= 9.75:
                    continue

            # 止损失卖出
            if market.close_price <= position.conf.stop_loss:
                if market.increase <= 9.75:
                    continue
            # 持仓超时卖出
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
        for index in range(len(self.trade_date_list)):
            #处理买信号
            self.buy_operate(index)
            #处理平仓
            self.sell_operate(index)


if __name__ == '__main__':
    pass
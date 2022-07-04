#设定初始资金，设定建仓规则，平仓规则。计算策略信号在指定时间段内收益
#做出通用回测工具，支持策略、因子回测
#设定卖出标准，止盈，止损，超时
#支持多种模式，1、额定初始资金调仓模式，2、无限资金算收益比模式
#远期：支持策略、因子信号计算缓存，现计算校验已有策略已计算数据
import datetime
import logging

import pub_uti_a
import pandas as pd
from enum import Enum

logging.basicConfig(level=logging.INFO, filename='../log/backflow.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

class call_type(Enum):
    open = 1
    filter =2 #2.5%
    middle = 3
    close = 4
class put_type(Enum):
    close_price = 1
    assin_pl_price = 2
class signal_source(Enum):
    csv = 1
    db = 2
class base_config:
    mode = 'ratio'#limit_cash:限定资金；ratio:比值模式
    timeout = 3
    stop_profit = 0.05 * 100#
    stop_loss = -0.05 * 100
    trade_delta = 1#T
    call = call_type.filter
    put = put_type.assin_pl_price
init_capital = 1000000
start_time = ""
end_time = ""

#信号
class signal:
    def __init__(self,id,name,grade,signal_date,trade_code,mark=None):
        self.id = id
        self.name = name
        self.grade = grade
        self.signal_date = signal_date
        self.trade_code = trade_code
        self.conf = base_config
        self.mark = mark

#信号buffer
class signal_hub:
    def __init__(self,start_date,end_date):
        print('signal start.', datetime.datetime.now())
        self.start_date = start_date
        self.end_date = end_date
        self.df = None
        self.signal_csv_path = '../strategy/factor_verify_res/limit_fall_signal.csv'
        self.signal_source = signal_source.db#db#signal_source.csv
        self.select_signal()
        self.signal_buffer = {}
        self.create_signal_buffer()
    def select_signal(self):
        if self.signal_source == signal_source.csv:
            self.select_signal_from_csv()
        elif self.signal_source == signal_source.db:
            self.select_signal_from_db()
        else:
            print('ERROR')
    def select_signal_from_csv(self):
        self.df = pd.read_csv(self.signal_csv_path)
        #恢复账号前导0
        self.df['stock_id'] = self.df['stock_id'].apply(lambda x:str(x).rjust(6,'0'))
    def select_signal_from_db(self):
        sql = "select trade_code,trade_date,stock_id,stock_name,grade" \
              " from limit_up_single " \
              " where trade_date >= '{}' and trade_date <='{}' and grade > 0".format(self.start_date,self.end_date)
        #test
        # sql = "select trade_code,trade_date,stock_id,stock_name,grade" \
        #       " from limit_up_single " \
        #       " where trade_date >= '{}' and trade_date <='{}' and grade > 0 and stock_id='002528'".format(self.start_date,self.end_date)

        # sql = "select trade_code,trade_date,stock_id,stock_name,0 as grade " \
        #       " from stock_trade_data " \
        #       " where trade_date >= '{}' and trade_date <='{}' ".format(self.start_date,self.end_date)

        #验证新版单涨停信号
        # sql = "select trade_code,trade_date,stock_id,stock_name,grade " \
        #       " from limit_up_single_validate " \
        #       " where trade_date >= '{}' and trade_date <='{}' and grade > 1 and type = 'wave' ".format(self.start_date,self.end_date)

        #验证热门回撤
        sql = "select trade_code,trade_date,stock_id,stock_name,grade" \
              " from remen_retracement " \
              " where trade_date >= '{}' and trade_date <='{}' and grade > 0".format(self.start_date, self.end_date)

        self.df = pub_uti_a.creat_df(sql, ascending=True)
        self.df['mark'] = ''#占位
        print('signals select completed.', datetime.datetime.now())
    def create_signal_buffer(self):
        for index,row in self.df.iterrows():
            self.signal_buffer.setdefault(row['trade_date'],[])
            self.signal_buffer[row['trade_date']].append(signal(id = row['stock_id'],name = row['stock_name'],grade=row['grade'],
                                                                signal_date=row['trade_date'],trade_code = row['trade_code'],mark = row['mark']))
        print('signal_buffer completed.',datetime.datetime.now())
        # print('止盈参数：',self.signal_buffer['2022-02-22'][0].conf.stop_profit , '止损参数：', self.signal_buffer['2022-02-22'][0].conf.stop_loss)
    def get_signals_by_date(self,date):
        return self.signal_buffer.get(date,[])
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
    def __init__(self,trade_date,stock_id,stock_name,open_price,close_price,high_price,low_price,increase):
        self.trade_date = trade_date
        self.stock_id = stock_id
        self.stock_name = stock_name
        self.open_price= open_price
        self.close_price= close_price
        self.high_price= high_price
        self.low_price= low_price
        self.increase= increase
class market_hub:
    def __init__(self,start_date,end_date):
        print('market start.', datetime.datetime.now())
        self.start_date = start_date
        self.end_date = end_date
        self.market_df = None
        self.market_buffer = {}
        self.select_market()
        self.create_market_buffer()
    #创建时段内行情信息
    def select_market(self):
        sql = "select trade_code,trade_date,stock_id,stock_name,open_price,close_price,high_price,low_price,increase " \
              " from stock_trade_data " \
              "where trade_date >= '{}' and trade_date <= '{}' ".format(self.start_date,self.end_date)
        self.market_df = pub_uti_a.creat_df(sql)
        print('market select complete.', datetime.datetime.now())
    #create buffer
    def create_market_buffer(self):
        for index,row in self.market_df.iterrows():
            self.market_buffer.setdefault(row['trade_date'],{})
            # print('row',row,'\n',row['trade_date'],self.market_buffer)
            self.market_buffer[row['trade_date']][row['stock_id']] = market(trade_date=row['trade_date'],stock_id = row['stock_id'],
                                                                       stock_name = row['stock_name'],open_price= row['open_price'],close_price= row['close_price'],
                                                                       high_price= row['high_price'],low_price= row['low_price'],increase= row['increase'])
        print('market buffer complete.', datetime.datetime.now())
    def get_market_by_day(self,date,stock_id):
        return self.market_buffer.get(date).get(stock_id)

class position:
    def __init__(self,trade_code,stock_id,stock_name,qty,buy_price,conf,start_date,mark=None):
        self.trade_code = trade_code
        self.stock_id = stock_id
        self.stock_name = stock_name
        self.qty = qty
        self.buy_price = buy_price
        self.sell_price = None
        self.return_ratio = None
        self.hold_days = 0
        self.conf = conf
        self.start_date = start_date
        self.end_date = None
        self.close_type = None
        self.closed_flag = False
        self.mark = mark

class trading:
    def __init__(self,start_date,end_date,sig_hub,market_hub,init_capital=0):
        self.start_date = start_date
        self.end_date = end_date
        self.position_buffer = {}
        self.init_capital = init_capital
        self.available_capital = init_capital
        self.position_value = 0
        self.all_capital = init_capital
        self.single_day_trade = 0
        self.start_grade = 0
        self.trade_count = 0
        self.trade_date_list = []
        self.sig_hub = sig_hub#signal_hub(self.start_date,self.end_date)
        self.market_hub = market_hub#market_hub(self.start_date,self.end_date)
        self.count_return_ratio =0
        self.trade()



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
            market = self.market_hub.get_market_by_day(date,signal.id)
            if not market:
                continue
            last_close_price= market.close_price /(1+(market.increase/100))
            if not market:
                continue
            #开盘价等于收盘价格，且涨幅大于9.75，筛选一字板（未纳入300、688）
            if market.high_price == market.low_price and market.increase >= 9.75:
                logging.info('一字板未能买入：name:{},single_date:{}'.format(signal.name,single_date))
                continue
            #todo 收盘买入需要判断涨停
            #盘中涨幅触发买入
            if signal.conf.call == call_type.filter:
                filter_price = last_close_price * 1.025
                if market.high_price >= filter_price:
                    #todo 持仓唯一判断，是否已有持仓则不再买入
                    #trade，创建持仓
                    buy_price = filter_price
                    self.position_buffer[signal.trade_code] = position(signal.trade_code, signal.id,
                                                                       signal.name, qty = 100,
                                                                       buy_price = buy_price,conf=signal.conf,
                                                                       start_date = date,mark=signal.mark)
            #开盘价买入
            if signal.conf.call == call_type.open:
                buy_price = market.open_price
                self.position_buffer[signal.trade_code] = position(signal.trade_code, signal.id,
                                                                   signal.name, qty=100,
                                                                   buy_price=buy_price, conf=signal.conf,
                                                                   start_date=date)
            #todo 其他买入方式判断

    #平仓操作
    def sell_operate(self,index):
        for id, position in self.position_buffer.items():
            #判断已平仓
            if position.closed_flag == True:
                continue
            position.hold_days += 1
            if position.hold_days == 1:
                continue
            #行情
            stock_id = position.stock_id
            date = self.trade_date_list[index]
            market = self.market_hub.get_market_by_day(date, stock_id)
            if not market:
                continue
            if position.conf.put == put_type.close_price:
                return_ratio = (market.close_price/position.buy_price - 1) * 100
            elif position.conf.put == put_type.assin_pl_price:
                return_ratio = (market.high_price / position.buy_price - 1) * 100
            else:
                print('ERROR: {} not in assin!'.format(position.conf.put_type) )
                continue
            close_type = ''
            # 止盈 按收盘价卖出（涨停不卖）
            if return_ratio >= position.conf.stop_profit:
                #涨停判断
                if market.increase >= 9.75:
                    continue
                if position.conf.put == put_type.assin_pl_price:
                    return_ratio = position.conf.stop_profit
                close_type = '止盈'
            # 止损强平
            elif return_ratio <= position.conf.stop_loss:
                #跌停判断
                if market.increase <= -9.75:
                    continue
                if position.conf.put == put_type.assin_pl_price:
                    return_ratio = position.conf.stop_loss
                close_type = '止损'
            # 超时强平 (程序日期先+1，所以需要大于)
            elif position.hold_days > position.conf.timeout:
                return_ratio = (market.close_price / position.buy_price - 1) * 100
                close_type = '超时'
            else:
                continue
            #并入收益率
            print('return_ratio:',position.stock_name,position.trade_code,position.buy_price,market.close_price,return_ratio,self.count_return_ratio,close_type)
            #筛除异常值
            if return_ratio >= 150:
                return_ratio = 0
            self.count_return_ratio += return_ratio
            #修改平仓标志
            position.closed_flag = True
            #填入平仓信息
            position.close_type = close_type
            position.sell_price = market.close_price
            position.return_ratio = return_ratio
            position.end_date = date


    #创建交易日列表
    def create_tradedate_list(self):
        sql = "select distinct trade_date from stock_trade_data where trade_date >= '{}' and trade_date <= '{}'".format(
            self.start_date,self.end_date
        )
        trade_data_df = pub_uti_a.creat_df(sql,ascending=True)
        self.trade_date_list = trade_data_df['trade_date'].to_list()
        print('trade_date_list:',self.trade_date_list)
    #检验代码逻辑，剩余未平
    def code_verify(self):
        position_count = 0
        for id, position in self.position_buffer.items():
            if not position.closed_flag:
                position_count += 1
            else:
                self.trade_count += 1
        print('未平仓数',position_count,'总成交数：',self.trade_count)
    #持仓转为df输出
    def positions_to_df(self):
        df = pd.DataFrame(columns=['trade code','stock id','stock name','buy price','sell price','start_date',
                                   'end_date','hold days','close type','return ratio','mark'])
        for id, position in self.position_buffer.items():
            df.loc[len(df)] = (position.trade_code, position.stock_id, position.stock_name, position.buy_price,
                               position.sell_price, position.start_date, position.end_date, position.hold_days,
                               position.close_type, position.return_ratio, position.mark)
        time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        df.to_csv('./validate_result/backflow_result_{}.csv'.format(time), index=False)
    #遍历日期，触发建仓平仓
    def trade(self):
        self.create_tradedate_list()
        for index in range(len(self.trade_date_list)):
            print('日期：',self.trade_date_list[index])
            #处理买信号
            self.buy_operate(index)
            #处理平仓
            self.sell_operate(index)
        #检验代码逻辑，剩余未平
        self.code_verify()
        # self.positions_to_df()

def run(start_date='2018-01-01',end_date='2022-06-23'):
    market_h = market_hub(start_date,end_date)
    sug_h = signal_hub(start_date,end_date)
    t = trading(start_date,end_date,sug_h,market_h,init_capital=0)
    print('count_return_ratio:', t.count_return_ratio,'trade_count:',t.trade_count,'per_ratio:',t.count_return_ratio/t.trade_count)
def data_analysis():
    df = pd.read_csv('./validate_result/backflow_result_20220628162406.csv')
    #拆分mark
    df['type'] = df['mark'].apply(lambda x:x[0])
    df['days'] = df['mark'].apply(lambda x: int(x[1:]))
    print(df.head())
    high_type_df = df[(df.type=='H') & (df['return ratio'] < 100 )]
    print('high_type:',len(high_type_df),high_type_df['return ratio'].sum(),high_type_df['return ratio'].sum()/len(high_type_df))
    open_type_df = df[(df.type=='O') & (df['return ratio'] < 100 )]
    print('open_type_df:',len(open_type_df),open_type_df['return ratio'].sum(),open_type_df['return ratio'].sum()/len(open_type_df))
    # test_df = df[(df.type=='O') & (df.days == 4)]
    # print(test_df)
    # test_df.to_csv('./validate_result/limit_up_O_4.csv')
    close_type_df = df[(df.type=='C') & (df['return ratio'] < 100 )]
    print('close_type_df:',len(close_type_df),close_type_df['return ratio'].sum(),close_type_df['return ratio'].sum()/len(close_type_df))
    for type in ('H','O','C'):
        for i in range(1,10):
            s_df = df[(df.type==type) & (df.days == i) & (df['return ratio'] < 100 )]
            print(type,i,len(s_df),s_df['return ratio'].sum(),s_df['return ratio'].sum()/len(s_df))

def hyper_param_pl(start_date='2018-01-01',end_date='2022-06-23'):
    res_str = ''
    market_h = market_hub(start_date,end_date)
    for i in range(1,20):
        print('超参 i：',i)
        base_config.stop_profit = i
        # base_config.stop_loss = -i
        # base_config.timeout = 10
        sug_h = signal_hub(start_date,end_date)
        m = trading(start_date,end_date,sug_h,market_h,init_capital=0)
        res_str += '超参 i：{}, count_return_ratio:{}, trade_count:{} ratio:{}\n'.format(i,m.count_return_ratio,
                                                                                      m.trade_count,m.count_return_ratio/m.trade_count)
    print('result:',res_str)
if __name__ == '__main__':
    #单个
    # run('2022-01-01','2022-06-01')

    #数据分析
    # data_analysis()

    #超参 止盈止损
    hyper_param_pl('2020-01-01','2022-04-15')
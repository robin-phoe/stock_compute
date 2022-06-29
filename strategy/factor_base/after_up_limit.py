'''
研究涨停次日低开
1、次日h_price 低于昨收【H】
2、次日开盘、收盘价低于昨收、h_price高于昨收【O】
3、次日开盘价格高于昨收，收盘价低于昨收【C】
factor name: limit_fall
'''
import pandas as pd
from typedef import market,signal
import pub_uti_a

###核心函数，输入昨收（涨停日），次日高开低收，计算分数
def compute_limit_fall_grade(y_close_price,market:market) -> str:
    type = None
    if market.high_price <= y_close_price:
        type = 'H'
    elif market.high_price > y_close_price and market.open_price <= y_close_price and market.close_price <= y_close_price:
        type = 'O'
    elif market.high_price > y_close_price and market.open_price >= y_close_price and market.close_price <= y_close_price:
        type = 'C'
    return type

###生成信号 输入单只股票行情记录表（升序）
def create_signal(single_df,signal_df):
    #清洗688、300个股 #在上个环节清理
    # single_df['flag'] = 0
    # def clean_fun(row):
    #     if row['stock_id'][0:3] in ('688','300'):
    #         row['flag'] = 1
    #     return row
    # single_df = single_df.apply(clean_fun,axis = 1)
    # single_df = single_df[single_df.flag ==0 ].reset_index(drop=True)
    # del single_df['flag']
    #标记涨停
    single_df['limit_up'] = 0
    def mark_limit(row):
        if row['increase'] >= 9.75:
            row['limit_up'] = 1
        return row
    single_df = single_df.apply(mark_limit,axis = 1)
    ##生成信号,涨停后,低开（三种），单日涨幅>=2.5截至，mark 编辑哦[H|O|C]+天数
    limit_flag = False
    count_days = 0
    limit_fall_type= None#H O C
    for inx,row in single_df.iterrows():
        if row['limit_up'] == 1:
            limit_flag = True
            count_days = 0
            continue
        if limit_flag:
            if row['increase'] < 2.5:
                if count_days == 0:
                    y_close_price = single_df.loc[inx-1,'close_price']
                    market_ins = market(row['trade_date'],row['stock_id'],row['stock_name'],row['open_price'],
                                    row['close_price'],row['high_price'],row['low_price'],row['increase'])
                    limit_fall_type = compute_limit_fall_grade(y_close_price, market_ins)
                    if limit_fall_type == 'H':
                        grade = 30000
                    elif limit_fall_type == 'O':
                        grade = 20000
                    elif limit_fall_type == 'C':
                        grade = 10000
                    else:
                        limit_flag = False
                        continue
                count_days +=1
                mark = limit_fall_type + str(count_days)
                # signal_ins = signal(stock_id=row['stock_id'],name=row['stock_name'],grade=grade,
                #                 signal_date=row['trade_date'],trade_code=row['trade_code'],conf=None,mark=mark)
                signal_df.loc[len(signal_df)] = [row['stock_id'],row['stock_name'],grade,row['trade_date'],row['trade_code'],'',mark]
                # signal_df = signal_add_2_df(signal_ins, signal_df)

            else:
                limit_flag = False
                count_days = 0
    return signal_df


def signal_add_2_df(signal:signal,signal_df):
    # filed_list = {'stock_id','name','grade','signal_date','trade_code','conf','mark'}
    filed_list = {'stock_id', 'stock_name', 'grade', 'trade_date', 'trade_code', 'conf', 'mark'}
    filed_delta = filed_list - set(signal_df.columns.to_list())
    for filed in filed_delta:
        signal_df[filed] = ''
    # signal_df = signal_df[['stock_id','name','grade','signal_date','trade_code','conf','mark']]
    signal_df = signal_df[['stock_id', 'stock_name', 'grade', 'trade_date', 'trade_code', 'conf', 'mark']]
    signal_df.loc[len(signal_df)] = [signal.stock_id,signal.name,signal.grade,signal.signal_date,signal.trade_code,signal.conf,signal.mark]
    return signal_df
        
def main():
    start_t = None
    end_t = None
    if start_t != None and end_t != None:
        sql = "SELECT trade_code,stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price,increase  FROM stock_trade_data \
                where trade_date >= '{0}' and trade_date <= '{1}'".format(start_t, end_t)
    else:
        sql = "SELECT trade_code,stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price,increase  " \
              "FROM stock_trade_data "
        #test
        sql = "SELECT trade_code,stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price,increase  " \
              "FROM stock_trade_data where stock_id = '000001'"
    df = pub_uti_a.creat_df(sql, ascending=True)
    id_set = set(df['stock_id'].to_list())
    signal_df =pd.DataFrame(columns=['stock_id', 'stock_name', 'grade', 'trade_date', 'trade_code', 'conf', 'mark'])
    count = 1
    for id in id_set:
        if id[0:3] in ('688','300'):
            continue
        print('id',count,id)
        count += 1
        single_df = df[df.stock_id == id]
        single_df.reset_index(inplace=True,drop=True)
        signal_df = create_signal(single_df, signal_df)
    signal_df.to_csv('../factor_verify_res/limit_fall_signal_test.csv', index=False)
    # print(signal_df)
if __name__ == '__main__':
    main()
###超参研究
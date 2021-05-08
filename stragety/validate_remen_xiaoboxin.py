#后一日hight price >=3% 触发监控，计触发数1，open price + 3% 计算为call_price
#触发后一日open price 计为 put_open_price, close price 计为 put_close_price
#pl_layer1: (,0] ;  pl_layer2: (0,2.5) ; pl_layer3:[2.5,6),pl_layer4:[6,)
import logging
import pymysql
import pandas as pd
import datetime
import re
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)

logging.basicConfig(level=logging.DEBUG, filename='../log/validate_remen_xiaoboxin.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

db_config = read_config('db_config')
db = pymysql.connect(host=db_config["host"], user=db_config["user"],
                     password=db_config["password"], database=db_config["database"])
def get_df_from_db(sql, db):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    # df = df.set_index(keys=['trade_date'])
    df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
    df.reset_index(inplace=True)
    # df['avg_5'] = df['close_price'].rolling(5).mean()
    cursor.close()
    return df
def sel_remen_xiaoboxin(date):
    sql = "select trade_code,trade_date,stock_id,stock_name from remen_xiaoboxin where trade_date = '{}' ".format(date)
    remen_df = get_df_from_db(sql, db)
    id_list = remen_df['stock_id'].tolist()
    id_tuple = tuple(id_list)
    return remen_df,id_tuple
def deal_data(df):
    def com_pl(row):
        if row['call_high'] ==0 or row['close_price'] ==0:
            return row
        if row['call_high']/row['close_price'] >= 1.03:
            row['call_price'] = row['close_price']*1.03
            pl = row['put_mid_price']/row['call_price'] -1
            if pl <= 0:
                row['pl_layer1'] = 1
            elif 0< pl <0.025:
                row['pl_layer2'] = 1
            elif 0.025<= pl <0.06:
                row['pl_layer3'] = 1
            elif 0.06 <= pl:
                row['pl_layer4'] = 1
        return row
    df = df.apply(com_pl,axis=1)
    return df
def sel_trade_data(date,id_tuple):
    end_date = (datetime.datetime.strptime(date,'%Y-%m-%d')+datetime.timedelta(days=10)).strftime('%Y-%m-%d')
    if len(id_tuple) == 1:
        sql = "select stock_id,stock_name,open_price,close_price,high_price,trade_date " \
              "from stock_trade_data where stock_id = '{0}' and trade_date>='{1}' and trade_date<='{2}'".format(
            id_tuple[0], date, end_date)
    else:
        sql = "select stock_id,stock_name,open_price,close_price,high_price,low_price,trade_date " \
              "from stock_trade_data where stock_id in {0} and trade_date>='{1}' and trade_date<='{2}'".format(
            id_tuple,date,end_date)
    print('sql:',sql)
    trade_df = get_df_from_db(sql,db)
    # print('trade_df:',trade_df)
    trade_df['call_price'] = 0
    trade_df['pl_layer1']= trade_df['pl_layer2']= trade_df['pl_layer3']= trade_df['pl_layer4'] = 0
    trade_df['call_high'] = trade_df.groupby(['stock_id'])['high_price'].shift(-1)
    trade_df['call_open'] = trade_df.groupby(['stock_id'])['open_price'].shift(-1)
    trade_df['call_close'] = trade_df.groupby(['stock_id'])['close_price'].shift(-1)
    trade_df['put_open_price'] = trade_df.groupby(['stock_id'])['open_price'].shift(-2)
    trade_df['put_close_price'] = trade_df.groupby(['stock_id'])['close_price'].shift(-2)
    trade_df['put_high_price'] = trade_df.groupby(['stock_id'])['high_price'].shift(-2)
    trade_df['put_low_price'] = trade_df.groupby(['stock_id'])['low_price'].shift(-2)
    trade_df['put_mid_price'] = (trade_df['put_high_price'] + trade_df['put_low_price'])/2
    trade_df.fillna(0,inplace=True)
    trade_df = deal_data(trade_df)
    #删除不是今日的数据行
    trade_df.drop(trade_df[trade_df.trade_date != date].index, inplace=True)
    #删除call_price = 0的行
    trade_df.drop(trade_df[trade_df.call_price == 0].index, inplace=True)
    trade_df['call_close_pl'] = trade_df['call_close'] / trade_df['call_price'] -1
    trade_df['put_open_pl'] = trade_df['put_open_price'] / trade_df['call_price'] -1
    trade_df['put_close_pl'] = trade_df['put_close_price'] / trade_df['call_price'] -1
    trade_df['put_mid_pl'] = trade_df['put_mid_price'] / trade_df['call_price'] -1
    print(trade_df[['stock_name','stock_id','close_price','call_price','call_high','call_close','put_open_price','put_open_pl',
                    'put_close_price','put_close_pl','put_mid_price','put_mid_pl']])
    call_close_mean = trade_df['call_close_pl'].mean()
    put_open_mean = trade_df['put_open_pl'].mean()
    put_close_mean = trade_df['put_close_pl'].mean()
    put_mid_mean = trade_df['put_mid_pl'].mean()
    count_df = len(trade_df)
    pl_layer1_count = trade_df['pl_layer1'].tolist().count(1)
    pl_layer2_count = trade_df['pl_layer2'].tolist().count(1)
    pl_layer3_count = trade_df['pl_layer3'].tolist().count(1)
    pl_layer4_count = trade_df['pl_layer4'].tolist().count(1)
    print('call_close_mean:{0},put_open_mean:{1},put_close_mean:{2},put_mid_mean:{3},\n,pl_layer1_count:{4},'
          'pl_layer2_count:{5},pl_layer3_count:{6},pl_layer4_count:{7},count_df:{8}'.format(
        call_close_mean,put_open_mean,put_close_mean,put_mid_mean,
        pl_layer1_count,pl_layer2_count,pl_layer3_count,pl_layer4_count,count_df))
    return [date,call_close_mean,put_open_mean,put_close_mean,put_mid_mean,
            count_df,pl_layer1_count,pl_layer2_count,pl_layer3_count,pl_layer4_count]
    

def main(date):
    if date == None:
        date = datetime.datetime.now().strftime('%Y-%m-%d')
    remen_df,id_tuple = sel_remen_xiaoboxin(date)
    if len(id_tuple) == 0:
       return []
    res_list = sel_trade_data(date, id_tuple)
    return res_list
def history(start_date,end_date):
    data = {'date': [],
            'call_close_mean': [],
            'put_open_mean': [],
            'put_close_mean': [],
            'put_mid_mean': [],
            'count_df':[],
            'pl_layer1_count':[],
            'pl_layer2_count':[],
            'pl_layer3_count':[],
            'pl_layer4_count':[],
            }
    res_df = pd.DataFrame(data)
    sql = "select distinct(trade_date) from com_redu_test where trade_date >= '{}' and trade_date <= '{}'".format(start_date,end_date)
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    date_tuple = cursor.fetchall()
    print('date_tuple:',date_tuple)
    cursor.close()
    for date in date_tuple:
        date_str = date[0].strftime("%Y-%m-%d")
        res_list = main(date_str)
        if len(res_list) == 0:
            continue
        res_df.loc[len(res_df)] = res_list
    row = len(res_df)
    res_df.loc[row, 'call_close_mean'] = res_df['call_close_mean'].mean()
    res_df.loc[row,'put_open_mean'] = res_df['put_open_mean'].mean()
    res_df.loc[row, 'put_close_mean'] = res_df['put_close_mean'].mean()
    res_df.loc[row, 'put_mid_mean'] = res_df['put_mid_mean'].mean()
    res_df.loc[row, 'count_df'] = res_df['count_df'].sum()
    res_df.loc[row, 'pl_layer1_count'] = res_df['pl_layer1_count'].sum()
    res_df.loc[row, 'pl_layer2_count'] = res_df['pl_layer2_count'].sum()
    res_df.loc[row, 'pl_layer3_count'] = res_df['pl_layer3_count'].sum()
    res_df.loc[row, 'pl_layer4_count'] = res_df['pl_layer4_count'].sum()
    res_df['pl_layer1_rate'] = res_df['pl_layer1_count'] / res_df['count_df'] * 100
    res_df['pl_layer2_rate'] = res_df['pl_layer2_count'] / res_df['count_df'] * 100
    res_df['pl_layer3_rate'] = res_df['pl_layer3_count'] / res_df['count_df'] * 100
    res_df['pl_layer4_rate'] = res_df['pl_layer4_count'] / res_df['count_df'] * 100
    file_name = "./validate_report/validate_xiaoboxin.csv"
    res_df.to_csv(file_name,encoding='utf-8')
if __name__ == '__main__':
    date = '2021-04-29'
    # main(date)
    history('2021-04-01','2021-04-29')
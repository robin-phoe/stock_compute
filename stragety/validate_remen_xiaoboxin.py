import logging
import pymysql
import pandas as pd
import datetime
import re
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config

logging.basicConfig(level=logging.DEBUG, filename='../log/validate_redu_5.log', filemode='w',
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
def sel_remen_5(date):
    sql = "select trade_code,trade_date,stock_id,stock_name from com_redu_test where trade_date = '{}' and redu_5 >= 10000".format(date)
    redu_df = get_df_from_db(sql, db)
    id_list = redu_df['stock_id'].tolist()
    id_tuple = tuple(id_list)
    return redu_df,id_tuple
def sel_trade_data(date,id_tuple):
    end_date = (datetime.datetime.strptime(date,'%Y-%m-%d')+datetime.timedelta(days=20)).strftime('%Y-%m-%d')
    if len(id_tuple) == 1:
        sql = "select stock_id,stock_name,close_price,high_price,trade_date " \
              "from stock_trade_data where stock_id = '{0}' and trade_date>='{1}' and trade_date<='{2}'".format(
            id_tuple[0], date, end_date)
    else:
        sql = "select stock_id,stock_name,close_price,high_price,trade_date " \
              "from stock_trade_data where stock_id in {0} and trade_date>='{1}' and trade_date<='{2}'".format(
            id_tuple,date,end_date)
    print('sql:',sql)
    trade_df = get_df_from_db(sql,db)
    trade_df['high'] = trade_df.groupby(['stock_id'])['high_price'].shift(-1)
    print('trade_df:', trade_df)
    trade_df.drop(trade_df[trade_df.trade_date != date].index, inplace=True)
    # print('trade_df:',trade_df)
    trade_df.reset_index(inplace=True)
    trade_df['increase_max'] = trade_df['high']/trade_df['close_price'] - 1
    print('trade_df:',trade_df)
    trade_df['increase_flag'] = trade_df['increase_max'].apply(lambda x: '1' if 0.03<x<0.05 else ('2' if x>=0.05 else '0'))
    count_layer_1 = trade_df['increase_flag'].tolist().count('1')
    count_layer_2 = trade_df['increase_flag'].tolist().count('2')
    print(count_layer_1,count_layer_2)
    len_trade_df = len(trade_df)
    count_layer_1_rate = count_layer_1 / len_trade_df
    count_layer_2_rate = count_layer_2 / len_trade_df
    logging.info('date:{0},count_layer_1_rate:{1},count_layer_2_rate:{2}'.format(date,count_layer_1_rate,count_layer_2_rate))
    print('date:{0},count_layer_1_rate:{1},count_layer_2_rate:{2}'.format(date,count_layer_1_rate,count_layer_2_rate))
    return [date,len_trade_df,count_layer_1,count_layer_1_rate,count_layer_2,count_layer_2_rate]
def main(date):
    if date == None:
        date = datetime.datetime.now().strftime('%Y-%m-%d')
    redu_df,id_tuple = sel_remen_5(date)
    if len(id_tuple) == 0:
       return []
    res_list = sel_trade_data(date, id_tuple)
    return res_list
def history(start_date,end_date):
    data = {'date': [],
            'amount': [],
            'layer_1_count': [],
            'layer_1_rate': [],
            'layer_2_count': [],
            'layer_2_rate': []
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
    res_df['reach_rate'] = res_df['layer_1_rate'] + res_df['layer_2_rate']
    row = len(res_df)
    res_df['reach_count'] = res_df['layer_1_count'] + res_df['layer_2_count']
    res_df.loc[row, 'layer_1_count'] = res_df['layer_1_count'].mean()
    res_df.loc[row,'layer_1_rate'] = res_df['layer_1_rate'].mean()
    res_df.loc[row, 'layer_1_rate'] = res_df['layer_1_rate'].mean()
    res_df.loc[row, 'layer_2_count'] = res_df['layer_2_count'].mean()
    res_df.loc[row, 'layer_2_rate'] = res_df['layer_2_rate'].mean()
    res_df.loc[row, 'reach_rate'] = res_df['reach_rate'].mean()
    file_name = "./validate_report/validate_remen_5.csv"
    res_df.to_csv(file_name,encoding='utf-8')
if __name__ == '__main__':
    date = '2021-04-22'
    # main(date)
    history('2018-01-01','2021-04-29')
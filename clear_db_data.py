import pymysql
from multiprocessing import Pool
import pandas as pd
import numpy as np


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
    df = df.dropna(axis=0, how='any')
    df.reset_index(inplace=True)
    # df['trade_date2'] = df['trade_date'].copy()
    # print('trade_date2:',type(df['trade_date2'][0]))
    # df['trade_date2'] = pd.to_datetime(df['trade_date2']).map(date2num)
    # df['dates'] = np.arange(0, len(df))
    # df['avg_10'] = df['close_price'].rolling(10).mean()
    # df['avg_5'] = df['close_price'].rolling(5).mean()
    cursor.close()
    # # print(df)
    # # df['trade_date'] = date2num(df['trade_date'])
    # print('df:', df[['avg_10', 'close_price']])
    return df
def make_data(df,db,h_table):
    cursor = db.cursor()
    for i in range(1,len(df)):
        increase = (df.loc[i,'close_price'] - df.loc[i-1,'close_price'])/df.loc[i-1,'close_price']
        print('increase:',increase,'trade_code:',df.loc[i,'trade_code'])
        try:
            sql = "update stock_history_trade{0} set increase = '{1}' where trade_code = '{2}'".format(h_table,increase,df.loc[i,'trade_code'])
            print('sql:',sql)
            cursor.execute(sql)  # 执行SQL语句
            db.commit()
            print('存储成功。')
        except Exception as err:
            db.rollback()
            print('存储失败:', err)
    cursor.close()
def main(h_table):
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db.cursor()
    sql = "select distinct stock_id from stock_history_trade{0}".format(h_table)
    cursor.execute(sql)  # 执行SQL语句
    id_list = cursor.fetchall()
    cursor.close()
    for id in id_list:
        id = id[0]
        sql = 'select trade_code,close_price,trade_date from stock_history_trade{0} where stock_id = {1}'.format(h_table,id)
        df = get_df_from_db(sql,db)
        print(df)
        make_data(df, db, h_table)

def run():
    p = Pool(8)
    for i in range(1, 11):
        p.apply_async(main, args=(str(i),))
    #    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
if __name__ == '__main__':
    # h_table = '1'
    # main(h_table)

    run()

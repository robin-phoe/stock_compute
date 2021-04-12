'''
数据来源：东方财富网-行情中心
http://quote.eastmoney.com/center
'''
#coding=utf-8
import requests
import re
import pymysql
#import pandas as pd
import logging
#import threading
from multiprocessing import Pool

logging.basicConfig(level=logging.DEBUG,filename='stock_history_trade.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')



def select_info(table,cursor,db):
    sql="select stock_id from stock_informations where h_table={}".format(table)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    #print(stock_id_list)
    return stock_id_list
def get_data(table,stock_id,cursor,db):
    if stock_id[0]=='6':
        url="http://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.{}&fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&beg=20181001&end=20220101&ut=fa5fd1943c7b386f172d6893dbfba10b".format(stock_id)
        url2="http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&volt=2&fields=f58,f84,f85&secid=1.{}".format(stock_id)
    elif stock_id[0]=='0':
        url="http://push2his.eastmoney.com/api/qt/stock/kline/get?secid=0.{}&fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&beg=20181001&end=20220101&ut=fa5fd1943c7b386f172d6893dbfba10b".format(stock_id)
        url2="http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&volt=2&fields=f58,f84,f85&secid=0.{}".format(stock_id)
    else:
        return 0
    header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url,headers=header)
    response2 = requests.get(url2,headers=header)
    text=response.text
    text2=response2.text
    #print('text2:',text2)
    res_capital=re.findall('"f84":(.*?),"f85":(.*?)}',text2)
    #print('text:',res_capital)
    datas=re.findall('"data":{"code":"(.*?)",.*?"name":"(.*?)",.*?,"klines":\[(.*?)\]',text)
    if len(datas)==0:
        return 0
    #print(datas)
    data_days_list=datas[0][2].split('","')
    val=[]
    #print('data_days_list:',data_days_list)
    if len(data_days_list[0])==0 or len(res_capital)==0:
        logging.error('data_days_list or res_capital is null:{},{}'.format(data_days_list,res_capital))
        return 0
    for data_day in data_days_list:
        data_day=data_day.replace('"','')
        data_list=data_day.split(',')
        #print('data_list:',data_list,res_capital)
        #print('data_list[5]:',data_list[5],res_capital[0][1])
        if data_list[5]=='"-"' or res_capital[0][1] == '"-"':
            continue
        #print(datas[0][0],datas[0][1],data_list)
        data_str=re.sub('-','',data_list[0])
        trade_code=data_str+datas[0][0]
        turnover_rate=float(data_list[5])/float(res_capital[0][1])*100
        #print('turnover_rate:',turnover_rate)
        #print('all:',trade_code,datas[0][0],datas[0][1],data_list[0],data_list[1],data_list[2],data_list[3],data_list[4],data_list[5],data_list[6],res_capital[0][1],res_capital[0][0],turnover_rate)
        val.append((trade_code,datas[0][0],datas[0][1],data_list[0],data_list[1],data_list[2],data_list[3],data_list[4],data_list[5],data_list[6],res_capital[0][1],res_capital[0][0],turnover_rate,trade_code))
        #val=((trade_code,datas[0][0],datas[0][1],data_list[0],data_list[1],data_list[2],data_list[3],data_list[4],data_list[5],data_list[6],res_capital[0][1],res_capital[0][0],str(turnover_rate)),)
        '''
        try:
            sql="insert into stock_history_trade{13}(trade_code,stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price,trade_amount,trade_money,circulation,capital_stock,turnover_rate) \
                values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}')\
                ".format(trade_code,datas[0][0],datas[0][1],data_list[0],data_list[1],data_list[2],data_list[3],data_list[4],data_list[5],data_list[6],res_capital[0][1],res_capital[0][0],turnover_rate,table)
            cursor.execute(sql)
            db.commit()
            print('存储完成')
            logging.info('存储完成:id:{},name:{}'.format(datas[0][0],datas[0][1]))
        except Exception as err:
            db.rollback()
            print('存储失败:',err)
            logging.error('存储失败:id:{},name:{}\n{}\n{}'.format(datas[0][0],datas[0][1],data_list,err))
        '''
        #print('val:',val)
    try:
    #if 1:
        #取新值
        sql="replace into stock_history_trade{}(trade_code,stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price,trade_amount,trade_money,circulation,capital_stock,turnover_rate) \
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(table)
        '''% \
            (trade_code,datas[0][0],datas[0][1],data_list[0],data_list[1],data_list[2],data_list[3],data_list[4],data_list[5],data_list[6],res_capital[0][1],res_capital[0][0],str(turnover_rate))
        '''
        '''
        sql="insert into stock_history_trade{13}(trade_code,stock_id,stock_name,trade_date,open_price,close_price,high_price,low_price,trade_amount,trade_money,circulation,capital_stock,turnover_rate) \
                values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}')"
        '''
        #更新
        sql = "update stock_history_trade{} set trade_code=%s,stock_id=%s,stock_name=%s,trade_date=%s,open_price=%s,close_price=%s," \
              "high_price=%s,low_price=%s,trade_amount=%s,trade_money=%s,circulation=%s,capital_stock=%s,turnover_rate=%s where trade_code=%s ".format(table)
        #print('tuple(val):',val)
        #print('tuple(sql):',sql)
        cursor.executemany(sql,val)
        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{},name:{}'.format(datas[0][0],datas[0][1]))
    except Exception as err:
    #else:
        db.rollback()
        print('存储失败:',err)
        logging.error('存储失败:id:{},name:{}\n{}\n{}'.format(datas[0][0],datas[0][1],data_list,err))

def make_one_table(table):
    db = pymysql.connect(host="localhost", user="root", password="Zzl08382020", database="stockdb")
    cursor = db.cursor()
    #get_data(stock_id='603828')#000790
    #get_data(stock_id='000790')
    stock_id_list=select_info(table,cursor,db)
    #stock_id_list = [('603931',)]
    #stock_id_list = [('000790',)]
    for stock in stock_id_list:
        print('stock[0]:',stock[0])
        get_data(table,stock[0],cursor,db)
    print('表：',table)


def run():
    p = Pool(8)
    for i in range(1,11):
        p.apply_async(make_one_table, args=(i,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')

if __name__ == '__main__':
    # run()
    make_one_table(1)
    # p = Pool(8)
    # for i in range(1,11):
    #     p.apply_async(make_one_table, args=(i,))
    # print('Waiting for all subprocesses done...')
    # p.close()
    # p.join()
    # print('All subprocesses done.')
    #.start()
##    for i in range(1,11):
##        t_name='thread'+str(i)
##        myThread(i, t_name, i).start()
##    print ("退出主线程")
##    t1=myThread(1, 't1', 1)
##    t2=myThread(2, 't2', 2)
##    t3=myThread(3, 't3', 3)
##    t4=myThread(4, 't1', 4)
##    t5=myThread(5, 't1', 5)
##    t6=myThread(6, 't1', 6)
##    t7=myThread(7, 't1', 7)
##    t8=myThread(8, 't1', 8)
##    t9=myThread(9, 't1', 9)
##    t10=myThread(10, 't1', 10)
##    t1.start()
##    t2.start()
##    t1.join()
##    t2.join()
##    print ("退出主线程")


'''
数据来源：东方财富网-行情中心
http://quote.eastmoney.com/center
'''
#coding=utf-8
import requests
import re
import logging
import pymysql
from multiprocessing import Pool
from clear_db_data_trade import clear_main
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config

logging.basicConfig(level=logging.DEBUG, filename='../log/get_history_trade.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')



def select_info(table,db):
    cursor = db.cursor()
    sql="select stock_id from stock_informations where h_table={}".format(table)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    cursor.close()
    #print(stock_id_list)
    return stock_id_list
def get_data(table,stock_id,cursor,db,start_date,end_date):
    if stock_id[0]=='6':
        url="http://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.{0}&fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&beg={1}&end={2}&ut=fa5fd1943c7b386f172d6893dbfba10b".format(stock_id,start_date,end_date)
        url2="http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&volt=2&fields=f58,f84,f85&secid=1.{}".format(stock_id)
    elif stock_id[0]=='0' or stock_id[0]=='3':
        url="http://push2his.eastmoney.com/api/qt/stock/kline/get?secid=0.{0}&fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&beg={1}&end={2}&ut=fa5fd1943c7b386f172d6893dbfba10b".format(stock_id,start_date,end_date)
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
    val_insert=[]
    val_update = []
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
        turnover_rate=float(data_list[5])/float(res_capital[0][1])*10000
        # print('turnover_rate:',float(data_list[5]),float(res_capital[0][1]),turnover_rate)
        #print('all:',trade_code,datas[0][0],datas[0][1],data_list[0],data_list[1],data_list[2],data_list[3],data_list[4],data_list[5],data_list[6],res_capital[0][1],res_capital[0][0],turnover_rate)
        val_update.append((trade_code,datas[0][0],datas[0][1],data_list[0],data_list[1],data_list[2],data_list[3],data_list[4],
                           data_list[5],data_list[6],res_capital[0][1],res_capital[0][0],turnover_rate,trade_code))
        val_insert.append((trade_code, datas[0][0], datas[0][1], data_list[0], data_list[1], data_list[2], data_list[3],
                           data_list[4], data_list[5], data_list[6], res_capital[0][1], res_capital[0][0],
                           turnover_rate))
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
        # print('val_insert:',val_insert)
        # print('val_update:', val_update)
    try:
        #取新值
        sql="insert into stock_trade_data(trade_code,stock_id,stock_name,trade_date,open_price,close_price,high_price," \
            "low_price,trade_amount,trade_money,circulation,capital_stock,turnover_rate) \
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        sql="insert into stock_trade_data(trade_code,stock_name,stock_id,trade_date,close_price,increase," \
                    "open_price,turnover_rate,P_E,P_B,high_price,low_price,trade_amount,trade_money) " \
                    "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')" \
                    "ON DUPLICATE KEY UPDATE trade_code='{0}',stock_name='{1}',stock_id='{2}',trade_date='{3}'," \
                    "close_price='{4}',increase='{5}',open_price='{6}',turnover_rate='{7}'," \
                    "P_E='{8}',P_B='{9}',high_price='{10}',low_price='{11}',trade_amount='{12}',trade_money='{13}'" \
                    .format(trade_code,data['f14'],data['f12'],date_str,data['f2'],data['f3'],data['f17'],
                            data['f8'],data['f9'],data['f23'],data['f15'],data['f16'],data['f5'],data['f6'])
        cursor.executemany(sql, val_insert)
        #更新
        # sql = "update stock_history_trade{} set trade_code=%s,stock_id=%s,stock_name=%s,trade_date=%s,open_price=%s,close_price=%s," \
        #       "high_price=%s,low_price=%s,trade_amount=%s,trade_money=%s,circulation=%s,capital_stock=%s,turnover_rate=%s where trade_code=%s ".format(table)
        # cursor.executemany(sql,val_update)

        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{},name:{}'.format(datas[0][0],datas[0][1]))
    except Exception as err:
    #else:
        db.rollback()
        print('存储失败:',err)
        logging.error('存储失败:id:{},name:{}\n{}\n{}'.format(datas[0][0],datas[0][1],data_list,err))

def make_one_table(table,start_date,end_date):
    db_config = read_config('db_config')
    db = pymysql.connect(host=db_config["host"], user=db_config["user"], password=db_config["password"],
                         database=db_config["database"])
    cursor = db.cursor()
    #清除原数据
    # h_table = "stock_history_trade{}".format(table)
    # sql = "delete from {}".format(h_table)
    # cursor.execute(sql)
    # print('已清除原数据')

    #get_data(stock_id='603828')#000790
    #get_data(stock_id='000790')
    stock_id_list=select_info(table,db)
    #stock_id_list = [('603931',)]
    # stock_id_list = [('002831',)]

    start_date_str = re.sub('-','',start_date) #YYYYmmdd
    end_date_str = re.sub('-','',end_date) #YYYYmmdd
    for stock in stock_id_list:
        print('stock[0]:',stock[0])
        get_data(table,stock[0],cursor,db,start_date_str,end_date_str)
    print('表：',table)
    clear_main(table,start_date,end_date)

def run(start_date,end_date):
    p = Pool(8)
    for i in range(0,10):
        p.apply_async(make_one_table, args=(i,start_date,end_date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')

if __name__ == '__main__':
    start_date = '2022-04-06'
    end_date = '2022-04-15'
    run(start_date,end_date)
    # make_one_table(0,start_date,end_date)


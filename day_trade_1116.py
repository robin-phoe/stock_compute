



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
import json
import datetime
logging.basicConfig(level=logging.DEBUG,filename='stock_day_trade1.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
with open('stock_day_trade1.log','r') as f:
    f.read()
    print(f)
db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
cursor = db.cursor()
count=0

#获取单个页面股票数据
def getOnePageStock(page):
    global count
    url = "http://18.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406268274658974922_1605597357094&pn={}" \
          "&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13," \
          "m:0+t:80,m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20," \
          "f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1605597357108".format(page)
    #url = 'http://18.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406268274658974922_1605597357094&pn=2&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1605597357108'
    print('url:',url)
    header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url,headers=header)
    text=response.text
    result = re.findall('\[.*?\]', text)
    print('result:',result)
    if len(result) == 0:
        return 0
    else:
        Data_json = json.loads(result[0])
    date_str = datetime.datetime.now().strftime('%Y%m%d')
    print('date_str:',date_str)
    for data in Data_json:
        print('data:',data)
        trade_code=date_str+data['f12']
        print(trade_code)
        sql = "select h_table from stock_informations where stock_id={}".format(data['f12'])
        cursor.execute(sql)
        h_table = cursor.fetchall()
        print('h_table1:',h_table)
        if len(h_table) != 0:
            h_table = h_table[0][0]
            print('h_table2:', h_table)
            try:
                sql="insert into stock_history_trade{14}(trade_code,stock_name,stock_id,trade_date,close_price,increase," \
                    "open_price,turnover_rate,P_E,P_B,high_price,low_price,trade_amount,trade_money) " \
                    "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')" \
                    "ON DUPLICATE KEY UPDATE trade_code='{0}',stock_name='{1}',stock_id='{2}',trade_date='{3}'," \
                    "close_price='{4}',increase='{5}',open_price='{6}',turnover_rate='{7}'," \
                    "P_E='{8}',P_B='{9}',high_price='{10}',low_price='{11}',trade_amount='{12}',trade_money='{13}'" \
                    .format(trade_code,data['f14'],data['f12'],date_str,data['f2'],data['f3'],data['f17'],
                            data['f8'],data['f9'],data['f23'],data['f15'],data['f16'],data['f5'],data['f6'],h_table)
                cursor.execute(sql)
                db.commit()
                print('存储完成:page:{},id:{},name:{}'.format(page,data['f12'],data['f14']))
                logging.info('存储完成:page:{},id:{},name:{}'.format(page,data['f12'],data['f14']))
                #存储当日收盘数据
                sql="insert into last_day_price(stock_id,close_price,increase) " \
                    "values('{0}','{1}','{2}')" \
                    "ON DUPLICATE KEY UPDATE stock_id='{0}',close_price='{1}',increase='{2}'" \
                    .format(data['f12'],data['f2'],data['f3'])
                cursor.execute(sql)
                db.commit()
                count += 1
                print('count:',count)
            except Exception as err:
                db.rollback()
                logging.error('存储失败:page:{},id:{},name:{},err:{}'.format(page,data['f12'],data['f14'],err))
                print('存储失败:page:{},id:{},name:{},err:{}'.format(page,data['f12'],data['f14'],err))
    return 1
def main():
    flag = 1
    page = 1
    while flag:
        flag = getOnePageStock(str(page))
        page = int(page) + 1
if __name__ == '__main__':
    main()
    print('completed!')


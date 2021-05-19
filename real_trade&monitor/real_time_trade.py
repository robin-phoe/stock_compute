



'''
数据来源：东方财富网-行情中心
http://quote.eastmoney.com/center
'''
#coding=utf-8
import requests
import re
import pymysql
import logging
import json
import datetime
import time
from multiprocessing import Pool
import redis
logging.basicConfig(level=logging.DEBUG,filename='../log/real_trade_data.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

db = pymysql.connect(host="192.168.1.6", user="user1", password="Zzl08382020", database="stockdb" )
cursor = db.cursor()
count=0
r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
"""
获取单个页面股票数据
"""
def getOnePageStock(page):
    global count,r
    url = "http://18.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406268274658974922_1605597357094&pn={}" \
          "&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13," \
          "m:0+t:80,m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20," \
          "f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1605597357108".format(page)
    url = "http://18.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406268274658974922_1605597357094&pn={}" \
          "&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13," \
          "m:0+t:80,m:1+t:2,m:1+t:23&fields=f2,f3,f12&_=1605597357108".format(page)
    #url = 'http://18.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406268274658974922_1605597357094&pn=2&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1605597357108'
    #print('url:',url)
    header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url,headers=header)
    text=response.text
    result = re.findall('\[.*?\]', text)
    print('result:',page,result)
    if len(result) == 0:
        return 0
    else:
        Data_json = json.loads(result[0])
        #print(Data_json)
    for data in Data_json:
        # print('data:',data)
        id = data['f12']
        price = data['f2']
        increase = data['f3']
        if price == '-':
            price = 0
        if increase == '-':
            increase = 0
        # r.set(id + '_price',data['f2'])
        # r.set(id + '_increase', data['f3'])
        r.lpush(id + '_price_list', price)
        r.lpush(id + '_increase_list', increase)
    #     trade_code=date_str+data['f12']
    #     print(trade_code)
    #     sql = "select h_table from stock_informations where stock_id={}".format(data['f12'])
    #     cursor.execute(sql)
    #     h_table = cursor.fetchall()
    #     print('h_table1:',h_table)
    #     if len(h_table) != 0:
    #         h_table = h_table[0][0]
    #         print('h_table2:', h_table)
    #         try:
    #             sql="insert into stock_history_trade{14}(trade_code,stock_name,stock_id,trade_date,close_price,increase," \
    #                 "open_price,turnover_rate,P_E,P_B,high_price,low_price,trade_amount,trade_money) " \
    #                 "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')" \
    #                 "ON DUPLICATE KEY UPDATE trade_code='{0}',stock_name='{1}',stock_id='{2}',trade_date='{3}'," \
    #                 "close_price='{4}',increase='{5}',open_price='{6}',turnover_rate='{7}'," \
    #                 "P_E='{8}',P_B='{9}',high_price='{10}',low_price='{11}',trade_amount='{12}',trade_money='{13}'" \
    #                 .format(trade_code,data['f14'],data['f12'],date_str,data['f2'],data['f3'],data['f17'],
    #                         data['f8'],data['f9'],data['f23'],data['f15'],data['f16'],data['f5'],data['f6'],h_table)
    #             cursor.execute(sql)
    #             db.commit()
    #             print('存储完成:page:{},id:{},name:{}'.format(page,data['f12'],data['f14']))
    #             logging.info('存储完成:page:{},id:{},name:{}'.format(page,data['f12'],data['f14']))
    #             count += 1
    #             print('count:',count)
    #         except Exception as err:
    #             db.rollback()
    #             logging.error('存储失败:page:{},id:{},name:{},err:{}'.format(page,data['f12'],data['f14'],err))
    #             print('存储失败:page:{},id:{},name:{},err:{}'.format(page,data['f12'],data['f14'],err))
    return 1
"""
获取日内板块信息
"""
def get_bk_info(page):
    global r
    url = "http://44.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112404219198714508219_1607384110344&pn={}&pz=20&po=1&np=1&" \
          "ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12," \
          "f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207," \
          "f208,f209,f222&_=1607384110349".format(page)

    header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url,headers=header)
    text=response.text
    result = re.findall('\[.*?\]', text)
    print('result:',page,result)
    if len(result) == 0:
        return 0
    else:
        Data_json = json.loads(result[0])
        #print(Data_json)
    for data in Data_json:
        # print('data:',data)
        bk_name = data['f14']
        bk_id = data['f12']
        increase = data['f3']
        if increase == '-':
            increase = 0
        r.hset('bk_increase', bk_id,increase)
        logging.info('bk_increase:{},{}'.format(bk_id,increase))
        print('bk_increase:{},{}'.format(bk_id,increase))
"""
将键值对行情存入列表
废弃。修改后行情直接存入列表
"""
def market_tranfer():
    len_monitor = r.llen('monitor_list')
    print('len_monitor',len_monitor)
    if len_monitor == 0:
        return
    monitor_list = r.lrange('monitor_list',0,len_monitor)
    print('monitor_list',monitor_list)
    for id in monitor_list:
        # print('id:',id)
        if r.get(id+'_price') != None and r.get(id + '_increase') != None:
            r.lpush(id+'_price_list',r.get(id+'_price'))
            # print(id+'price_list:',r.lrange(id+'_price_list',0,r.llen(id+'_price_list')))
            r.lpush(id + '_increase_list', r.get(id + '_increase'))
"""
个股、板块单次页面获取
"""
def main(page):
    # print('时间1:',datetime.datetime.now().strftime('%H:%M:%S,%f'))
    # print('page:',page)
    getOnePageStock(str(page))
    if int(page) <=5:
        get_bk_info(page)

"""
多进程执行获取行情
"""
def run():
    p = Pool(8)
    for i in range(220):
        p.apply_async(main, args=(str(i),))
    #    p.apply_async(main, args=('1',date,))
    #print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    # market_tranfer()
    # print('All subprocesses done.')
if __name__ == '__main__':
    run()
    # main(page=1)
    # i=0
    # flush_flag = 1
    # while True:
    #     time_now = datetime.datetime.now().strftime("%H:%M:%S")
    #     if flush_flag ==1:
    #         r.flushdb()
    #         print('已清空redis')
    #         flush_flag = 0
    #     elif time_now >= "09:26:00" and time_now <= "15:30:00" :#
    #         time1 = datetime.datetime.now()
    #         # main()
    #         run()
    #         time2 = datetime.datetime.now()
    #         time_delta = time2 - time1
    #         print('时间:',i,  time2.strftime('%H:%M:%S,%f'))
    #         print('时间差:',time_delta)
    #         i+=1
    #         time.sleep(50)
    #     time.sleep(1)


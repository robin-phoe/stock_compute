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
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config

logging.basicConfig(level=logging.DEBUG,filename='../log/bankuai_trade.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

db_config = read_config('db_config')
db = pymysql.connect(host=db_config["host"], user=db_config["user"],
                     password=db_config["password"], database=db_config["database"])

count=0
bk_dict = {}
"""获取单个板块历史数据"""
def get_history():
    cursor = db.cursor()
    sql ="select distinct bankuai_code,bankuai_name from bankuai_day_data"
    cursor.execute(sql)
    bankuai_info = cursor.fetchall()
    print('bankuai_info:',bankuai_info)
    bk_dict = {}
    for i in range(len(bankuai_info)):
        bk_dict[bankuai_info[i][0]] = bankuai_info[i][1]
    print('bk_dict:',bk_dict)
    for bk in bk_dict:
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery112409565109930423827_1607350575238&secid=90.{0}&ut=" \
              "fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58&klt=" \
              "101&fqt=0&beg={1}&end={2}&_=1607350575239".format(bk,'20200101','20220101') #20200101
        print('url:',url)
        header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
        response = requests.get(url,headers=header)
        text=response.text
        print('text:',text)
        result = re.findall('\[.*?\]', text)
        print('result:',result)
        if len(result) == 0:
            continue
        else:
            Data_json = json.loads(result[0])
            print('Data_json:',Data_json)
        for day_data_str in Data_json:
            day_data = day_data_str.split(',')
            bk_id = re.sub('-','',day_data[0]) + bk
            print('day_data',day_data,bk_id)
            # increase = day_data[1]/day_data[2] - 1
            try:
                sql="insert into bankuai_day_data(bankuai_id,bankuai_name,bankuai_code,trade_date,open_price,close_price," \
                    "high_price,low_price,amount,amount_money,zhenfu) " \
                    "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}')" \
                    "ON DUPLICATE KEY UPDATE bankuai_id='{0}',bankuai_name='{1}',bankuai_code='{2}',trade_date='{3}'," \
                    "open_price='{4}',close_price='{5}',high_price='{6}',low_price='{7}'," \
                    "amount='{8}',amount_money='{9}',zhenfu='{10}'" \
                    .format(bk_id,bk_dict[bk],bk,day_data[0],day_data[1],day_data[2],day_data[3],
                            day_data[4],day_data[5],day_data[6],day_data[7])
                cursor.execute(sql)
                db.commit()
                print('存储完成:id:{},name:{},trade_date:{}'.format(bk,bk_dict[bk],day_data[0]))
                logging.info('存储完成:id:{},name:{},trade_date:{}'.format(bk,bk_dict[bk],day_data[0]))
            except Exception as err:
                db.rollback()
                logging.error('存储失败:id:{},name:{},trade_date:{},err:{}'.format(bk,bk_dict[bk],day_data[0],err))
                print('存储失败:id:{},name:{},trade_date:{},err:{}'.format(bk,bk_dict[bk],day_data[0],err))
    cursor.close()
def com_his_rank(start_date = '2020-01-01',end_date = '2021-05-15'):
    global bk_dict
    cursor = db.cursor()
    datestart = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    dateend = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    date_list = []
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        date_str = datestart.strftime('%Y-%m-%d')
        print('date_str',date_str)
        date_list.append(date_str)
    for date in date_list:
        sql = "select bankuai_id,(close_price/open_price-1) as increase from bankuai_day_data where trade_date = '{}'".format(date)
        cursor.execute(sql)
        bk_dict = dict(cursor.fetchall())
        print('bk_dict:',bk_dict)
        save_sort()
    cursor.close()
"""获取当日全部板块信息"""
def getOnePageStock(page,date_str):
    global count
    cursor = db.cursor()
    url = "http://44.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112404219198714508219_1607384110344&pn={}&pz=20&po=1&np=1&" \
          "ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12," \
          "f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207," \
          "f208,f209,f222&_=1607384110349".format(page)
    print('url:',url)
    header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url,headers=header)
    text=response.text
    print('text:',text)
    result = re.findall('\[.*?\]', text)
    print('result:',result)
    if len(result) == 0:
        return 0
    else:
        Data_json = json.loads(result[0])
    for data in Data_json:
        print('data:',data)
        bankuai_id=date_str+data['f12']
        print(bankuai_id)
        bk_dict[bankuai_id]=data['f3']
        try:
            sql="insert into bankuai_day_data(bankuai_id,bankuai_name,bankuai_code,trade_date,open_price,close_price," \
                "high_price,low_price,amount,amount_money,increase,turnover_rate,lingzhang,lingzhang_zhangfu," \
                "shangzhang_jiashu,xiadie_jiashu) " \
                "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}')" \
                "ON DUPLICATE KEY UPDATE bankuai_id='{0}',bankuai_name='{1}',bankuai_code='{2}',trade_date='{3}'," \
                "open_price='{4}',close_price='{5}',high_price='{6}',low_price='{7}'," \
                "amount='{8}',amount_money='{9}',increase='{10}',turnover_rate='{11}',lingzhang='{12}',lingzhang_zhangfu='{13}'," \
                "shangzhang_jiashu='{14}',xiadie_jiashu='{15}'" \
                .format(bankuai_id,data['f14'],data['f12'],date_str,data['f17'],data['f2'],data['f15'],
                        data['f16'],data['f5'],data['f6'],data['f3'],data['f8'],data['f128'],
                        data['f136'],data['f104'],data['f105'])
            cursor.execute(sql)
            db.commit()
            print('存储完成:page:{},id:{},name:{}'.format(page,data['f12'],data['f14']))
            logging.info('存储完成:page:{},id:{},name:{}'.format(page,data['f12'],data['f14']))
            count += 1
            print('count:',count)
        except Exception as err:
            db.rollback()
            logging.error('存储失败:page:{},id:{},name:{},err:{}'.format(page,data['f12'],data['f14'],err))
            print('存储失败:page:{},id:{},name:{},err:{}'.format(page,data['f12'],data['f14'],err))
    cursor.close()
    return 1
def save_sort():
    print('bk_dict2:',bk_dict)
    sort_dict = dict(sorted(bk_dict.items(), key = lambda kv:(kv[1], kv[0]),reverse=True))
    print('sort_dict:',sort_dict)
    rank = 1
    cursor = db.cursor()
    print('sort_dict:',sort_dict)
    for key in sort_dict:
        bk_id = key
        redu  = 1/rank
        rank += 1
        try:
            sql = "insert into bankuai_day_data(bankuai_id,redu) " \
                  "values('{0}','{1}')" \
                  "ON DUPLICATE KEY UPDATE bankuai_id='{0}',redu='{1}'" \
                .format(bk_id,redu)
            cursor.execute(sql)
            db.commit()
            print('存储完成:id:{},redu:{}'.format(bk_id,redu))
            logging.info('存储完成:id:{},redu:{}'.format(bk_id,redu))
        except Exception as err:
            db.rollback()
            logging.error('存储失败:id:{},redu:{},err:{}'.format(bk_id,redu, err))
            print('存储失败:id:{},redu:{},err:{}'.format(bk_id,redu, err))
    cursor.close()
def main(date):
    flag = 1
    page = 1
    if date == None:
        date_str = datetime.datetime.now().strftime('%Y%m%d')
    else:
        date_str = date
    while flag:
        flag = getOnePageStock(str(page),date_str)
        page = int(page) + 1
    save_sort()
if __name__ == '__main__':
    date = None
    # main(date)
    get_history()
    com_his_rank(start_date='2020-12-20', end_date='2020-12-29')
    # print('completed!')


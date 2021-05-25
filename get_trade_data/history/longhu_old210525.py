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
logging.basicConfig(level=logging.DEBUG, filename='../../log/longhu_trade.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

db = pymysql.connect(host="localhost", user="root", password="Zzl08382020", database="stockdb")
# def make_date(date):
#     if date == None:
#         date = datetime.datetime.now().strftime("%Y-%m-%d")
#     return date
def get_longhu(date):
    # url = "http://data.eastmoney.com/stock/tradedetail/2020-12-11.html"
    url = "http://data.eastmoney.com/stock/tradedetail/{}.html".format(date)
    print('url:', url)
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url, headers=header)
    text = response.text
    # print('text:', text)
    data_str_list = re.findall('var data_tab_1={"success":true,"pages":1,"data":(\[.*?\])',text)
    print('data_str_list:',data_str_list)
    if len(data_str_list) !=0:
        data_str = data_str_list[0]
        print('data:',data_str)
        data_list = json.loads(data_str)
        print('data_list:',data_list)
        cursor = db.cursor()
        for stock in data_list:
            trade_date = re.sub('-','',stock['Tdate'])
            shangbang_id =trade_date  + stock['SCode']
            if stock['Ltsz'] == '':
                stock['Ltsz'] =0
            try:
                sql = "insert into longhu_info(shangbang_id,stock_code,stock_name,trade_date,jmmoney,bmoney," \
                      "smoney,lh_trade_money,all_trade_money,reson,jmrate,all_trade_rate,turnover,lt_value,jd) " \
                      "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}'" \
                      ",'{14}')" \
                      "ON DUPLICATE KEY UPDATE shangbang_id='{0}',stock_code='{1}',stock_name='{2}',trade_date='{3}'," \
                      "jmmoney='{4}',bmoney='{5}',smoney='{6}',lh_trade_money='{7}',all_trade_money='{8}',reson='{9}',jmrate='{10}'" \
                      ",all_trade_rate='{11}',turnover='{12}',lt_value='{13}',jd='{14}'" \
                    .format(shangbang_id, stock['SCode'], stock['SName'], stock['Tdate'], stock['JmMoney'], stock['Bmoney'], stock['Smoney'],
                            stock['ZeMoney'], stock['Turnover'], stock['Ctypedes'], stock['JmRate'],stock['ZeRate'],stock['Dchratio'],
                            stock['Ltsz'],stock['JD'])
                cursor.execute(sql)
                db.commit()
                print('存储完成:id:{},name:{},trade_date:{}'.format(stock['SCode'], stock['SName'], stock['Tdate']))
                logging.info('存储完成:id:{},name:{},trade_date:{}'.format(stock['SCode'], stock['SName'], stock['Tdate']))
            except Exception as err:
                db.rollback()
                logging.error('存储失败:id:{},name:{},trade_date:{},err:{}'.format(stock['SCode'], stock['SName'], stock['Tdate'], err))
                print('存储失败:id:{},name:{},trade_date:{},err:{}'.format(stock['SCode'], stock['SName'], stock['Tdate'], err))
        cursor.close()
    else:
        print('{} no content!'.format(date))
def get_history(start,end):
    datestart = datetime.datetime.strptime(start, '%Y-%m-%d')
    dateend = datetime.datetime.strptime(end, '%Y-%m-%d')
    date_list = []
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        date_str = datestart.strftime('%Y-%m-%d')
        print('date_str',date_str)
        date_list.append(date_str)
    for date in date_list:
        get_longhu(date)
if __name__ == "__main__":
    date = None #'2020-12-11'
    get_longhu(date)
    # get_history('2020-12-29', '2021-04-12')
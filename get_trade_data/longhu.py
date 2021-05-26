'''
数据来源：东方财富网-行情中心
http://quote.eastmoney.com/center
'''
# coding=utf-8
import requests
import re
import pymysql
# import pandas as pd
import logging
import json
import datetime

logging.basicConfig(level=logging.DEBUG, filename='../log/longhu_trade.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

db = pymysql.connect(host="192.168.1.6", user="user1", password="Zzl08382020", database="stockdb")


def make_date(date):
    if date == None:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
    return date
def get_longhu(date):
    '''
    "000592|平潭发展|3.71|-6.3131|17.887|-49077912.18|1271330513|342340897|日跌幅偏离值达到7%的前5只证券|4024193|123658876.18|74580964|198239840.18|2021-05-25|-3.86|15.59||||||||||||||||||32.49999999|55.23012554|25.76271182|64.88888889|||||||1家机构卖出，成功率40.67%"
    {0: '000592', 1: '平潭发展', 2: '3.71', 3: '-6.3131', 4: '17.887', 5: '-49077912.18', 6: '1271330513', 7: '342340897',
     8: '日跌幅偏离值达到7%的前5只证券', 9: '4024193', 10: '123658876.18', 11: '74580964', 12: '198239840.18', 13: '2021-05-25',
     14: '-3.86', 15: '15.59', 33: '32.49999999', 34: '55.23012554', 35: '25.76271182', 36: '64.88888889', 37: '',
     38: '', 39: '', 40: '', 41: '', 42: '', 43: '1家机构卖出，成功率40.67%'}
    {0: 'id', 1: 'name', 2: 'close_price', 3: 'increase', 4: 'turnover', 5: 'jmmoney', 6: 'all_trade_money', 7: '342340897',
     8: 'reson', 9: '4024193', 10: 'smoney', 11: 'bmoney', 12: '198239840.18', 13: 'trade_date',
     14: 'jmrate', 15: 'all_trade_rate', 33: '32.49999999', 34: '55.23012554', 35: '25.76271182', 36: '64.88888889', 37: '',
     38: '', 39: '', 40: '', 41: '', 42: '', 43: '解读'}
     url = "http://datainterface3.eastmoney.com/EM_DataCenter_V3/api/LHBGGDRTJ/GetLHBGGDRTJ?js=jQuery11230166114247705907_1621862142891&sortColumn=&sortRule=1&pageSize=200&pageNum=1&tkn=eastmoney&dateNum=&cfg=lhbggdrtj&mkt=0&startDateTime=2021-05-25&endDateTime=2021-05-25"
     '''
    date = make_date(date)
    url = "http://datainterface3.eastmoney.com/EM_DataCenter_V3/api/LHBGGDRTJ/GetLHBGGDRTJ?" \
          "js=jQuery11230166114247705907_1621862142891&sortColumn=&sortRule=1&pageSize=200&pageNum=1" \
          "&tkn=eastmoney&dateNum=&cfg=lhbggdrtj&mkt=0&startDateTime={0}&endDateTime={0}".format(date)
    print('url:', url)
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url, headers=header)
    text = response.text
    print('text:', text)
    data_long_str = re.findall('\((.*?)\)', text)[0]
    print('data_long_str:',data_long_str)
    data_json = json.loads(data_long_str)
    print("data_json['Data']:",len(data_json['Data']))
    data = data_json['Data'][0]['Data']
    data_list_save = []
    for single_str in data:
        deal_data(single_str,data_list_save)
def deal_data(single_str,data_list_save):
    info_list = single_str.split('|')
    #(trade_code,stock_id,stock_name,trade_date,jmmoney,bmoney,smoney,lh_trade_money,all_trade_money,reson,jmrate,all_trade_rate,turnover,lt_value,jd)
    date_str = re.sub('-','',info_list[13])
    trade_code = date_str + info_list[0]
    stock_id = info_list[0]
    stock_name = info_list[1]
    trade_date = info_list[13]
    jmmoney = info_list[5]
    bmoney = info_list[11]
    smoney = info_list[10]
    lh_trade_money = float(info_list[6]) * float(info_list[15])/100 #info_list[0]
    all_trade_money = info_list[6]
    reson = info_list[8]
    jmrate = info_list[14]
    all_trade_rate = info_list[15]
    turnover = info_list[4]
    lt_value = 0#info_list[0]
    jd = info_list[43]

    print('{} {}'.format(stock_name,reson))
    logging.info('{} {}'.format(stock_name,reson))
    cursor = db.cursor()
    try:
        sql = "insert into longhu_info(trade_code,stock_id,stock_name,trade_date,jmmoney,bmoney," \
              "smoney,lh_trade_money,all_trade_money,reson,jmrate,all_trade_rate,turnover,lt_value,jd) " \
              "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}'" \
              ",'{14}')" \
              "ON DUPLICATE KEY UPDATE trade_code='{0}',stock_id='{1}',stock_name='{2}',trade_date='{3}'," \
              "jmmoney='{4}',bmoney='{5}',smoney='{6}',lh_trade_money='{7}',all_trade_money='{8}',reson='{9}',jmrate='{10}'" \
              ",all_trade_rate='{11}',turnover='{12}',lt_value='{13}',jd='{14}'" \
            .format(trade_code,stock_id,stock_name,trade_date,jmmoney,bmoney,smoney,lh_trade_money,
                    all_trade_money,reson,jmrate,all_trade_rate,turnover,lt_value,jd)
        cursor.execute(sql)
        db.commit()
        print('存储完成:id:{},name:{},trade_date:{}'.format(stock_id, stock_name, trade_date))
        logging.info('存储完成:id:{},name:{},trade_date:{}'.format(stock_id, stock_name, trade_date))
    except Exception as err:
        db.rollback()
        logging.error(
            '存储失败:id:{},name:{},trade_date:{},err:{}'.format(stock_id, stock_name, trade_date,
                                                             err))
        print('存储失败:id:{},name:{},trade_date:{},err:{}'.format(stock_id, stock_name, trade_date,
                                                               err))
    cursor.close()



def get_history(start, end):
    datestart = datetime.datetime.strptime(start, '%Y-%m-%d')
    dateend = datetime.datetime.strptime(end, '%Y-%m-%d')
    date_list = []
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        date_str = datestart.strftime('%Y-%m-%d')
        print('date_str', date_str)
        date_list.append(date_str)
    for date in date_list:
        get_longhu(date)


if __name__ == "__main__":
    date = None  # '2020-12-11'
    get_longhu(date)
    # get_history('2020-12-29', '2021-04-12')

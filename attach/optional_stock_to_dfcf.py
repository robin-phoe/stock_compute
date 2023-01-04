#东方财富app 自动管理自选股
#https://myfavor.eastmoney.com/v4/webouter/asz?appkey=d41d8cd98f00b204e9800998ecf8427e&cb=jQuery35105696906718847401_1672760492665&sc=0%24002528&_=1672760492691


# coding: utf-8
#
import uiautomator2 as u2
from time import sleep as sleep
import pymysql
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"strategy"))
import pub_uti_a

d = u2.connect()
# d = u2.connect('192.168.1.88')
# d=u2.connect_usb()
def init():
    d(resourceId="com.miui.home:id/icon_icon", description="东方财富").click()
    sleep(5)
def fill_stock(stock_dict):
    # stock_dict = {'庄线':["002218",'603456'],'小波形':['605299'],'回撤':[],'单涨停':[],'波形':[]} //test
    d.widget.click("00011#自选")
    try:
        d.widget.click("00022#暂无股票  点击添加")
    except:
        d.widget.click("00012#添加股票")
    count = 0
    for key,v in stock_dict.items():
        for stock_id in stock_dict[key]:
            d.send_keys(stock_id, clear=True)
            sleep(1)
            d(resourceId="com.eastmoney.android.berlin:id/f_add").click()
            d.widget.click("00018#热门小波形")
            d.widget.click("00020#确定")
            print('count:',count)
            count +=1
def sel_data_from_db(date):
    if date == None:
        sql = "select DATE_FORMAT(max(trade_date),'%Y-%m-%d') as last_date from monitor "
        date = pub_uti_a.select_from_db(sql=sql)[0][0]
    print('date:',date)
    sql = "select stock_id,monitor_type from monitor where trade_date = '{}'".format(date)
    type_dic = {'zhuang':'庄线','remen_xiaoboxin':'小波形','remen_boxin':'波形','single_limit_retra':'单涨停','remen_retra':'回撤'}
    stock_dict = {}
    for type in type_dic:
        stock_dict[type_dic[type]] = []
    df = pub_uti_a.creat_df(sql= sql)
    df.apply(lambda raw:stock_dict[type_dic[raw['monitor_type']]].append(raw['stock_id']),axis = 1)
    print(len(stock_dict),stock_dict)
    for key in stock_dict:
        print(key,len(stock_dict[key]))


    return stock_dict
def main(date):
    stock_dict = sel_data_from_db(date)
    init()
    fill_stock(stock_dict)
    print('completed.')
if __name__ == '__main__':
    date =None
    # init()
    # fill_stock()
    main(date)

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
import json

logging.basicConfig(level=logging.DEBUG,filename='stock_history_trade.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')



def select_info(cursor,db):
    sql="select stock_id from stock_informations "
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    #print(stock_id_list)
    return stock_id_list
def get_data(stock_id,cursor,db):
    if stock_id[0]=='6':
        url = "http://f10.eastmoney.com/CompanySurvey/CompanySurveyAjax?code=SH{}".format(stock_id)
    elif stock_id[0]=='0':
        url = "http://f10.eastmoney.com/CompanySurvey/CompanySurveyAjax?code=SZ{}".format(stock_id)
    else:
        return 0
    header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url,headers=header)
    text=response.text
    #print('text:',text)
    cym=re.findall('"cym":"(.*?)"',text)[0]
    dchy=re.findall('"sshy":"(.*?)"',text)[0]
    zjhy=re.findall('"sszjhhy":"(.*?)"',text)[0]
    gyrs=re.findall('"gyrs":"(.*?)"',text)[0]
##    gsjj=re.findall('"gsjj":"(.*?)"',text)[0]
##    #gsjj = "。公司成立以来,在徐明波董事长的带领下,始终秉承\"以质量求生存,以创新求发展\"的企业经营理念"
##    print('gsjj1:',gsjj)
##    gsjj = re.sub('\\"','',gsjj)
    gsjj = ""
    #print('gsjj2:',gsjj)
    jyfw=re.findall('"jyfw":"(.*?)"',text)[0]
    jyfw = ''
    ssrq=re.findall('"ssrq":"(.*?)"',text)[0]
    fxl=re.findall('"fxl":"(.*?)"',text)[0]
    if fxl[-1]=='万':
        fxl = float(fxl[0:-1])*10000
    elif fxl[-1]=='亿':
        fxl = float(fxl[0:-1])*100000000
    else:
        fxl = float(fxl)
    qy=re.findall('"qy":"(.*?)"',text)[0]
    mgfxj=re.findall('"mgfxj":"(.*?)"',text)[0]
    #print('cym:',cym,dchy,zjhy,gyrs,gsjj,jyfw,ssrq,'fxl:',fxl,qy)
    try:
        sql="update stock_informations set 发行量={0},所属东财行业='{1}', 证监会行业='{2}', 上市日期='{3}', 曾用名='{4}', 每股发行价='{5}', 区域='{6}', \
            雇员人数='{7}', 经营范围='{8}', 公司简介='{9}' where stock_id = '{10}'\
            ".format(fxl,dchy,zjhy,ssrq,cym,mgfxj,qy,gyrs,jyfw,gsjj,stock_id)
        print('sql',sql)
        cursor.execute(sql)
        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{}'.format(stock_id))
    except Exception as err:
        db.rollback()
        print('存储失败:',err)
        logging.error('存储失败:id:{},{}'.format(stock_id,err))


def main():
    db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
    cursor = db.cursor()
    #get_data(stock_id='603828')#000790
    #get_data(stock_id='000790')
    stock_id_list=select_info(cursor,db)
    #stock_id_list = [('002038',)]
    stock_id_list = [('000518',)]
    for stock in stock_id_list:
        print('stock[0]:',stock[0])
        get_data(stock[0],cursor,db)




if __name__ == '__main__':
    main()



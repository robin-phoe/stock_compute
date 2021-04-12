'''
数据来源：东方财富网-行情中心
http://quote.eastmoney.com/center
'''
#coding=utf-8
import requests
import re
import pymysql
import pandas as pd
import logging
#import threading
import json

logging.basicConfig(level=logging.DEBUG,filename='stock_history_trade.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')


def get_df_from_db(sql, db):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    cursor.close()
    return df
# def select_info(db):
#     cursor = db.cursor()
#     sql="select stock_id from stock_informations "
#     cursor.execute(sql)
#     stock_id_list = cursor.fetchall()
#     #print(stock_id_list)
#     cursor.close()
#     return stock_id_list
def git_base_info(db):
    for num in range(0,1000):
        num_str = '{:0>3d}'.format(num)
        stock_id =  '600' + num_str
        print('stock_id:',stock_id)
        get_data(stock_id, db)
        stock_id =  '002' + num_str
        get_data(stock_id, db)
def get_data(stock_id,db):
    cursor = db.cursor()
    if stock_id[0]=='6':
        url = "http://f10.eastmoney.com/CompanySurvey/CompanySurveyAjax?code=SH{}".format(stock_id)
    elif stock_id[0]=='0' :
        url = "http://f10.eastmoney.com/CompanySurvey/CompanySurveyAjax?code=SZ{}".format(stock_id)
    else:
        return 0
    header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url,headers=header)
    text=response.text
    print('text:',text)
    if text.find('股票代码不合法') != -1:
        print('flag')
        return 0
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
    if ssrq == '--':
        ssrq = '1971-01-01'
    #name
    agjc = re.findall('"agjc":"(.*?)"', text)[0]
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
        sql = "insert into stock_informations(stock_id,stock_name,发行量,bk_name,证监会行业," \
              "上市日期,曾用名,每股发行价,区域,雇员人数,经营范围,公司简介) " \
              "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}')" \
              "ON DUPLICATE KEY UPDATE stock_id='{0}',stock_name='{1}',发行量='{2}',bk_name='{3}'," \
              "证监会行业='{4}',上市日期='{5}',曾用名='{6}',每股发行价='{7}',区域='{8}',雇员人数='{9}',经营范围='{10}'" \
              ",公司简介='{11}'" \
            .format(stock_id,agjc,fxl,dchy,zjhy,ssrq,cym,mgfxj,qy,gyrs,jyfw,gsjj)
        # sql="update stock_informations set 发行量={0},bk_name='{1}', 证监会行业='{2}', 上市日期='{3}', 曾用名='{4}', 每股发行价='{5}', 区域='{6}', \
        #     雇员人数='{7}', 经营范围='{8}', 公司简介='{9}' where stock_id = '{10}'\
        #     ".format(fxl,dchy,zjhy,ssrq,cym,mgfxj,qy,gyrs,jyfw,gsjj,stock_id)
        print('sql',sql)
        cursor.execute(sql)
        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{}'.format(stock_id))
    except Exception as err:
        db.rollback()
        print('存储失败:',err)
        logging.error('存储失败:id:{},{}'.format(stock_id,err))
    cursor.close()
def deal_info(db):
    sql = "select * from stock_informations"
    df = get_df_from_db(sql, db)
    df = df.reset_index()
    df['h_table'] = df['index'].apply(lambda x:x%10)
    cursor = db.cursor()
    for i in range(len(df)):
        sql = ""
    cursor.close()
def update_other_tab(db):
    #历史记录分表
    for i in range(0,10):
        h_tab_list = 'stock_history_trade'+str(i)
    table_list = ['']
    # table_list.extend(h_tab_list)
    sql = "select * from stock_informations"
    df = get_df_from_db(sql, db)
    cursor = db.cursor()
    for i in range(len(df)):
        stock_name = df.loc[i,'stock_name']
        bk_name = df.loc[i,'stock_name']
        stock_id = df.loc[i, 'stock_id']
        h_table = df.loc[i, 'h_table']
        for tab in table_list:
            sql = "update {0} set stock_name='{1}',bk_name='{2}' where stock_id = '{3}'".format(tab,)

    cursor.close()

def main():
    db = pymysql.connect(host="localhost", user="root", password="Zzl08382020", database="stockdb")
    # cursor = db.cursor()
    #get_data(stock_id='603828')#000790
    #get_data(stock_id='000790')
    # stock_id_list=select_info(db)
    #stock_id_list = [('002038',)]

    # for stock in stock_id_list:
    #     print('stock[0]:',stock[0])
    #     get_data(stock[0],db)
    git_base_info(db)



if __name__ == '__main__':
    main()



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
import datetime
import pub_uti_a

logging.basicConfig(level=logging.DEBUG, filename='../log/get_information.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')


# def get_df_from_db(sql, db):
#     cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
#     cursor.execute(sql)  # 执行SQL语句
#     data = cursor.fetchall()
#     # 下面为将获取的数据转化为dataframe格式
#     columnDes = cursor.description  # 获取连接对象的描述信息
#     columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
#     df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
#     cursor.close()
#     return df

'''
【功能】查詢板塊名與板塊編號映射
'''
def get_bk_relation():
    bk_map = {}
    sql = "select distinct bk_name,bk_code from bankuai_day_data"
    res = pub_uti_a.select_from_db(sql)
    for tup in res:
        bk_map[tup[0]] = tup[1]
    return bk_map
def clear_info():
    sql = "delete  from stock_informations"
    pub_uti_a.commit_to_db(sql)
    print('清除成功。')
def get_base_info():
    #清除原数据
    clear_info()
    bk_map  = get_bk_relation()
    s = pub_uti_a.save()
    for num in range(0,1000):
        num_str = '{:0>3d}'.format(num)
        for capital_num in ['600','601','603','688','002','000','300']:
            stock_id = capital_num + num_str
            sql = get_data(stock_id, bk_map)
            if sql:
                s.add_sql(sql)
    s.commit()
def get_data(stock_id,bk_map):
    if stock_id[0]=='6':
        url = "http://f10.eastmoney.com/CompanySurvey/CompanySurveyAjax?code=SH{}".format(stock_id)
    elif stock_id[0]=='0' or stock_id[0]=='3':
        url = "http://f10.eastmoney.com/CompanySurvey/CompanySurveyAjax?code=SZ{}".format(stock_id)
    else:
        return None
    header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url,headers=header)
    text=response.text
    print('text:',text)
    if text.find('股票代码不合法') != -1:
        print('flag')
        return None
    #print('text:',text)
    cym=re.findall('"cym":"(.*?)"',text)[0]
    dchy=re.findall('"sshy":"(.*?)"',text)[0]
    zjhy=re.findall('"sszjhhy":"(.*?)"',text)[0]
    gyrs=re.findall('"gyrs":"(.*?)"',text)[0]
    gsjj = "" #暫時不需要
    #print('gsjj2:',gsjj)
    jyfw=re.findall('"jyfw":"(.*?)"',text)[0]
    jyfw = '' #暫時不需要 需要時需要清洗 \ 符號
    ssrq=re.findall('"ssrq":"(.*?)"',text)[0]
    if ssrq == '--':
        ssrq = '1971-01-01'
    #name
    agjc = re.findall('"agjc":"(.*?)"', text)[0]
    if agjc == '--':
        return None
    fxl=re.findall('"fxl":"(.*?)"',text)[0]
    if  fxl == '--':
        fxl ='0'
    if fxl[-1]=='万':
        fxl = float(fxl[0:-1])*10000
    elif fxl[-1]=='亿':
        fxl = float(fxl[0:-1])*100000000
    else:
        fxl = float(fxl)
    qy=re.findall('"qy":"(.*?)"',text)[0]
    mgfxj=re.findall('"mgfxj":"(.*?)"',text)[0]
    h_table = stock_id[-1]
    #print('cym:',cym,dchy,zjhy,gyrs,gsjj,jyfw,ssrq,'fxl:',fxl,qy)
    bk_code = ''
    if dchy != '--':
        bk_code = bk_map[dchy]
    update_time = datetime.datetime.now().strftime('%Y-%m-%d')
    sql = "insert into stock_informations(stock_id,stock_name,发行量,bk_name,证监会行业," \
          "上市日期,曾用名,每股发行价,区域,雇员人数,经营范围,公司简介,h_table,bk_code,updatetime) " \
          "values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}') " \
          "ON DUPLICATE KEY UPDATE stock_id='{0}',stock_name='{1}',发行量='{2}',bk_name='{3}'," \
          "证监会行业='{4}',上市日期='{5}',曾用名='{6}',每股发行价='{7}',区域='{8}',雇员人数='{9}',经营范围='{10}'" \
          ",公司简介='{11}',h_table='{12}',bk_code='{13}',updatetime = '{14}'" \
        .format(stock_id,agjc,fxl,dchy,zjhy,ssrq,cym,mgfxj,qy,gyrs,jyfw,gsjj,h_table,bk_code,update_time)
    # sql="update stock_informations set 发行量={0},bk_name='{1}', 证监会行业='{2}', 上市日期='{3}', 曾用名='{4}', 每股发行价='{5}', 区域='{6}', \
    #     雇员人数='{7}', 经营范围='{8}', 公司简介='{9}' where stock_id = '{10}'\
    #     ".format(fxl,dchy,zjhy,ssrq,cym,mgfxj,qy,gyrs,jyfw,gsjj,stock_id)
    print('sql',sql)
    return sql



def update_other_tab():
    table_list = ['stock_trade_data', ]
    sql = "select stock_name,bk_name,stock_id from stock_informations"
    result = pub_uti_a.select_from_db(sql)
    print('查询完成。',result)
    start_time = datetime.datetime.now()
    s= pub_uti_a.save()
    for table in table_list:
        for tup in result:
            sql = "update {0} set stock_name='{1}',bk_name='{2}' where stock_id = '{3}'".format(table,tup[0],tup[1],tup[2])
            print('sql:', sql)
            s.add_sql(sql)
    s.commit()
    print('耗时：{}'.format(datetime.datetime.now() - start_time))
# def update_other_tab(db):
#     table_list = ['stock_trade_data',] #stock_trade_data, monitor
#     sql = "select stock_name,bk_name,stock_id from stock_informations"
#     cursor = db.cursor()
#     cursor.execute(sql)
#     result = cursor.fetchall()
#     print('查询完成。')
#     start_time = datetime.datetime.now()
#     for table in table_list:
#         try:
#             sql = "update {0} set stock_name=(%s),bk_name=(%s) where stock_id = (%s)".format(table)
#             print('sql:',sql)
#             cursor.executemany(sql,result)
#             db.commit()
#             print('储存完成。table:{}'.format(table))
#         except Exception as err:
#             db.rollback()
#             print('存储失败!table:{},{}'.format(table, err))
#             logging.error('存储失败!table:{},{}'.format(table, err))
#     print('耗时：{}'.format(datetime.datetime.now() - start_time))
#     # df = get_df_from_db(sql, db)
#     # cursor = db.cursor()
#     # for i in range(len(df)):
#     #     stock_name = df.loc[i,'stock_name']
#     #     bk_name = df.loc[i,'stock_name']
#     #     stock_id = df.loc[i, 'stock_id']
#     #     print('stock_id:{}'.format(stock_id))
#     #     for tab in table_list:
#     #         sql = "update {0} set stock_name='{1}',bk_name='{2}' where stock_id = '{3}'".format(tab, stock_name,
#     #                                                                                             bk_name,
#     #                                                                                             stock_id)
#     #         cursor.execute(sql)
#     # try:
#     #
#     #     db.commit()
#     #     print('存储完成')
#     # except Exception as err:
#     #     db.rollback()
#     #     print('存储失败:id:{},{}'.format(stock_id, err))
#     #     logging.error('存储失败:id:{},{}'.format(stock_id, err))
#     cursor.close()

def main(update_flag = 0):
    if update_flag ==1:
        get_base_info()
        update_other_tab()
    elif update_flag == 0:
        get_base_info()
    elif update_flag == 2:
        update_other_tab()


if __name__ == '__main__':
    main(update_flag = 2)

    #test
    # stock_id ='600824'
    # bk_map = get_bk_relation()
    # get_data(stock_id, bk_map)



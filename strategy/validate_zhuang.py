# 1、计算结果中60日内30%、50%，涨幅占比。
# 2、统计大拉升与庄线计算结果的交集占比
#结果输出为文本报告
#查询庄线分数满足的股票
import logging
import pymysql
import pandas as pd
import datetime
import re
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config

logging.basicConfig(level=logging.DEBUG, filename='../log/validate_zhuang.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

db_config = read_config('db_config')
db = pymysql.connect(host=db_config["host"], user=db_config["user"],
                     password=db_config["password"], database=db_config["database"])
def get_df_from_db(sql, db):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    # df = df.set_index(keys=['trade_date'])
    # df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
    # df.reset_index(inplace=True)
    cursor.close()
    return df
def sel_zhuang_stcok(zhuang_table):
    sql = "select * from {} where zhuang_grade >=10000".format(zhuang_table)
    zhuang_df = get_df_from_db(sql, db)
    def get_year(content):
        result = re.findall("\'(.*?)\'",content)
        if len(result) != 0:
            year = result[-1][0:4]
            return year
        else:
            logging.error('此庄线没有庄线区间')
            print('此庄线没有庄线区间')
    zhuang_df['year'] = zhuang_df['zhuang_section'].apply(get_year)
    zhuang_df['year_code'] = zhuang_df['year'] + zhuang_df['stock_id']
    return zhuang_df
#查询拉升结果股票：
def sel_lasheng_stcok():
    sql = "select * from tongji_dalasheng"
    lasheng_df = get_df_from_db(sql, db)
    lasheng_df['year'] = lasheng_df['trade_date'].astype(str).apply(lambda x:x[0:4])
    lasheng_df['year_code'] = lasheng_df['year'] + lasheng_df['stock_id']
    return lasheng_df
#计算庄股结果与拉升结果交集股票
def comp_intersection(zhuang_df,lasheng_df):
    len_zhuang = len(zhuang_df)
    len_lasheng = len(lasheng_df)
    inter_df = pd.merge(zhuang_df,lasheng_df,how='inner',on=['year_code'])
    len_inter_df = len(inter_df)
    inter_zhuang_rate = len_inter_df / len_zhuang
    inter_lasheng_rate = len_inter_df / len_lasheng
    inter_report = "庄线个股数:{0}，拉升统计个股数：{1},交集个股数：{2}。\n交集占庄线比例：{3}，交集占拉升比例：{4}".format(
        len_zhuang,len_lasheng,len_inter_df,inter_zhuang_rate,inter_lasheng_rate
    )
    inter_report = "=====================================\n" + inter_report
    print(inter_report)
    return inter_report

#计算庄线结果中60日内30%、50%，涨幅占比。
def creat_increase_report(df):
    count_stock = len(df)
    df_1_layer = df.drop(df[13000000 <= df.zhuang_grade].index)
    increase_30 = count_stock - len(df_1_layer)
    increase_30_rate = increase_30 / count_stock
    df_2_layer = df.drop(df[df.zhuang_grade <= 15000000].index)
    increase_50 = len(df_2_layer)
    increase_50_rate = increase_50 / count_stock
    increase_report = "庄线个股数(60日前):{0}。\n涨幅大于30%个股数:{1}。比值为:{2}。\n涨幅大于等于50%个股数:{3}。比值为:{4}。".format(
        count_stock,increase_30,increase_30_rate,increase_50,increase_50_rate)
    print(increase_report)
    return increase_report
def main(zhuang_table):
    zhuang_df = sel_zhuang_stcok(zhuang_table)
    lasheng_df = sel_lasheng_stcok()
    increase_report = creat_increase_report(zhuang_df)
    comp_intersection(zhuang_df, lasheng_df)
    inter_report = report_name = './validate_report/validate_zhuang_report_{}.txt'.format(zhuang_table)
    with open(report_name,'w',encoding='utf-8') as f:
        f.write(increase_report + inter_report)
    print('completed.')
if __name__ == '__main__':
    zhuang_table = 'com_zhuang'#'com_zhuang0427'
    main(zhuang_table)
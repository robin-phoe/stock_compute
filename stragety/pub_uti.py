
import pandas as pd
import pymysql
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"config"))
from readconfig import read_config
import logging
"""
【功能】创建db
"""
class con_db:
    def __init__(self):
        pass
    def creat_db(self):
        db_config = read_config('db_config')
        db = pymysql.connect(host=db_config["host"], user=db_config["user"], password=db_config["password"],
                             database=db_config["database"])
        return db
"""
【功能】创建df
"""
class creat_df_from_db:
    def __init__(self):
        cd = con_db()
        self.db = cd.creat_db()
    def creat_df(self,sql):
        cursor = self.db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
        cursor.execute(sql)  # 执行SQL语句
        data = cursor.fetchall()
        # 下面为将获取的数据转化为dataframe格式
        columnDes = cursor.description  # 获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
        df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
        if 'trade_date' in df.columns:
            df = df.sort_values(axis=0, ascending=False, by='trade_date', na_position='last')
            df.reset_index(inplace=True)
            df['trade_date'] = df['trade_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
        cursor.close()
        # print('df:',df)
        return df
creat_df_from_db = creat_df_from_db()
creat_df = creat_df_from_db.creat_df

"""
【功能】一般查询功能
"""
class select_db:
    def __init__(self):
        cd = con_db()
        self.db = cd.creat_db()
    def select_from_db(self,sql):
        cursor = self.db.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        return data
s_d = select_db()
select_from_db = s_d.select_from_db
"""
【功能】存储功能
"""
class save:
    def __init__(self):
        cd = con_db()
        self.db = cd.creat_db()
        self.cursor = self.db.cursor()
    def add_sql(self,sql):
        # sql = 'insert into boxin_list(stock_id,boxin_list) ' \
              # 'values(\'{0}\',\'{1}\') ' \
              # 'ON DUPLICATE KEY UPDATE stock_id=\'{0}\',boxin_list=\'{1}\' ' \
              # ''.format(id,boxin_list)
        self.cursor.execute(sql)
    def commit(self):
        try:
            self.db.commit()
            print('存储完成')
            logging.info('存储完成')
        except Exception as err:
            self.db.rollback()
            print('存储失败:', err)
            logging.error('存储失败:{}'.format(err))
        self.cursor.close()
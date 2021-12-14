
import pandas as pd
import pymysql
import sys
import os
import logging
from sqlalchemy import create_engine
import json
import config.config as config

"""
【功能】读取配置
"""
# class read_conf:
#     def __init__(self):
#         self.param = os.path.dirname(os.getcwd())
#     def read_config(self,param=None):
#         if param == None:
#             param = self.param
#         with open('{}\\stock_django\\config\\db_config.json'.format(param),'r') as f:
#             config_param = json.load(f)
#         return config_param
# rc = read_conf()
# read_conf = rc.read_config
"""
【功能】创建db
"""
class con_db:
    def __init__(self):
        pass
    def creat_db(self):
        db_config = config.db_config
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
    def creat_df(self,sql,ascending=False):
        cursor = self.db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
        cursor.execute(sql)  # 执行SQL语句
        data = cursor.fetchall()
        # 下面为将获取的数据转化为dataframe格式
        columnDes = cursor.description  # 获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
        df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
        if 'trade_date' in df.columns:
            #ascending true 升序
            df = df.sort_values(axis=0, ascending=ascending, by='trade_date', na_position='last')
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
【功能】db一般执行功能
"""
class commit_to_db_cla:
    def __init__(self):
        cd = con_db()
        self.db = cd.creat_db()
    def commit_db(self,sql):
        cursor = self.db.cursor()
        try:
            cursor.execute(sql)
            self.db.commit()
            print('执行完成')
            logging.info('执行完成')
        except Exception as err:
            self.db.rollback()
            print('执行失败:', err, sql)
            logging.error('执行失败:{}'.format(err))
        cursor.close()
c_d = commit_to_db_cla()
commit_to_db = c_d.commit_db
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
"""
【功能】df存储到mysql
"""
class df_to_db:
    def __init__(self):
        db_config = config.db_config
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"]
        self.conf = "mysql+pymysql://{user}:{password}@{host}:3306/{database}".format(
            user = user,password =password,host = host,database = database
        )
        self.conf = "mysql+pymysql://user1:Zzl08382020@192.168.1.6:3306/stockdb"
        self.engine = create_engine(self.conf)
    def clean_table(self,table):
        sql = "delete from {}".format(table)
        commit_to_db(sql)
    def df_to_mysql(self,table,df):
        self.clean_table(table)
        print('conf:', self.conf)
        df.to_sql(name=table,con=self.engine,if_exists='append')
dd = df_to_db()
df_to_mysql = dd.df_to_mysql

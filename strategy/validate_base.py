#后一日hight price >=3% 触发监控，计触发数1，open price + 3% 计算为call_price
#触发后一日open price 计为 put_open_price, close price 计为 put_close_price
#pl_layer1: (,0] ;  pl_layer2: (0,2.5) ; pl_layer3:[2.5,6),pl_layer4:[6,)
import logging
import pymysql
import pandas as pd
import datetime
import re
import pub_uti

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)

logging.basicConfig(level=logging.INFO, filename='../log/validate_remen_xiaoboxin.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

class validate_buffer:
    def __init__(self,vali_start,vali_end):
        self.trade_df = None
        self.vali_df = None
        self.result_df = None
        self.stcoK_set = set()
        self.vali_start = vali_start
        self.vali_end = vali_end
        self.vali_range_day = 10
        self.trade_date_start = self.vali_start
        self.trade_date_end = datetime.datetime.strptime(self.vali_end , '%Y-%m-%d') + datetime.timedelta(days = self.vali_range_day+1)
        self.report_file_name = './validate_report/validate_retacement.csv'
        self.vali_table = 'remen_retracement'
        self.grade = 10001
    def com(self):
        self.get_trade_date()
        self.get_vali_stock()
        self.create_reslut_df()
        self.init_stock()
    def get_trade_date(self):
        sql = "select * from stock_trade_data where trade_date >= '{0}' and trade_date <= '{1}'".format(
            self.trade_date_start,self.trade_date_end)
        self.trade_df = pub_uti.creat_df(sql)
        self.trade_df['mod_price'] = (self.trade_df['high_price'] + self.trade_df['low_price'])/2
    def get_vali_stock(self):
        # sql = "select * from {0} where trade_date >='{1}' and trade_date <='{2}' and grade >= '{3}'".format(
        #     self.vali_table,self.vali_start,self.vali_end,self.grade)
        sql = "select * from {0} where trade_date >='{1}' and trade_date <='{2}' and stock_id = '600844' and grade >= '{3}'".format(
            self.vali_table,self.vali_start,self.vali_end,self.grade)
        self.vali_df = pub_uti.creat_df(sql)
        self.stcoK_set = set(self.vali_df['stock_id'].to_list())
    def init_stock(self):
        for stock_id in self.stcoK_set:
            vali_single_df = self.vali_df.loc[self.vali_df.stock_id == stock_id]
            vali_single_df.reset_index(inplace=True)
            trade_single_df = self.trade_df.loc[self.trade_df.stock_id == stock_id]
            trade_single_df.reset_index(inplace=True)
            st_b = stock_buffer(vali_single_df,trade_single_df)
            self.result_df = st_b.commput(self.result_df)
        print('result_df:',self.result_df)
        # self.result_df.to_csv(self.report_file_name,encoding='utf_8_sig')
    def create_reslut_df(self):
        data = {'trade_date': [],'stock_id': [],'stock_name': [],'grade': [],'call_price': [],'low_inc_1': []
            , 'high_inc_1': [],'mod_inc_1': [],'low_inc_2': [],'high_inc_2': [],'mod_inc_2': [],'low_inc_3': [],
                'high_inc_3': [],'mod_inc_3': []
                }
        self.result_df = pd.DataFrame(data)
class stock_buffer:
    def __init__(self,vali_single_df,trade_single_df):
        self.stock_id = vali_single_df.loc[0,'stock_id']
        self.stock_name = vali_single_df.loc[0,'stock_name']
        self.vali_single_df = vali_single_df
        self.vali_date_list = []
        self.trade_single_df = trade_single_df
    def commput(self,result_df):
        for index,raw in self.vali_single_df.iterrows():
            st = stock(raw,self.trade_single_df)
            if not st.compute():
                continue
            raw_list = [st.trade_date,self.stock_id,self.stock_name,st.grade,st.call_price,
                        st.low_inc_1,st.high_inc_1,st.mod_inc_1,
                        st.low_inc_2,st.high_inc_2,st.mod_inc_2,
                        st.low_inc_3,st.high_inc_3,st.mod_inc_3]
            print('raw_list:',raw_list)
            result_df.loc[len(result_df)] = raw_list
        return result_df
class stock:
    def __init__(self,raw,trade_single_df):
        self.trade_single_df = trade_single_df
        self.raw = raw
        self.trade_date = raw['trade_date']
        self.grade = raw['grade']
        self.call_price = None
        self.low_inc_1 = None
        self.high_inc_1 = None
        self.mod_inc_1 = None
        self.low_inc_2 = None
        self.high_inc_2 = None
        self.mod_inc_2 = None
        self.low_inc_3 = None
        self.high_inc_3 = None
        self.mod_inc_3 = None
    def compute(self,):
        print('日期：', self.trade_date)
        if self.raw['grade'] >= 20000:
            if not self.com_close():
                return False
        elif self.raw['grade'] >= 10000:
            if not self.com_in():
                return False
        else:
            return False
        self.low_inc_1 = self.trade_single_df.loc[self.index-1,'low_price']/self.call_price -1
        self.high_inc_1 = self.trade_single_df.loc[self.index-1, 'high_price']/self.call_price -1
        self.mod_inc_1 = self.trade_single_df.loc[self.index-1, 'mod_price']/self.call_price -1
        self.low_inc_2 = self.trade_single_df.loc[self.index-2, 'low_price']/self.call_price -1
        self.high_inc_2 = self.trade_single_df.loc[self.index-2, 'high_price']/self.call_price -1
        self.mod_inc_2 = self.trade_single_df.loc[self.index-2, 'mod_price']/self.call_price -1
        self.low_inc_3 = self.trade_single_df.loc[self.index-3, 'low_price']/self.call_price -1
        self.high_inc_3 = self.trade_single_df.loc[self.index-3, 'high_price']/self.call_price -1
        self.mod_inc_3 = self.trade_single_df.loc[self.index-3, 'mod_price']/self.call_price -1
        return True
    def com_close(self):
        index_list = self.trade_single_df[self.trade_single_df.trade_date == self.trade_date].index.to_list()
        if len(index_list) != 1:
            logging.error('{} 日期未找到或者多个:{},{}'.format(self.raw['stock_id'],self.trade_date,index_list))
            print('{} 日期未找到或者多个:{}'.format(self.raw['stock_id'],self.trade_date))
            return False
        self.index = index_list[0]
        if len(self.trade_single_df) <= self.index + 4:
            logging.error('{} 交易记录长度不够:{}'.format(self.raw['stock_id'],self.trade_date))
            print('{} 交易记录长度不够:{}'.format(self.raw['stock_id'],self.trade_date))
            return False
        self.call_price = self.trade_single_df.loc[self.index, 'close_price']
        return True
    def com_in(self):
        index_list = self.trade_single_df[self.trade_single_df.trade_date == self.trade_date].index.to_list()
        if len(index_list) != 1:
            logging.error('{} 日期未找到或者多个:{},{}'.format(self.raw['stock_id'], self.trade_date,index_list))
            print('{} 日期未找到或者多个:{}'.format(self.raw['stock_id'], self.trade_date))
            return False
        # print('index:',self.raw['stock_id'],self.trade_date,index_list)
        self.index = index_list[0] -1
        if self.index < 4:
            logging.error('{} 索引超过下限:{},{}'.format(self.raw['stock_id'], self.trade_date,index_list))
            print('{} 索引超过下限:{}'.format(self.raw['stock_id'], self.trade_date))
            return False
        print('计算日期：',self.trade_single_df.loc[self.index , 'trade_date'])
        if len(self.trade_single_df) <= self.index + 4:
            logging.error('{} 交易记录长度不够:{}'.format(self.raw['stock_id'], self.trade_date))
            print('{} 交易记录长度不够:{}'.format(self.raw['stock_id'], self.trade_date))
            return False
        self.call_price = self.trade_single_df.loc[self.index + 1, 'close_price'] * 1.025
        if self.trade_single_df.loc[self.index, 'high_price'] < self.call_price:
            print('未达到买入标准：日期：{0} ，call_price:{1} ,high_price:{2}'.format(
                self.trade_single_df.loc[self.index + 1, 'trade_date'],self.call_price,self.trade_single_df.loc[self.index, 'high_price']))
            return False
        return True



# def get_df_from_db(sql, db):
#     cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
#     cursor.execute(sql)  # 执行SQL语句
#     data = cursor.fetchall()
#     # 下面为将获取的数据转化为dataframe格式
#     columnDes = cursor.description  # 获取连接对象的描述信息
#     columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
#     df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
#     # df = df.set_index(keys=['trade_date'])
#     df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
#     df.reset_index(inplace=True)
#     # df['avg_5'] = df['close_price'].rolling(5).mean()
#     cursor.close()
#     return df
# def sel_remen_xiaoboxin(date):
#     sql = "select trade_code,trade_date,stock_id,stock_name from remen_boxin " \
#           "where trade_date = '{}' and grade > 10000 and stock_id not like '300%' and stock_id not like '688%' ".format(date)
#     remen_df = get_df_from_db(sql, db)
#     id_list = remen_df['stock_id'].tolist()
#     id_tuple = tuple(id_list)
#     return remen_df,id_tuple
# def save(df):
#     df['layer'] = df['pl_layer1'] + df['pl_layer2']*2 + df['pl_layer3']*3 + df['pl_layer4']*4
#     df_layer = df[['layer','trade_code']]
#     cursor = db.cursor()
#     layer_list = df_layer.apply(lambda row: tuple(row), axis=1).values.tolist()
#     try:
#         sql = "update remen_boxin SET validate_layer=(%s) where trade_code=(%s)"
#         cursor.executemany(sql, layer_list)  # commit_id_list上面已经说明
#         db.commit()
#         print('存储成功。')
#     except Exception as err:
#         logging.exception('存储失败:',err)
#         db.rollback()
#         print('存储失败:',err)
#     cursor.close()
# def deal_data(df):
#     def com_pl(row):
#         if row['call_high'] ==0 or row['close_price'] ==0:
#             return row
#         if row['call_high']/row['close_price'] >= 1.03:
#             row['call_price'] = row['close_price']*1.03
#             pl = row['put_mid_price']/row['call_price'] -1
#             if pl <= 0:
#                 row['pl_layer1'] = 1
#             elif 0< pl <0.025:
#                 row['pl_layer2'] = 1
#             elif 0.025<= pl <0.06:
#                 row['pl_layer3'] = 1
#             elif 0.06 <= pl:
#                 row['pl_layer4'] = 1
#         return row
#     df = df.apply(com_pl,axis=1)
#     return df
# def sel_trade_data(date,id_tuple):
#     end_date = (datetime.datetime.strptime(date,'%Y-%m-%d')+datetime.timedelta(days=10)).strftime('%Y-%m-%d')
#     if len(id_tuple) == 1:
#         sql = "select trade_code,stock_id,stock_name,open_price,close_price,high_price,low_price,trade_date " \
#               "from stock_trade_data where stock_id = '{0}' and trade_date>='{1}' and trade_date<='{2}'".format(
#             id_tuple[0], date, end_date)
#     else:
#         sql = "select trade_code,stock_id,stock_name,open_price,close_price,high_price,low_price,trade_date " \
#               "from stock_trade_data where stock_id in {0} and trade_date>='{1}' and trade_date<='{2}'".format(
#             id_tuple,date,end_date)
#     print('sql:',sql)
#     print('date:',date)
#     trade_df = get_df_from_db(sql,db)
#     # print('trade_df:',trade_df)
#     trade_df['call_price'] = 0
#     trade_df['pl_layer1']= trade_df['pl_layer2']= trade_df['pl_layer3']= trade_df['pl_layer4'] = 0
#     trade_df['call_high'] = trade_df.groupby(['stock_id'])['high_price'].shift(-1)
#     trade_df['call_open'] = trade_df.groupby(['stock_id'])['open_price'].shift(-1)
#     trade_df['call_close'] = trade_df.groupby(['stock_id'])['close_price'].shift(-1)
#     trade_df['put_open_price'] = trade_df.groupby(['stock_id'])['open_price'].shift(-2)
#     trade_df['put_close_price'] = trade_df.groupby(['stock_id'])['close_price'].shift(-2)
#     trade_df['put_high_price'] = trade_df.groupby(['stock_id'])['high_price'].shift(-2)
#     trade_df['put_low_price'] = trade_df.groupby(['stock_id'])['low_price'].shift(-2)
#     trade_df['put_mid_price'] = (trade_df['put_high_price'] + trade_df['put_low_price'])/2
#     trade_df.fillna(0,inplace=True)
#     #计算数据
#     trade_df = deal_data(trade_df)
#     #删除不是今日的数据行
#     trade_df.drop(trade_df[trade_df.trade_date != date].index, inplace=True)
#     #删除call_price = 0的行
#     trade_df.drop(trade_df[trade_df.call_price == 0].index, inplace=True)
#     trade_df['call_close_pl'] = trade_df['call_close'] / trade_df['call_price'] -1
#     trade_df['put_open_pl'] = trade_df['put_open_price'] / trade_df['call_price'] -1
#     trade_df['put_close_pl'] = trade_df['put_close_price'] / trade_df['call_price'] -1
#     trade_df['put_mid_pl'] = trade_df['put_mid_price'] / trade_df['call_price'] -1
#     print(trade_df[['stock_name','stock_id','close_price','call_price','call_high','call_close','put_open_price','put_open_pl',
#                     'put_close_price','put_close_pl','put_mid_price','put_mid_pl']])
#     call_close_mean = trade_df['call_close_pl'].mean()
#     put_open_mean = trade_df['put_open_pl'].mean()
#     put_close_mean = trade_df['put_close_pl'].mean()
#     put_mid_mean = trade_df['put_mid_pl'].mean()
#     count_df = len(trade_df)
#     pl_layer1_count = trade_df['pl_layer1'].tolist().count(1)
#     pl_layer2_count = trade_df['pl_layer2'].tolist().count(1)
#     pl_layer3_count = trade_df['pl_layer3'].tolist().count(1)
#     pl_layer4_count = trade_df['pl_layer4'].tolist().count(1)
#     save(trade_df)
#     print('call_close_mean:{0},put_open_mean:{1},put_close_mean:{2},put_mid_mean:{3},\n,pl_layer1_count:{4},'
#           'pl_layer2_count:{5},pl_layer3_count:{6},pl_layer4_count:{7},count_df:{8}'.format(
#         call_close_mean,put_open_mean,put_close_mean,put_mid_mean,
#         pl_layer1_count,pl_layer2_count,pl_layer3_count,pl_layer4_count,count_df))
#     return [date,call_close_mean,put_open_mean,put_close_mean,put_mid_mean,
#             count_df,pl_layer1_count,pl_layer2_count,pl_layer3_count,pl_layer4_count]
    

# def main(date):
#     if date == None:
#         date = datetime.datetime.now().strftime('%Y-%m-%d')
#     remen_df,id_tuple = sel_remen_xiaoboxin(date)
#     if len(id_tuple) == 0:
#        return []
#     res_list = sel_trade_data(date, id_tuple)
#     return res_list
# def history(start_date,end_date):
#     data = {'date': [],
#             'call_close_mean': [],
#             'put_open_mean': [],
#             'put_close_mean': [],
#             'put_mid_mean': [],
#             'count_df':[],
#             'pl_layer1_count':[],
#             'pl_layer2_count':[],
#             'pl_layer3_count':[],
#             'pl_layer4_count':[],
#             }
#     res_df = pd.DataFrame(data)
#     sql = "select distinct(trade_date) from remen_boxin where trade_date >= '{}' and trade_date <= '{}'".format(start_date,end_date)
#     cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
#     cursor.execute(sql)  # 执行SQL语句
#     date_tuple = cursor.fetchall()
#     print('date_tuple:',date_tuple)
#     cursor.close()
#     for date in date_tuple:
#         date_str = date[0].strftime("%Y-%m-%d")
#         res_list = main(date_str)
#         if len(res_list) == 0:
#             continue
#         res_df.loc[len(res_df)] = res_list
#     row = len(res_df)
#     res_df.loc[row, 'call_close_mean'] = res_df['call_close_mean'].mean()
#     res_df.loc[row,'put_open_mean'] = res_df['put_open_mean'].mean()
#     res_df.loc[row, 'put_close_mean'] = res_df['put_close_mean'].mean()
#     res_df.loc[row, 'put_mid_mean'] = res_df['put_mid_mean'].mean()
#     res_df.loc[row, 'count_df'] = res_df['count_df'].sum()
#     res_df.loc[row, 'pl_layer1_count'] = res_df['pl_layer1_count'].sum()
#     res_df.loc[row, 'pl_layer2_count'] = res_df['pl_layer2_count'].sum()
#     res_df.loc[row, 'pl_layer3_count'] = res_df['pl_layer3_count'].sum()
#     res_df.loc[row, 'pl_layer4_count'] = res_df['pl_layer4_count'].sum()
#     res_df['pl_layer1_rate'] = res_df['pl_layer1_count'] / res_df['count_df'] * 100
#     res_df['pl_layer2_rate'] = res_df['pl_layer2_count'] / res_df['count_df'] * 100
#     res_df['pl_layer3_rate'] = res_df['pl_layer3_count'] / res_df['count_df'] * 100
#     res_df['pl_layer4_rate'] = res_df['pl_layer4_count'] / res_df['count_df'] * 100
#     file_name = "./validate_report/validate_boxin.csv"
#     res_df.to_csv(file_name,encoding='utf-8')
if __name__ == '__main__':
    date = '2021-04-14'
    # main(date)
    vali = validate_buffer(vali_start = '2021-01-01',vali_end='2021-06-20')
    vali.com()
    print('completed.')
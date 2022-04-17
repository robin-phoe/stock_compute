#获取历史日线数据

import pub_uti_a
import logging
import requests
import re
from multiprocessing import Pool
logging.basicConfig(level=logging.DEBUG, filename='../log/get_history_data.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
import json

class stock:
    def __init__(self,stock_id,stock_name,start_date,end_date):
        self.stock_id = stock_id
        self.stock_name = stock_name
        self.start_date = start_date
        self.end_date = end_date
        self.check_date()
        self.exchange = None
        self.judge_exchange()
        self.get_kline_url = "http://push2his.eastmoney.com/api/qt/stock/kline/get?secid={3}.{0}&" \
                   "fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&" \
                   "beg={1}&end={2}&ut=fa5fd1943c7b386f172d6893dbfba10b"\
            .format(self.stock_id,start_date,end_date,self.exchange)
        self.base_info_url = "http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&" \
                             "fltt=2&invt=2&volt=2&fields=f58,f84,f85&secid={0}.{1}".format(self.exchange,self.stock_id)
        self.header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                     '(KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
        self.get_data_from_url()
    def check_date(self):
        if '-' in self.start_date or '-' in self.end_date:
            self.start_date = re.sub('-','',self.start_date)
            self.end_date = re.sub('-', '', self.end_date)
    def judge_exchange(self):
        if self.stock_id[0] == '6':
            self.exchange = 1 #上证
        elif self.stock_id[0] == '0' or self.stock_id[0] == '3':
            self.exchange = 0  # 深证
        else:
            self.exchange = 9 #未知
    def get_data_from_url(self):
        kline_response = requests.get(self.get_kline_url,headers=self.header).text
        base_info_response = requests.get(self.base_info_url, headers=self.header).text
        try:
            self.kline_list = json.loads(kline_response)['data']['klines']
            self.circulation_value = json.loads(base_info_response)['data']['f85']
        except Exception as err:
            logging.error('get response:{}'.format(err))
            print('ERROR: get response:{}'.format(err))
            return
        self.save_data()
    def save_data(self):
        s = pub_uti_a.save()
        for day_data in self.kline_list:
            day_data_list = day_data.split(',')
            if len(day_data_list) ==0:
                logging.error('when split daily kline has error:{}'.format(day_data_list))
                print('when split daily kline has error:{}'.format(day_data_list))
                return 0
            trade_date = day_data_list[0]
            open_price = day_data_list[1]
            close_price = day_data_list[2]
            high_price = day_data_list[3]
            low_price = day_data_list[4]
            trade_amount = day_data_list[5]
            #验证数据
            if self.circulation_value == '-':
                logging.warning('stock_id:{} circulation_value is "-"'.format(self.stock_id))
                return 0
            if trade_amount == '-':
                logging.warning('stock_id:{} trade_amount is "-"'.format(self.stock_id))
                return 0
            trade_code = re.sub('-','',trade_date) + self.stock_id
            turnover_rate = float(trade_amount) / float(self.circulation_value) * 10000
            increase = 10000 #填充
            P_E = 9999 #填充
            P_B = 9999 #填充
            trade_money = 9999 #填充
            sql="insert into stock_trade_data(trade_code,stock_name,stock_id,trade_date,close_price,increase," \
                        "open_price,turnover_rate,P_E,P_B,high_price,low_price,trade_amount,trade_money) " \
                        "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')" \
                        "ON DUPLICATE KEY UPDATE trade_code='{0}',stock_name='{1}',stock_id='{2}',trade_date='{3}'," \
                        "close_price='{4}',increase='{5}',open_price='{6}',turnover_rate='{7}'," \
                        "P_E='{8}',P_B='{9}',high_price='{10}',low_price='{11}',trade_amount='{12}',trade_money='{13}'" \
                        .format(trade_code,self.stock_name,self.stock_id,trade_date,close_price,increase,open_price,
                                turnover_rate,P_E,P_B,high_price,low_price,trade_amount,trade_money)
            s.add_sql(sql)
        s.commit()
class stock_buff:
    def __init__(self,start_date,end_date):
        self.info_tuple = ()
        self.start_date = start_date
        self.end_date = end_date
        self.get_stock_list()
    def get_stock_list(self):
        sql = "select distinct stock_id,stock_name from stock_trade_data "
        self.info_tuple = pub_uti_a.select_from_db(sql)
    def deal_page(self,num):
        for tup in self.info_tuple:
            stock_name = tup[1]
            stock_id = tup[0]
            if tup[0][5] == str(num):
                print(tup)
                stock(stock_id,stock_name,self.start_date,self.end_date)
    def deal_all(self):
        count = 0
        for tup in self.info_tuple:
            print(count,tup)
            stock_name = tup[1]
            stock_id = tup[0]
            stock(stock_id, stock_name, self.start_date, self.end_date)
            count +=1
# def run(self):
#     p = Pool(8)
#     for i in range(0, 10):
#         p.apply_async(deal_page, args=(i, self.start_date, self.end_date,))
#     print('Waiting for all subprocesses done...')
#     p.close()
#     p.join()
#     print('All subprocesses done.')
if __name__ == '__main__':
    #test
    # stock('603568','伟明环保','20220320','20220402')
    # stock_buff('20220320','20220322').deal_page(1)
    stock_buff('20220218', '20220415').deal_all()
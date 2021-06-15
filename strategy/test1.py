import pub_uti
import pandas as pd
import json
import re
import datetime


sql = 'select trade_code,wave_data1 from stock_trade_data '
df = pub_uti.creat_df(sql)
print(df)
s = pub_uti.save()
def sql_fun(raw):
    # print('1:',raw['wave_data1'])
    if raw['wave_data1'] != None and raw['wave_data1'] != '-':
        # print(raw['wave_data1'])
        sql = "update stock_trade_data set wave_data = '{}' where trade_code = '{}' ".format(float(raw['wave_data1']),raw['trade_code'])
        s.add_sql(sql)
df.apply(sql_fun , axis = 1)
s.commit()



# df = pd.DataFrame(lis)
# print(df)
# df.to_csv('data.csv')

if __name__ == '__main__':
    pass

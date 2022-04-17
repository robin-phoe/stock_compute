#计算清理数据表中基础数据
import pub_uti_a
import datetime
class compute_base_data:
    def __init__(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date
    def run_all(self):
        pass
    def comput_increase(self):
        sql = "select trade_code,trade_date,stock_id,stock_name,close_price from stock_trade_data " \
              "where trade_date >= '{}' and trade_date <= '{}'".format(self.start_date,self.end_date)
        df = pub_uti_a.creat_df(sql,ascending=True)
        id_set = set(df['stock_id'].to_list())
        s = pub_uti_a.save()
        count = 0
        for id in id_set:
            print('id:',id,count)
            single_df = df[df.stock_id == id]
            single_df['pre_close'] = single_df['close_price'].shift(1)
            single_df.dropna(inplace=True)
            single_df.reset_index(inplace=True,drop=True)
            print('singel index:',single_df.index)
            single_df['increase'] = (single_df['close_price']/single_df['pre_close']-1) * 100
            for idx,raw in single_df.iterrows():
            # len_df =len(single_df)
            # for i in range(len_df):
            #     raw = single_df.loc[i]
                sql = "update stock_trade_data set increase={} where trade_code = '{}'".format(raw['increase'],raw['trade_code'])
                print(sql)
                s.add_sql(sql)
            count += 1
        s.commit()


if __name__ == '__main__':
    compute_base_data('2022-02-16','2022-04-07').comput_increase()


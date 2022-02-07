"""
庄线相关因素验证
【正向验证】
1、预测多个与预期结果的相关因素
2、设定相关因素的量化边界
3、遍历相关因素数值范围，验证命中预期结果情况（涨幅比例）
4、查看散点分布情况
5、多相关因素联合比对相关性 OR 因素独立比对结果相关性   联合对比，独立对比太多

【反向验证】（统计出大拉升结果后，反向计算猜测因素的值）
1、计算出 统计拉升 结果
2、计算出拉升起点到之前x个交易日中最符合猜测切片的区间及影响参数的值
3、根据柱状图量分布情况，查看是否有众数存在（可先清洗目标外情况数据），是多少。
4、根据结果数据再正向计算，验证结果正确比值

相关因素：
#bool因素
1、斜率。 切片前后收盘价差涨幅绝对值   猜测斜率需要小于0.02
2、涨幅平稳（切片内涨跌幅小于2的日均数量）。 切片内日均涨幅   猜测标准小于0.7
3、凹凸系数。 遍历切片内每日偏离出切片首尾连线值的总和/切片长度  猜测标准小于0.015
#量化因素 总值小于10000
1、历史极值比。  历史最大值/切片平均值   猜测系数1000 最大值9000
2、拉升检测。    切片结束到数据结束  极大值大于 切片平均值的1.3倍为拉升  用于验证过期分数
3、庄线长度。     切片起始至结束*10    最大900

启动因素：
1、换手量升高，换手前10日平均值的2倍
"""
import pandas as pd

import pub_uti_a
class data:
    def __init__(self):
        self.zhangting_count = 4
        self.df = None
        self.select_lasheng()
    #查询拉升数据及其成交数据：
    def select_lasheng(self):
        sql = "select T.stock_id,T.stock_name,T.zhangting_count," \
              "T.trade_date as lasheng_date,S.trade_date," \
              "S.close_price,S.high_price,S.low_price " \
              " FROM tongji_dalasheng T " \
              " LEFT JOIN stock_trade_data S " \
              " ON T.stock_id = S.stock_id " \
              " WHERE T.zhangting_count >= {} ".format(self.zhangting_count)#AND T.stock_id = '000665'
        self.df =pub_uti_a.creat_df(sql,ascending=True)

class validate:
    def __init__(self):
        self.df = data().df
        self.res_df = pd.DataFrame(columns=("stock_id","stock_name","xielv",
                                            "aotu","day_rate","trade_date","zhangting_count"))
        self.file_name = "./validate_result/zhuang_element_valiadate_result.csv"
        self.stock_buffer()
        self.save()
    def stock_buffer(self):
        stock_id_set = set(self.df['stock_id'].to_list())
        for id in stock_id_set:
            single_df = self.df[self.df['stock_id'] == id]
            lasheng_date_set = set(single_df['lasheng_date'].to_list())
            for lasheng_date in lasheng_date_set:
                single_df_sun = single_df[single_df['lasheng_date']==lasheng_date]
                s = stock(single_df_sun)
                if s.effective:
                    self.res_df.loc[len(self.res_df)] = [
                        id,s.name,s.xielv,s.aotu,s.day_rate,lasheng_date,s.zhangting_count
                    ]
    def save(self):
        print(self.res_df)
        self.res_df.to_csv(self.file_name,index=False,encoding='UTF-8')
class stock:
    def __init__(self,single_df):
        self.name = single_df['stock_name'].to_list()[0]
        self.zhangting_count = single_df['zhangting_count'].to_list()[0]
        self.df = single_df
        self.effective = True
        self.piece = 45
        self.area_len = 120
        self.xielv = 10000
        self.aotu = 10000
        self.day_rate = 10000
        self.run()
    def run(self):
        print(self.name)
        self.clean_data()
        if not self.effective:
            return 0
        self.comp_zhuang()
    def clean_data(self):
        self.df = self.df.reset_index(drop=True)
        lasheng_date = str(self.df.loc[0,'lasheng_date'])
        for i in range(1, len(self.df) - 1):
            # DB中历史老数据缺失increase
            self.df.loc[i, 'increase'] = (self.df.loc[i, 'close_price'] - self.df.loc[i - 1, 'close_price']) / self.df.loc[
                i - 1, 'close_price'] * 100
            if -2 <= float(self.df.loc[i, 'increase']) <= 2:
                self.df.loc[i, 'increase_flag'] = 1
            else:
                self.df.loc[i, 'increase_flag'] = 0
        self.df.fillna(0,inplace=True)
        self.df['piece_flag_sum'] = self.df.increase_flag.rolling(self.piece).sum()
        self.df['increase_abs_sum'] = self.df.increase_flag.rolling(self.piece).sum()
        lasheng_index = self.df[self.df.trade_date == lasheng_date].index.to_list()[0]
        #判断df长度是否足够
        if self.area_len > lasheng_index:
            if self.area_len >30:
                self.area_len = lasheng_index
            else:
                print('拉升前长度不够。')
                self.effective = False
        self.df = self.df[lasheng_index-self.area_len : lasheng_index]
        self.df = self.df.reset_index(drop=True)
    def comp_zhuang(self):
        for i in range(len(self.df)-1,self.piece,-1):
            behind_cp = self.df.loc[i, 'close_price']
            front_cp = self.df.loc[i - self.piece, 'close_price']
            #斜率计算
            xielv = abs(behind_cp - front_cp) / front_cp
            #涨幅平稳比例（切片内涨跌幅小于2的日均数量）
            day_rate = self.df.loc[i, 'piece_flag_sum'] / self.piece
            #凹凸系数
            per_day = (behind_cp - front_cp) / self.piece
            ind_start = self.df.query("trade_date == '{}'".format(self.df.loc[i - self.piece, 'trade_date'])).index[0]
            ind_end = self.df.query("trade_date == '{}'".format(self.df.loc[i, 'trade_date'])).index[0]
            sum = 0
            count = 0
            for j in range(ind_start, ind_end + 1):
                refer = front_cp + per_day * count
                sum += abs(self.df.loc[j, 'close_price'] - refer) / refer
                count += 1
            aotu = abs(sum / self.piece)
            compare_count = 0
            if xielv < self.xielv:
                compare_count +=1
            if  day_rate < self.day_rate:
                compare_count +=1
            if aotu < self.aotu:
                compare_count +=1
            if compare_count >= 2:
                self.xielv = xielv
                self.day_rate = day_rate
                self.aotu = aotu

if __name__ == '__main__':
    validate()
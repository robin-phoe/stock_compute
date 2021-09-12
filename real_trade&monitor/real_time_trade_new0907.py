
#coding=utf-8
import requests
import re
import pymysql
import logging
import json
import datetime
import time
from multiprocessing import Pool
import redis
import pub_uti_a

logging.basicConfig(level=logging.INFO,filename="../log/real_trade_data.log",filemode="w",
                    format="%(asctime)s-%(levelname)5s: %(message)s")

count=0
r = redis.StrictRedis(host="localhost", port=6379, db=0, decode_responses=True)
"""
获取单个页面股票数据
{name:F14,最新价:F2,涨跌幅:F3,成交量:F5,ID:F12,成交额:F6,振幅:F7,最高:F15,最低:F16,今开:F17,昨收:F18,量比:F10,换手:F8,市盈(动):F9,市净率:F23}
"""
def getOnePageStock(page):
    global count,r
    url = "http://18.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406268274658974922_1605597357094&pn={}" \
          "&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13," \
          "m:0+t:80,m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20," \
          "f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1605597357108".format(page)
    url = "http://18.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406268274658974922_1605597357094&pn={}" \
          "&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13," \
          "m:0+t:80,m:1+t:2,m:1+t:23&fields=f2,f3,f4,f5,f8,f10,f12,f14,f15,f16,f17&_=1605597357108".format(page)
    #url = "http://18.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406268274658974922_1605597357094&pn=2&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1605597357108"
    #print("url:",url)
    header={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"}
    response = requests.get(url,headers=header)
    text=response.text
    result = re.findall("\[.*?\]", text)
    print("result:",page,result)
    if len(result) == 0:
        return 0
    else:
        Data_json = json.loads(result[0])
        #print(Data_json)
    save_to_redis(Data_json)
    save_to_redis_old(Data_json)
"""
临时使用redis存储，兼容旧版本监控等行情支持
"""
def save_to_redis_old(Data_json):
    for data in Data_json:
        # print('data:',data)
        id = data['f12']
        price = data['f2']
        increase = data['f3']
        high_price = data['f15']
        low_price = data['f16']
        open_price = data['f17']
        if price == '-':
            price = 0
        if increase == '-':
            increase = 0
        if high_price == '-':
            high_price = 0
        if low_price == '-':
            low_price = 0
        if open_price == '-':
            open_price = 0
        # r.set(id + '_price',data['f2'])
        # r.set(id + '_increase', data['f3'])
        r.lpush(id + '_price_list', price)
        r.lpush(id + '_increase_list', increase)
        r.hset('high_price', id,high_price)
        logging.info('high_price:{},{}'.format(id,high_price))
        r.hset('low_price', id,low_price)
        logging.info('low_price:{},{}'.format(id,low_price))
        r.hset('open_price', id,open_price)
        logging.info('open_price:{},{}'.format(id,open_price))

"""
日内行情保存至redis
"""
def save_to_redis(Data_json):
    for data in Data_json:
        timestamp = datetime.datetime.now().timestamp()
        id = data["f12"]
        name = data["f14"]
        price = data["f2"]
        increase = data["f3"]
        high_price = data["f15"]
        low_price = data["f16"]
        open_price = data["f17"]
        volume_rate = data["f10"]
        turnover = data["f8"]
        volume_money = data["f16"]
        volume = data['f5']
        #清理可能为 - 的数据值
        filed_list = [price,increase,high_price,low_price,open_price]
        for i in range(len(filed_list)):
            if filed_list[i] == "-":
                filed_list[i] = 0
        single_market = "single_market"
        day_market = "day_market"
        # 判断id 在hash中是否存在
        if (r.hexists(name=day_market, key=id)):
            old_market = json.loads(r.hget('single_market',id))
            # print('volume_check:',old_market,type(old_market))
            volume -= old_market["volume"]
            new_market = {"timestamp":timestamp,"id": id, "name": name, "price": price, "increase": increase, "volume": volume,
                          "volume_rate": volume_rate, "turnover": turnover, "volume_money": volume_money}
            all_market_str = r.hget('day_market',id)
            all_market_json = json.loads(all_market_str)
            all_market_json[timestamp] = new_market
        else:
            new_market = {"timestamp":timestamp,"id": id, "name": name, "price": price, "increase": increase, "volume": volume,
                          "volume_rate": volume_rate, "turnover": turnover, "volume_money": volume_money}
            all_market_json = {timestamp:new_market}

        r.hset("single_market", id,json.dumps(new_market, indent=2, ensure_ascii=False))
        r.hset("day_market", id, json.dumps(all_market_json, indent=2, ensure_ascii=False))
    return 1
"""
获取日内板块信息
"""
def get_bk_info(page):
    global r
    url = "http://44.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112404219198714508219_1607384110344&pn={}&pz=20&po=1&np=1&" \
          "ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12," \
          "f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207," \
          "f208,f209,f222&_=1607384110349".format(page)

    header={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"}
    response = requests.get(url,headers=header)
    text=response.text
    result = re.findall("\[.*?\]", text)
    print("result:",page,result)
    if len(result) == 0:
        return 0
    else:
        Data_json = json.loads(result[0])
        #print(Data_json)
    for data in Data_json:
        # print("data:",data)
        bk_name = data["f14"]
        bk_id = data["f12"]
        increase = data["f3"]
        if increase == "-":
            increase = 0

        r.hset("bk_increase", bk_id,increase)
        logging.info("bk_increase:{},{}".format(bk_id,increase))
        print("bk_increase:{},{}".format(bk_id,increase))


"""
个股、板块单次页面获取
"""
def main(page):
    # print("时间1:",datetime.datetime.now().strftime("%H:%M:%S,%f"))
    # print("page:",page)
    getOnePageStock(str(page))
    if int(page) <=5:
        get_bk_info(page)

"""
日交易结束，redis分时行情保存到mysql
"""
def save_to_mysql(date = None):
    if date == None:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
    all_market_json = r.hgetall('day_market')
    sv = pub_uti_a.save()
    for id in all_market_json:
        sql = "INSERT INTO miu_trade_date (stock_id,trade_date,data) values ('{0}','{1}','{2}')".format(id,date,all_market_json[id])
        sv.add_sql(sql)
    sv.commit()


"""
多进程执行获取行情
"""
def run():
    p = Pool(8)
    for i in range(220):
        p.apply_async(main, args=(str(i),))
    #    p.apply_async(main, args=("1",date,))
    #print("Waiting for all subprocesses done...")
    p.close()
    p.join()
    # market_tranfer()
    # print("All subprocesses done.")
if __name__ == "__main__":
    # run()
    # main(page=1)
    # save_to_mysql()
    i=0
    end_trade_flush= False
    start_trade_flush = True
    while True:
        time_now = datetime.datetime.now().strftime("%H:%M:%S")

        if time_now >= "09:20:00" and time_now <= "15:10:00" :
            # 开盘清理redis
            if start_trade_flush ==True:
                r.flushdb()
                print("已清空redis")
                start_trade_flush = False
                end_trade_flush = True
            time1 = datetime.datetime.now()
            run()
            time2 = datetime.datetime.now()
            time_delta = time2 - time1
            print("时间:",i,  time2.strftime("%H:%M:%S,%f"))
            print("时间差:",time_delta)
            i+=1
            time.sleep(50)
        #收盘redis持久化 & 清理redis
        elif end_trade_flush == True:
            save_to_mysql()
            time.sleep(300)
            r.flushdb()
            print("已清空redis")
            end_trade_flush = False
            start_trade_flush = True
        time.sleep(1)


'''
研究涨停次日低开
1、次日h_price 低于左收
2、次日开盘、收盘价低于昨收、h_price高于昨收
3、次日开盘价格高于昨收，收盘价低于昨收
factor name: limit_fall
'''
from .. typedef import market

###核心函数，输入昨收（涨停日），次日高开低收，计算分数
def compute_limit_fall_grade(y_close_price,maket:market) -> int:
    grade = 0
    if maket.high_price <= y_close_price:
        grade = 300000
    elif maket.high_price > y_close_price and maket.open_price <= y_close_price and maket.close_price <= y_close_price:
        grade = 200000
    elif maket.high_price > y_close_price and maket.open_price >= y_close_price and maket.close_price <= y_close_price:
        grade = 100000
    return grade




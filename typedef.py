#定义基础对象
from enum import Enum

class market:
    def __init__(self,trade_date,stock_id,stock_name,open_price,close_price,high_price,low_price,increase):
        self.trade_date = trade_date
        self.stock_id = stock_id
        self.stock_name = stock_name
        self.open_price= open_price
        self.close_price= close_price
        self.high_price= high_price
        self.low_price= low_price
        self.increase= increase

class signal:
    def __init__(self,stock_id,name,grade,signal_date,trade_code,conf=None,mark=None):
        self.stock_id = stock_id
        self.name = name
        self.grade = grade
        self.signal_date = signal_date
        self.trade_code = trade_code
        self.conf = conf
        self.mark = mark

class call_type(Enum):
    open = 1
    filter =2 #2.5%
    middle = 3
    close = 4
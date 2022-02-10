
#:!/usr/bin/env python
#:  -*- coding: utf-8 -*-
from npsdk import NpSdkApi

SYMBOL1 = "SH600000"
SYMBOL2 = "SH600007"
SYMBOL3 = "SH600010"

api = NpSdkApi(debug=True)
print("策略开始运行")

quote1 = api.fetch_quote(SYMBOL1)
quote2 = api.fetch_quote(SYMBOL2)
quote3 = api.fetch_quote(SYMBOL3)

klines1_min1 = api.fetch_klines(SYMBOL1, '1分钟线', 2000) 
klines1_min5 = api.fetch_klines(SYMBOL1, '5分钟线', 1700) 
klines2_day = api.fetch_klines(SYMBOL2, '日线', 2000) 
klines3_min5 = api.fetch_klines(SYMBOL3, '5分钟线', 1500) 

ticks1 = api.fetch_ticks(SYMBOL1)

while True:

    api.refreshing()

    if api.is_updated(klines1_min1.iloc[-1], ['time', 'close']):  # 判断K线变化
        print('%s klines1_min1变化 time:%s close:%s'%(klines1_min1.iloc[-1]["label"], klines1_min1.iloc[-1]["time"], klines1_min1.iloc[-1]["close"]))

    if api.is_updated(klines1_min5.iloc[-1], 'open'):  # 判断K线变化
        print('%s klines1_min5变化 open:%s'%(klines1_min5.iloc[-1]["label"], klines1_min5.iloc[-1]["open"]))

    if api.is_updated(klines2_day.iloc[-1]):  # 判断K线变化
        print('%s klines2_day变化 volume:%s'%(klines2_day.iloc[-1]["label"], klines2_day.iloc[-1]["volume"]))  

    if api.is_updated(klines3_min5.iloc[-1]):  # 判断K线变化
        print('%s klines3_min5变化 amount:%s'%(klines3_min5.iloc[-1]["label"], klines3_min5.iloc[-1]["amount"])) 

    if api.is_updated(quote1, ['time','open', 'high', 'low', 'close']): # 判断实时行情变化
        print("%s 开: %.2f 高: %.2f 低: %.2f 收: %.2f"%(quote1.name, quote1.open, quote1.high, quote1.low, quote1.close))

    if api.is_updated(quote2, ['time','close']): # 判断实时行情变化
        print("%s 收: %.2f"%(quote2.name,  quote2.close))

    if api.is_updated(quote3, ['time','volume','amount']): # 判断实时行情变化
        print("%s 成交量: %.2f 成交额: %.2f"%(quote3.name, quote3.volume,  quote3.amount))

    if api.is_updated(ticks1.iloc[-1],'time'):
        print("一个新tick序列")


# 关闭api,释放资源
api.close()
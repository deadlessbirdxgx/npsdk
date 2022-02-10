#:!/usr/bin/env python
#:  -*- coding: utf-8 -*-
from npsdk import NpSdkApi

SYMBOL = "SH600000"  # 证券代码
NDAY = 5  # 天数
K1 = 0.2  # 上轨K值
K2 = 0.2  # 下轨K值

api = NpSdkApi(debug=True)
print("策略开始运行")

quote = api.fetch_quote(SYMBOL)
klines = api.fetch_klines(SYMBOL, '日线', 1000)  # 使用日线

def dual_thrust(quote, klines):
    current_open = klines.iloc[-1]["open"]
    HH = max(klines.high.iloc[-NDAY - 1:-1])  # N日最高价的最高价
    HC = max(klines.close.iloc[-NDAY - 1:-1])  # N日收盘价的最高价
    LC = min(klines.close.iloc[-NDAY - 1:-1])  # N日收盘价的最低价
    LL = min(klines.low.iloc[-NDAY - 1:-1])  # N日最低价的最低价
    range = max(HH - LC, HC - LL)
    buy_line = current_open + range * K1  # 上轨
    sell_line = current_open - range * K2  # 下轨
    print("当前开盘价: %f, 上轨: %f, 下轨: %f" % (current_open, buy_line, sell_line))
    return buy_line, sell_line

while True:
    api.refreshing()

    if api.is_updated(klines.iloc[-1], ["time","open"]):  # 发生变化: 重新计算上下轨
        print('K线发生变化，重新计算上下轨')
        buy_line, sell_line = dual_thrust(quote, klines)
        
        if api.is_updated(quote, "close"): # 判断实时行情里的收盘价是否发生变化
            if quote.close > buy_line:  # 高于上轨
                print("高于上轨,目标持仓 多头3手")
            elif quote.close < sell_line:  # 低于下轨
                print("低于下轨,目标持仓 空头3手")
            else:
                print('未穿越上下轨,不调整持仓')

# 关闭api,释放资源
api.close()
#:!/usr/bin/env python
#:  -*- coding: utf-8 -*-
from npsdk import NpSdkApi

SYMBOL = "SH600000"  # 证券代码

api = NpSdkApi(debug=True)
print("策略开始运行")

ticks = api.fetch_ticks(SYMBOL)
klines = api.fetch_klines(SYMBOL, '1分钟线', 1000)

while True:
    api.refreshing()

    # 判断整个tick序列是否有变化
    if api.is_updated(ticks.iloc[-1],'time'):
        print("一个新tick序列")
            
    #判断最后一根K线的收盘价是否有变化
    if api.is_updated(klines.iloc[-1], ["time","close"]):
        # klines.close返回收盘价序列
        print("K线属性字段值变化", klines.close.iloc[-1])

# 关闭api,释放资源
api.close()

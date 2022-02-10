
#:!/usr/bin/env python
#:  -*- coding: utf-8 -*-
from npsdkapi import NpSdkApi

SYMBOL = "SH600000"

api = NpSdkApi(debug=True, playmode='nezipwebsocket')
print("策略开始运行")

quote = api.fetch_quote(SYMBOL)

while True:
        api.refreshing()

        if api.is_updated(quote, ['time','close']): # 判断实时行情里的收盘价是否发生变化
            if quote.close < 9: # 收盘价  
                print("%s : %s的收盘价 %.2f 低于9元"%(quote.label, quote.name, quote.close))
                break

# 关闭api,释放资源
api.close()
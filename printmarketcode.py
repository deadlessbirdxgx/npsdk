
#:!/usr/bin/env python
#:  -*- coding: utf-8 -*-
from npsdkapi import NpSdkApi
import time

api = NpSdkApi(debug=True)
print("打印网际风市场代码表......start")

while True:
    api.refreshing()

    time.sleep(10)
    
    api.print_market_code_info()
    break
            
print("打印网际风市场代码表......done")
# 关闭api,释放资源
api.close()

#:!/usr/bin/env python
#:  -*- coding: utf-8 -*-
from npsdkapi import NpSdkApi
import asyncio
import concurrent.futures
import time

"""
股票证券代码
"""
SYMBOL = [  "SH600000", "SH600004", "SH600006", "SH600007", "SH600010",
            "SH601778", "SH601788", "SH601789", "SH601798", "SH601799",

            "SZ000001", "SZ000002", "SZ000004", "SZ000005", "SZ000006",
            "SZ000060", "SZ000061", "SZ000062", "SZ000063", "SZ000065",
]

"""
期货证券代码
"""
SYMBOL1 = [  "SQag00", "SQag01", "SQag02", "SQag03", "SQag04",
            "SQag05", "SQag06", "SQag07", "SQag08", "SQag09",

            "SQal00", "SQal01", "SQal02", "SQal03", "SQal04",
            "SQal05", "SQal06", "SQal07", "SQal08", "SQal09",

            "DLl00", "DLl01", "DLl02", "DLl03", "DLl04",
            "DLl05", "DLl06", "DLl07", "DLl08", "DLl09",

            "DLjm00", "DLjm01", "DLjm02", "DLjm03", "DLjm04",
            "DLjm05", "DLjm06", "DLjm07", "DLjm08", "DLjm09",
]

symbol_num_todo = len(SYMBOL)
kline_len_todo = 500

api = NpSdkApi(debug=True, playmode='nezipwebsocket')
print("策略开始运行")

quotes =[]
for i in range(len(SYMBOL)):
    quotes.append(api.fetch_quote(SYMBOL[i]))

klines_1min = []
for i in range(symbol_num_todo):
    klines_1min.append(api.fetch_klines(SYMBOL[i], '1分钟线', kline_len_todo))

klines_5min = []
for i in range(symbol_num_todo):
    klines_5min.append(api.fetch_klines(SYMBOL[i], '5分钟线', kline_len_todo)) 

klines_1day = []
for i in range(symbol_num_todo):
    klines_1day.append(api.fetch_klines(SYMBOL[i], '日线', kline_len_todo))

def task1():
    for i in range(symbol_num_todo):
        if api.is_updated(quotes[i], ['time','open', 'high', 'low', 'close']): # 判断实时行情变化
            print("<%s> quotes变化 %s 开: %.2f 高: %.2f 低: %.2f 收: %.2f"%(i, quotes[i].name, quotes[i].open, quotes[i].high, quotes[i].low, quotes[i].close))
    #time.sleep(0.01)

def task2():
    for i in range(symbol_num_todo):
        if api.is_updated(klines_1min[i]):  # 判断K线变化
            print('<%s> %s klines_1min变化 time:%s close:%s'%(i, klines_1min[i].iloc[-1]["label"], klines_1min[i].iloc[-1]["time"], klines_1min[i].iloc[-1]["close"]))
    #time.sleep(0.01)

def task3():
    for i in range(symbol_num_todo):
        #print(klines_5min[i])
        if api.is_updated(klines_5min[i].iloc[-1]):  # 判断K线变化
            print('<%s> %s klines_5min变化 time:%s close:%s'%(i, klines_5min[i].iloc[-1]["label"], klines_5min[i].iloc[-1]["time"], klines_5min[i].iloc[-1]["close"]))
    #time.sleep(0.01) 

def task4():   
    for i in range(symbol_num_todo):
        #print(klines_1day[i])
        if api.is_updated(klines_1day[i].iloc[-1]):  # 判断K线变化
           print('<%s> %s klines_1day变化 time:%s close:%s'%(i, klines_1day[i].iloc[-1]["label"], klines_1day[i].iloc[-1]["time"], klines_1day[i].iloc[-1]["close"]))
    #time.sleep(0.01)

"""
异步协程 入口主函数
"""
async def _async_run_tasks() -> None:  

    loop = asyncio.get_running_loop()

    with concurrent.futures.ThreadPoolExecutor() as pool:      
        await loop.run_in_executor(pool, task1)  
        await loop.run_in_executor(pool, task2)  
        await loop.run_in_executor(pool, task3)  
        await loop.run_in_executor(pool, task4)  

    await asyncio.sleep(0.1)

while True:
    t_total = time.perf_counter()      

    api.refreshing()    

    asyncio.run(_async_run_tasks())     
    
    print(f'1 round({symbol_num_todo}) total coast:{time.perf_counter() - t_total:.8f}s')

# 关闭api,释放资源
api.close()


"""benchmark
40 quotes
40 klines(1min,5min,1day) 2000 lines

sync mode: 1 round coast: 2secs CPU: 30%
"""
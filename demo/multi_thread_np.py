#!/usr/bin/env python
#  -*- coding: utf-8 -*-
__author__ = 'Anderson'

import time
import threading
import PySimpleGUI as sg
from npsdkapi import NpSdkApi

SYMBOL1 = "SZ002174"
SYMBOL2 = "SH600007"
SYMBOL3 = "SH600010"

api = NpSdkApi()

quote_a = api.get_quote(SYMBOL1)
quote_b = api.get_quote(SYMBOL2)
kline_keys = [SYMBOL1, SYMBOL2, SYMBOL3]
kline_c = api.get_kline_serial(SYMBOL1, '1分钟线', 2000)
kline_d = api.get_kline_serial(SYMBOL2, '1分钟线', 2000)
kline_e = api.get_kline_serial(SYMBOL3, '1分钟线', 2000)
kline_values = [kline_c, kline_d, kline_e]
kdata = dict(zip(kline_keys, kline_values))
vv = SYMBOL1
real = True

class WorkingThread(threading.Thread):
    def run(self):
        while real:
            api.wait_update()

            if api.is_changing(quote_a):
                pass
            if api.is_changing(quote_b):
                pass

            try:
                if vv in kline_keys:
                    if api.is_changing(kdata[vv].iloc[-1], ['time', 'close']):  # 判断K线变化
                        time_local = time.localtime(kline_c.time.iloc[-1])
                        mytime = time.strftime("%Y-%m-%d %H:%M:%S",time_local)
                        print(f'{vv} {mytime} 收盘价: {kline_c.close.iloc[-1]}')

            except IndexError as e:
                print(f'{vv}索引越界异常：{e}'); 
                #if api.is_changing(kdata[vv]):  # 检查K线数据更新
                #    pass
                continue
            
        api.close()

# 创建线程
wt = WorkingThread()
wt.setDaemon(True)   # 守护线程，随主线程退出而退出
wt.start()

layout = [[sg.Text(SYMBOL1), sg.Text("99999", key="symbol01.last")],
          [sg.Text(SYMBOL2), sg.Text("99999", key="symbol02.last")],
          [sg.Text('spread'), sg.Text("99999", key="spread")],
          [sg.Text('输入新代码'), sg.Input(SYMBOL3, key="symbol")],
          [sg.Button('OK'), sg.Button('Cancel'), sg.Button('Exit')],
          ]

window = sg.Window('价差显示', layout)

while True:  # Event Loop
    event, values = window.Read(timeout=1)
    if event in (None, 'Exit'):
        #real = False
        break
    if event == 'OK':
        vv = values['symbol']
    if event == 'Cancel':
        window.Element('symbol').Update(vv)
    
    window.Element('symbol01.last').Update(quote_a.close)
    window.Element('symbol02.last').Update(quote_b.close)
    window.Element('spread').Update(quote_b.close - quote_a.close)

window.Close()
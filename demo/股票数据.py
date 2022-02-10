#!接收股票数据模块示例(python 3.7)

# 高速股票数据演示，使用前先运行 网际风.exe，打开本地 websocket

# websocket client 安装方法： pip install websocket-client
# numpy 安装方法：            pip install numpy

# 本示例尽量详细注释，没有注释的，请猜猜看

import time
import socket
import asyncio
import websocket
from websocket import create_connection

import threading
import configparser

import struct
import numpy as np
import os

from Stockdrv import *

class CRcvStock:
    def __init__(self, exeppath=".\\网际风\\"):
        os.environ["CUDA_DEVICES_ORDER"] = "PCI_BUS_IS"
        os.environ["CUDA_VISIBLE_DEVICES"] = '0'  # 表示调用0块GPU
        self.m_ws = None
        self.m_stop = False
        self.m_url = "ws://127.0.0.1:39398/"
        self.m_logined = 0
        # websocket.enableTrace(True)

        file = exeppath + "用户\\配置文件.ini"

        config = configparser.ConfigParser()  # 创建配置文件对象
        config.read(file, encoding='utf-16')  # 读取文件
        ip = config.get('第三方调用', '连接地址01')
        port = config.get('第三方调用', '端口01')
        self.m_url = 'ws://' + ip + ':' + port

    def ontimer(self):  # 定时线程
        while True:
            self.onask()
            if self.m_stop:
                break
            time.sleep(1)

    def onask(self):
        if self.m_logined != 10:
            return
        # self.m_logined += 1

        # 历史K线标准周期：分时、分笔、1分K线、5分钟K线、日线。15、30、60分钟由5分钟组成；周、月、季、年K线由日线生成。

        # 初始化：市场代码表、除权、财务、实时数据等，第一次连接，自动申请一次，市场代码有变动，主服务器会主动推送。
        # init = "类型=初始化&编码=unicode" # unicode 比 utf-8 快很多
        # self.send(init)

        # 复权：0 无复权或不填写(默认)；-1 向前复权：历史方向(通常做法)；1 向后复权：未来方向

        # 当天数据：0 不包括当天数据；
        # 格式：0 结构体(默认)、1 Json
        """
        设置K线参数 如果放在这里，就不会收到 日线推送
        """
        #separate = "类型=K线参数&当天数据=1&复权=0&格式=0"  # 按约定格式传回K线数据
        #self.send(separate)

        # split = "代码=SH600000&类型=除权"
        # finance = "代码=SH600000&类型=财务"
        # splitFin = "代码=SH600000&类型=除权财务"
        #f10 = "代码=SH600000&类型=F10资料"
        report = "代码=SH600000&类型=实时数据"

        print("正在申请%s \r\n" % (report))
        # self.send(split)
        # self.send(finance)
        # self.send(splitFin)
        #self.send(f10)
        self.send(report)

        # 读取本地缓存数据(预留功能)
        # {
        # CMemory m

        # if (self.send(split, &m))
        # {
        # OEM_DATA_HEAD *headS = (OEM_DATA_HEAD *)m.m_buf
        # DoStock(headS)
        # }
        # if (self.send(finance, &m))
        # {
        #   OEM_DATA_HEAD *headS = (OEM_DATA_HEAD *)m.m_buf
        #   DoStock(headS)
        # }
        # if (self.send(report, &m))
        # {
        #       OEM_DATA_HEAD *headS = (OEM_DATA_HEAD *)m.m_buf
        #       DoStock(headS)
        # }
        # }

        #day = "代码=SH600000&类型=日线&数量=0&开始=0"  #申请日线，数量：K线根数，0 使用面板设置值；开始：从最新往历史方向第几根
        #day = "代码=SH000006&类型=日线&数量=1000&开始=1000" #申请日线，数量：K线根数，0
        # 使用面板设置值；开始：从最新往历史方向第几根
        # week = "代码=SH000001&类型=周线&数量=5000" #申请周线(未支持)
        # month = "代码=SH600000&类型=月线&数量=5000" #申请月线(未支持)
        # quarter = "代码=SH600000&类型=季线&数量=5000" #申请季线(未支持)
        # year = "代码=SH600000&类型=年线&数量=5000" #申请年线(未支持)
        min1 = "代码=SH600000&类型=1分钟线&数量=2000" #申请1分钟线
        min5 = "代码=SH600000&类型=5分钟线&数量=3000" #申请5分钟线
        day = "代码=SH600000&类型=日线&数量=2000" #申请日线
        # min15 = "代码=SH600000&类型=15分钟线&数量=3000" #申请15分钟线(未支持)
        # min30 = "代码=SH600000&类型=30分钟线&数量=3000" #申请30分钟线(未支持)
        # min60 = "代码=SH600000&类型=60分钟线&数量=3000" #申请60分钟线(未支持)
        trace = "代码=SH600000&类型=分笔&数量=0" #申请分笔(每3秒)
        # tick = "代码=SH600000&类型=分时&数量=3000" #分时(每分钟)(未支持)

        #print("正在申请%s \r\n" % (trace))
        #for i in range(10):
        #    self.send(trace)

        # self.send(week)
        # self.send(month)
        # self.send(quarter)
        # self.send(year)

        #for i in range(10):
        #    self.send(min1)

        # self.send(min5)
        # self.send(min15)
        # self.send(min30)
        # self.send(min60)
        # self.send(trace)
        # self.send(tick)

        # printf("正在申请%s \r\n", askDay)
        # self.send(askDay)

        # 提示：申请某个板块，可以自行建立一个函数，连续申请，后台自动向服务器申请数据。

        # close  = "类型=关闭接口"
        # hide   = "类型=隐藏接口"
        # show   = "类型=显示接口"
        # market = "类型=市场代码表"

        # self.send(close)
        # self.send(hide)
        # self.send(show)
        # self.send(market)

        ask ='代码=SH600000&类型=1分钟线&数量=2000'
        self.send(ask)
        ask ='代码=SH600000&类型=5分钟线&数量=1700'
        self.send(ask)
        ask ='代码=SH600007&类型=日线&数量=2000'
        self.send(ask)
        ask ='代码=SH600010&类型=5分钟线&数量=1500'
        self.send(ask)
        ask ='代码=SH600000&类型=分笔&数量=0'
        self.send(ask)


    def on_message(self, message):  # 实时全推数据处理量大，建议拷贝数据后，扔到队列去 其它线程去处理
        p_head = cast(message, POINTER(OEM_DATA_HEAD))
        head = p_head[0]
        count = head.count
        kind = head.type
        #print('kind=%s len=%s count=%s'%(kind,head.len,head.count))
        #wsheadinfo = "ws data head info:\r\n[oemVer:%s] x [type:%s] x [label:%s] x [name:%s] x [flag:%s] x [askId:%s] x [len:%s] x [count:%s]" \
        #        % (head.oemVer, head.type, head.label, head.name, head.flag, head.askId, head.len, head.count)
        #print('wsheadinfo%s'%(wsheadinfo))
 
        size = sizeof(OEM_DATA_HEAD)  # 100 字节

        data = byref(head, size)
        if kind == '代码表':
            # size = sizeof(OEM_STKINFO)
            p_market = cast(data, POINTER(OEM_MARKETINFO))
            market = p_market[0]
            stk_info = cast(market.stkInfo, POINTER(OEM_STKINFO))  # 转换为指针，方便后续读入
            num = market.num
            mk_name = market.name
            for i in range(num):
                s = stk_info[i]
                label = s.label
                name = s.name
                if i < 2:
                    print("%s.%s(%s)" % (mk_name, label, name))

            return 1
        elif kind == '实时数据':
            # size = sizeof(OEM_REPORT)
            pReport = cast(data, POINTER(OEM_REPORT))
            for i in range(count):
                report = pReport[i]
                if report.label == 'SH600000':
                    label = report.label
                    name = report.name
                    tm = time.localtime(report.time)
                    tm_str = time.strftime('%Y-%m-%d %H:%M:%S', tm)
                    print("\r\n接收到实时数据%s(%s) : time:%s close : %0.2f\r\n" % (label, name, tm_str, report.close))
            return 1
        elif kind == '分时' or kind == '分笔':
            # size = sizeof(OEM_TRACE)
            p_trace = cast(data, POINTER(OEM_TRACE))
            buf = create_string_buffer(head.len + 1)
            memmove(buf, p_trace, head.len)  # 拷贝到内存
            #print('分笔len:%s'%head.len)

            # 下面的解析非常慢，是否有其它方式更快遍历数据？
            # i = 0
            # while i < count:
            # trace = pTrace[i]
            # i += 1

            # for j in range(count):
            #    trace = pTrace[j]

            if head.label == 'SH600000':
                label = head.label
                name = head.name
                print("接收到分时/分笔数据%s(%s) : " % (label, name))
            return 1



        elif self.Kline(kind):
            #logger.info(message)

            size = sizeof(RCV_KLINE)
            p_kline = cast(data, POINTER(RCV_KLINE))
            buf = create_string_buffer(head.len + 1)
            memmove(buf, p_kline, head.len)  # 直接拷贝到内存，然后保存，有需要到内存读取

            # 下面的解析非常慢，是否有其它方式更快遍历数据？
            # for i in range(count):
            #    kline = pKline[i]

            if head.label == 'SH600000':
                label = head.label
                name = head.name
                print("接收到K线(%s)数据%s(%s) 数量 %d : " % (kind, label, name, count))
            return 1
        elif kind == '除权':  # 头部 + 实际内容
            s_head = cast(data, POINTER(OEM_SPLIT_HEAD))
            split = cast(data, POINTER(OEM_SPLIT))
            i = 0
            while i < count:
                h = s_head[i]  # 取出头部
                num = h.num
                if h.label == 'SH600000':
                    label = h.label
                    name = h.name
                    print("\r\n接收到除权数据%s(%s) : \r\n" % (label, name))
                i += 1
                for j in range(num):
                    s = split[i + j]
                i += num

            return 1
        elif kind == '财务':
            p_fin = cast(data, POINTER(RCV_FINANCE))
            for i in range(count):
                f = p_fin[i]
                if f.label == 'SH600000':
                    label = f.label
                    name = f.name
                    print("\r\n接收到财务数据%s(%s) : \r\n" % (label, name))

            return 1
        elif kind == 'F10资料':  # 代码 + 名称 + 索引 + 文本，是否有更高效的处理方式？
            size = sizeof(OEM_F10INFO)
            h_len = count * size
            h = cast(data, POINTER(OEM_F10INFO))  # 索引
            t = byref(h[0], h_len)
            txt = cast(t, POINTER(CText))
            # str = txt.contents.data
            for i in range(count):
                id = h[i]

            if head.label == 'SH600000':
                label = head.label
                name = head.name
                print("\r\n接收到F10数据%s(%s)\r\n" % (label, name))

                # fName = "F10\\" + label + '.f10';  #保存索引备用
                # with open(fName, 'w') as f10:
                #    f10.write(h)

                # fName = "F10\\" + label + '.txt';  #保存文本
                # with open(fName, 'w') as fTxt:
                #    fTxt.write(str)

            return 1

    def Kline(self, type):  # 是否K线格式
        arr = ["1分钟线", "5分钟线", "15分钟线", "30分钟线", "60分钟线", "日线", "周线", "月线", "季线", "年线", "多日线"]
        x = arr.count(type)
        return x

    @staticmethod
    def on_error(error):
        print(error)

    def on_close(self):
        print("关闭服务器连接")
        self.m_logined = 0

    def on_open(self):
        print("服务器连接成功")
        self.m_logined = 10
        init = "类型=初始化&编码=unicode"  # unicode 比 utf-8 快很多
        self.send(init)

       
        """
        设置K线参数 如果放在这里，就会收到 日线推送
        """
        separate = "类型=K线参数&当天数据=1&复权=0&格式=0"  # 按约定格式传回K线数据
        self.send(separate)

    def send(self, ask):
        if self.m_logined == 0:  # 断线
            return 0

        try:
            ask = "股票数据?" + ask + "&版本=20210310"
            self.m_ws.send(ask)
        except Exception as e:
            error = "网络断开"
            self.on_error(error)

    def Connect(self):
        try:
            self.m_ws = create_connection(self.m_url, sockopt=((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),))
        except Exception as e:
            error = "连接失败"
            self.on_error(error)
            self.on_close()
            return
        try:
            self.on_open()
            while True:
                rcv = self.m_ws.recv()
                self.on_message(rcv)
        except Exception as e:
            error = "网络断开"
            self.on_error(error)
        finally:
            self.on_close()
            self.m_ws.close()
            return

    def Start(self):
        thread = threading.Thread(target=self.ontimer)
        thread.start()
        while True:
            self.Connect()
            if self.m_stop:
                break
            time.sleep(1)


if __name__ == "__main__":
    rcvStock = CRcvStock()
    rcvStock.Start()

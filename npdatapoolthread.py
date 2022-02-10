#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__all__ = ['NpWebsocketClient', 'NpDataPoolThread']
__author__ = 'nfjd'

import socket
import selectors
from websocket import create_connection, ABNF
from multiprocessing import Process, connection, JoinableQueue
import threading
import asyncio
import concurrent.futures
import configparser
import sys
import os
import time
import codecs
from urllib import parse
import numpy as np
import pandas as pd
from blinker import signal

from npsdkobjs import *
from Stockdrv import *

import logging
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
#logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
formatter = logging.Formatter(LOG_FORMAT)

handler = logging.FileHandler("npdatapoolthread.log")
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)
logger.addHandler(handler)
#logger.addHandler(console)

doc="""
	    #历史K线标准周期：分时、分笔、1分K线、5分钟K线、日线。15、30、60分钟由5分钟组成；周、月、季、年K线由日线生成。

	    #初始化：市场代码表、除权、财务、实时行情等，第一次连接，自动申请一次，市场代码有变动，主服务器会主动推送。
        #init = "类型=初始化&编码=unicode"  # unicode 比 utf-8 快很多
	    #self.Send(init)

	    #复权：0 无复权或不填写(默认)；-1 向前复权：历史方向(通常做法)；1 向后复权：未来方向

	    #当天数据：0 不包括当天数据；
	    #格式：0 结构体(默认)、1 Json
        #setPqram  = "类型=K线参数&当天数据=1&复权=0&格式=0&版本=OEM_Ver2020"  #按约定格式传回K线数据
        #self.Send(setPqram)

        #split    = "代码=SH600000&类型=除权"
        #finance  = "代码=SH600000&类型=财务"
        #splitFin = "代码=SH600000&类型=除权财务"
        #f10       = "代码=SH600000&类型=F10资料"
        #report   = "代码=SH600000&类型=实时行情"
        
        #print("正在申请%s \r\n" % (split))
        #self.send_command_over_ws(split)
        #self.send_command_over_ws(finance)
        #self.send_command_over_ws(splitFin)
        #self.send_command_over_ws(f10)
        #self.send_command_over_ws(report)
        
        #day      = "代码=SH600000&类型=日线&数量=0&开始=0"  #申请日线，数量：K线根数，0 使用面板设置值；开始：从最新往历史方向第几根
        day      = "代码=SH000001&类型=日线&数量=1000&开始=1000"  #申请日线，数量：K线根数，0 使用面板设置值；开始：从最新往历史方向第几根
        #week     = "代码=SH000001&类型=周线&数量=5000"         #申请周线(未支持)
        #month    = "代码=SH600000&类型=月线&数量=5000"         #申请月线(未支持)
        #quarter  = "代码=SH600000&类型=季线&数量=5000"         #申请季线(未支持)
        #year     = "代码=SH600000&类型=年线&数量=5000"         #申请年线(未支持)
        min1     = "代码=SH600000&类型=1分钟线&数量=3000"      #申请1分钟线
        min5     = "代码=SH600000&类型=5分钟线&数量=3000"      #申请5分钟线
        #min15    = "代码=SH600000&类型=15分钟线&数量=3000"     #申请15分钟线(未支持)
        #min30    = "代码=SH600000&类型=30分钟线&数量=3000"     #申请30分钟线(未支持)
        #min60    = "代码=SH600000&类型=60分钟线&数量=3000"     #申请60分钟线(未支持)
        #trace    = "代码=SH600000&类型=分笔&数量=0"            #申请分笔(每3秒)
        #tick     = "代码=SH600000&类型=分时&数量=3000"         #分时(每分钟)(未支持)
        
        #print("正在申请%s \r\n" % (day))
        #self.send_command_over_ws(day)
        #self.send_command_over_ws(week)
        #self.send_command_over_ws(month)
        #self.send_command_over_ws(quarter)
        #self.send_command_over_ws(year)
        #self.send_command_over_ws(min1)
        #self.send_command_over_ws(min5)
        #self.send_command_over_ws(min15)
        #self.send_command_over_ws(min30)
	    #self.send_command_over_ws(min60)
        #self.send_command_over_ws(trace)
        #self.send_command_over_ws(tick)
        
        #wprintf("正在申请%s \r\n", askDay)
        #self.send_command_over_ws(askDay)
        
        #提示：申请某个板块，可以自行建立一个函数，连续申请，后台自动向服务器申请数据。
        
        #close  = "类型=关闭接口"
        #hide   = "类型=隐藏接口"
        #show   = "类型=显示接口"
        #market = "类型=市场代码表"
        
        #self.send_command_over_ws(close)
        #self.send_command_over_ws(hide)
        #self.send_command_over_ws(show)
        #self.send_command_over_ws(market)
"""

"""
Nezip Websocket Class
"""
class NpWebsocketClient(object): 
    def __init__(self, parent, playmode = NPSDKAPI_PLAYMODE_NPWEBSOCKET, nezip_exe_path = ".\\网际风\\"):

        super().__init__()

        self._commands_set = set()
        self._commands_dict = {}
        
        """
        创建selector实例
        """
        self.npsel = selectors.DefaultSelector() 
        self.__parent = parent

        """
        初始化websocket client
        """
        self.m_ws   = None
        self.m_connected = False
        self.m_url  = "ws://127.0.0.1:39398/"
        #websocket.enableTrace(True)

        try:
            file = nezip_exe_path + "用户\\配置文件.ini"
            config = configparser.ConfigParser() 
            config.read(file, encoding='utf-16') 
            ip   = config.get('第三方调用', '连接地址01')
            port = config.get('第三方调用', '端口01')
            self.m_url = 'ws://' + ip +':' + port
        except Exception as e:
            self.m_url  = "ws://127.0.0.1:39398/"
            logger.warning('查找网际风数据接口程序配置参数 ip:port异常:%r'%e)
            logger.warning('使用默认ip:port参数: %s'%self.m_url) 

        self._playmode = playmode
        if self._playmode != NPSDKAPI_PLAYMODE_NPWEBSOCKET and self._playmode != NPSDKAPI_PLAYMODE_NPBACKTEST:
            self._playmode = NPSDKAPI_PLAYMODE_NPWEBSOCKET

        """
        数据包回放Websocket Server
        """
        if self._playmode == NPSDKAPI_PLAYMODE_NPBACKTEST:
            self.m_url  = "ws://127.0.0.1:6889"  

    def connect(self):
        try:
            if self.m_connected and self.m_ws:
                logprtstr = "数据接口已经连接成功"
                print(logprtstr); logger.info(logprtstr)  
                return True

            self.m_ws = create_connection(self.m_url, sockopt = ((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),))
            if self.m_ws:
                logprtstr = "数据接口连接成功"
                print(logprtstr); logger.info(logprtstr) 

                self.m_connected = True

                """
                websocket client 连接成功后，注册conn，读数据event，回调函数到selector上
                """    
                #self.m_ws.sock.setblocking(False)           
                self.npsel.register(self.m_ws, selectors.EVENT_READ, self.on_read_callback) 

                """
                如果策略程序正常循环运行，这时网际风数据接口程序（网际风.exe异常退出或者手工退出），再发初始化命令
                后续再收到的数据是否会影响策略程序？？？？？？？
                """
                self.send_init_commands()

                return True
            else:
                self.m_connected = False
                return False

        except Exception as e:
            logprtstr = "数据接口连接异常: %r"%e
            print(logprtstr); logger.warning(logprtstr)

            self.m_connected = False
            self.m_ws = None
            return False

    def open(self):
        logprtstr = "开始连接数据接口......"
        print(logprtstr); logger.info(logprtstr) 

        return self.connect()  

    def close(self):
        try:
            logprtstr = "关闭数据接口"
            print(logprtstr); logger.warning(logprtstr)

            """
            websocket client 关闭后，注销conn
            """                         
            self.npsel.unregister(self.m_ws)    

            self.m_connected = False
            self.m_ws.close() # 关闭websocket
            self.m_ws = None

        except Exception as e:
            logprtstr = 'close() exception: %r'% e
            print(logprtstr); logger.warning(logprtstr)


    #发送基于websocket的数据请求命令
    def send_wscommand(self, ask):
        if self.m_connected == False or self.m_ws == None:   
            logprtstr = "数据接口连接已断开(s)"
            logger.warning(logprtstr)
            return

        try:
            ask = "股票数据?" + ask + "&版本=20210310"
            self.m_ws.send(ask)     

            if self._playmode == NPSDKAPI_PLAYMODE_NPBACKTEST:
                time.sleep(0.1)  #数据包回测 发送命令消息需要 sleep,否则数据包回测会抛异常

        except Exception as e:
            logprtstr = 'send_wscommand() exception: %r'% e
            print(logprtstr); logger.warning(logprtstr)

            self.close()

    
    """
    主动读websocket数据函数
    return: (bool, bytes)
    """
    def receive_wsdata(self):
        if self.m_connected == False:   
            logprtstr = "数据接口连接已断开(r)"
            logger.warning(logprtstr)
            #print(logprtstr); 
            return False, b''

        try:
            # 网际风返回的是bytes, 数据包回测返回的是str
            msg = self.m_ws.recv() 

            # operation code values.
            #ABNF.OPCODE_CONT = 0x0
            #ABNF.OPCODE_TEXT = 0x1
            #ABNF.OPCODE_BINARY = 0x2
            #ABNF.OPCODE_CLOSE = 0x8
            #ABNF.OPCODE_PING = 0x9
            #ABNF.OPCODE_PONG = 0xa
            #resp_opcode, msg = self.m_ws.recv_data()
            
            # 数据包回放测试
            # https://wonzwang.blog.csdn.net/article/details/111600947  
            # 反斜杠转义问题 以及 bytes转str问题：
            # 问题描述：第一步：bytes 转 str 第二步 b = bytes(str, encoding="utf-8") 则 得到的bytes数据头多出来2个字节b',并且/变成//
            
            if self._playmode == NPSDKAPI_PLAYMODE_NPBACKTEST    \
                and isinstance(msg, str) and len(msg) >= 2 : #如果数据类型是str (数据包回测server传回来的数据类型为str)
                new_bytes = bytes(msg[2:-1], encoding="utf-8")  # 这是bytes类型, 切片截掉前2个字节 b'
                msgbytes = codecs.escape_decode(new_bytes, "hex-escape") # 返回元组
                msg = msgbytes[0]
            #-----------------------------------------------------------------------------------------

            return True, msg
        except Exception as e:
            logprtstr = 'receive_wsdata() exception: %r'% e
            print(logprtstr); logger.warning(logprtstr)

            self.close()
            return False, b''

    """
    selector 读数据回调函数
    """
    def on_read_callback(self, conn, mask):
        #print('mask:', mask)

        try:
            # 网际风返回的是bytes, 数据包回测返回的是str
            data = conn.recv()  # Should be ready
            if data:
                if self._playmode == NPSDKAPI_PLAYMODE_NPBACKTEST    \
                    and isinstance(data, str) and len(data) >= 2 : #如果数据类型是str (数据包回测server传回来的数据类型为str)
                    new_bytes = bytes(data[2:-1], encoding="utf-8")  # 这是bytes类型, 切片截掉前2个字节 b'
                    msgbytes = codecs.escape_decode(new_bytes, "hex-escape") # 返回元组
                    data = msgbytes[0]

                """
                if self.__parent._thread_lock.acquire():
                    self.__parent.on_message(data) # websocket数据进npdatapoolthread数据字典
                    self.__parent._thread_lock.release()
                else:
                    logger.info('npdatapool acquire thread_lock failed')
                """
       
                self.__parent.on_message_pro(data) # websocket数据进npdatapoolthread数据字典
                #asyncio.run(self.__parent.on_message(data))

            else:
                logprtstr = 'on_read_callback() exception: 有数据信号但读取到空数据'
                print(logprtstr); logger.warning(logprtstr)

                self.close() # 在 close()函数里 unregister(conn) 
                #self.npsel.unregister(conn)  # 对于linux系统: 如果收到的消息为空,注销conn对象,


        #except ConnectionResetError as e:
        except Exception as e:
            logprtstr = 'on_read_callback() exception: %r'% e
            print(logprtstr); logger.warning(logprtstr)

            self.close() # 在 close()函数里 unregister(conn) 
            #self.npsel.unregister(conn)  # 对于windows系统: 连接突然中断,注销conn对象,

    """
    websocket连接成功后，向网际风数据接口程序发送初始化业务命令
    """
    def send_init_commands(self):

        init = "类型=初始化&编码=unicode"  # unicode 比 utf-8 快很多
        self.send_wscommand(init)
        logger.info('send ws command: %s' % init)

        market = "类型=市场代码表"
        self.send_wscommand(market)
        logger.info('send ws command: %s' % market)

        #当天数据：0 不包括当天数据；
        #格式：0 结构体(默认)、1 Json
        #setPqram  = "类型=K线参数&当天数据=1&复权=0&格式=0&版本=OEM_Ver2020"  #按约定格式传回K线数据
        setPqram = "类型=K线参数&当天数据=1&复权=0&格式=0"  # 按约定格式传回K线数据
        self.send_wscommand(setPqram)   
        logger.info('send ws command: %s' % setPqram)

    """
    注册业务命令
    """
    def register_wscommand(self, command):

        self._commands_set.add(command) # 注册websocket commands命令用set集合去除重复命令
        
    """
    给不同的业务命令分配定时器阀值，起始时间戳，并且去除重复命令
    """
    def parse_command(self, command):

        if command in self._commands_dict.keys():
            return # 重复命令不处理

        # 解析command参数
        result = parse.urlparse(command)
        query_dict = parse.parse_qs(result.path)
        type = query_dict.get('类型').pop()

        # 根据业务命令类型分配 定时器阀值 及 起始时间戳
        # 代码=SH600007&类型=实时数据
        if type == NPSDKAPI_NEZIP_QUOTE:
            self._commands_dict[command] = [NPSDKAPI_TIMER_1SECS, time.time()] 

        # 代码=SH600000&类型=1分钟线&数量=2000
        elif type in NPSDKAPI_NEZIP_KTYPES:
                self._commands_dict[command] = [NPSDKAPI_TIMER_3SECS, time.time()]

        # 代码=SH600000&类型=分笔&数量=0
        elif type == NPSDKAPI_NEZIP_OEMTRACE:
            self._commands_dict[command] = [NPSDKAPI_TIMER_1SECS, time.time()]   

        else:
            #print('其它命令：', command)
            pass

    """
    扫描&发送业务命令定时器阀值
    """
    def send_command(self):

        for cmd in self._commands_set:
            self.parse_command(cmd)

        for cmd in self._commands_dict.keys():

            #delta = time.time() - self._commands_dict[cmd][1]
            #tim = self._commands_dict[cmd][0]

            if time.time() - self._commands_dict[cmd][1] >= self._commands_dict[cmd][0]: #超时
                self.send_wscommand(cmd)

                #if self.__parent._npsdk_debug:
                #    logger.info('send ws command: %s' % cmd)

                self._commands_dict[cmd][1] = time.time() # 重新设置起始时间戳

        #print('>>>', self._commands_dict)


"""
NpDataPool Thread Class
"""
class NpDataPoolThread(threading.Thread):

    internal_lock = threading.Lock()

    """ 
    本地业务数据内存截面
    1. 市场代码表 
    2. 实时行情 
    3. ...
    4. ...
    """
    glb_marketinfo_dict = {} # 市场表 证券代码表 数据字典
                                    # {                      
                                    #   mkId: [OEM_MARKETINFO, {'SH600000':OEM_STKINFO,
                                    #                           'SH600007':OEM_STKINFO,
                                    #                           },
                                    #           
                                    #           ],
                                    #           
                                    #           
                                    #   mkId: [OEM_MARKETINFO, {'SZ600000':OEM_STKINFO,
                                    #                           'SZ600007':OEM_STKINFO,
                                    #                           },
                                    #           
                                    #           ],
                                    # }
    glb_marketinfo_dict.clear()

    glb_stk_report_dict = {} # 证券实时行情 数据字典
                                    # {
                                    #   'SH600000':OEM_REPORT,
                                    #   'SH600007':OEM_REPORT,
                                    #   ......
                                    #   'SH600100':OEM_REPORT,
                                    #   'SH600101':OEM_REPORT,
                                    # }
    glb_stk_report_dict.clear()

    #["1分钟线", "5分钟线", "15分钟线", "30分钟线", "60分钟线", "日线", "周线", "月线", "季线", "年线", "多日线"] 还包括 "分笔"
    glb_stk_klines_dict = {} # 证券K线数据 数据字典 
                                    # {                      
                                    #   'SH600000':{'1分钟线':[原始数据字节流, df]    
                                    #               '5分钟线':[原始数据字节流, df]
                                    #               ......
                                    #              },
                                    #   ......
                                    #   'SH600007':{'1分钟线':[原始数据字节流, df]
                                    #               '日线':   [原始数据字节流, df]
                                    #               ......
                                    #              },
                                    # }
    glb_stk_klines_dict.clear()

    def __init__(self, lock, debug,  playmode, nezip_exe_path = ".\\网际风\\"):

        """ 运行策略程序时，如果网际风.exe未运行，则启动网际风.exe
        # 1.在DOS窗口下，功能正常
        # 2.在Vscode IDE环境下，如果网际风.exe未运行，则启动网际风.exe，
        # 但是如果通过Vscode菜单停止策略程序的同时，网际风.exe也随之结束，有问题！
        """
        super().__init__()

        self._thread_lock = lock  #全局线程锁
        self._npsdk_debug = debug

        #self._data_received_signal = data_received_signal
       
        self._playmode = playmode
        if self._playmode != NPSDKAPI_PLAYMODE_NPWEBSOCKET and self._playmode != NPSDKAPI_PLAYMODE_NPBACKTEST:
            self._playmode = NPSDKAPI_PLAYMODE_NPWEBSOCKET

        self._nezip_exe_path = nezip_exe_path

        # 创建WebSocket Client
        logger.info('create internal websocket client...')
        self.__NpWsClient = NpWebsocketClient(self, self._playmode, self._nezip_exe_path)

    def start_me(self):

        # 启动Websoket Client连接数据接口
        self.__NpWsClient.open()

        self.__npdatapoolthread_stopevent = threading.Event()
        self.__npdatapoolthread_stopevent.clear()

        self.__sendcommandsthread_stopevent = threading.Event()
        self.__sendcommandsthread_stopevent.clear()

        self.__sendcommandsthread = threading.Thread(target=self.send_commands_thread,args=(self.__sendcommandsthread_stopevent,))
        self.__sendcommandsthread.setDaemon(True)
        self.__sendcommandsthread.start()

        """ 主线程体 """ 
        self.receive_data_pro()

    def stop_me(self):
        
        self.__npdatapoolthread_stopevent.set() #停止线程npdatapoolthread
        self.__sendcommandsthread_stopevent.set() #停止线程sendcommandsthread
        
        self.__NpWsClient.close()

    """
    接收数据主函数
    """
    def receive_data_pro(self):

        """ 二种读取websocket数据的方法比较
        1. 循环调用 receive_wsdata() 读取数据 (recv 不能设置timeout)
        2. select EVENT_READ 触发读取 (select 可以设置timeout)

        大吞吐量数据，二者CPU占有率应该差不多。数据量小或者无数据时，方法2更高效。
        """

        while not self.__npdatapoolthread_stopevent.is_set():
            try:            
                """
                if self.__NpWsClient.m_connected and self.__NpWsClient.m_ws:

                    bret, message = self.__NpWsClient.receive_wsdata()  #无数据阻塞在这里
                    if bret:
                        self.on_message_pro(message) 
                        #asyncio.run(self.on_message(message))                       

                else: # 连接已断开，重新连接
                    self.__NpWsClient.open()
                
                time.sleep(NPSDKAPI_THREAD_SLEEP_VAL)
                """
                                                
                if self.__NpWsClient.m_connected and self.__NpWsClient.m_ws:
                    
                    """
                    # select(timeout)
                    # timeout -- if timeout > 0, this specifies the maximum wait time, in seconds
                    # if timeout <= 0, the select() call won't block, and will report the currently ready file objects
                    # if timeout is None, select() will block until a monitored file object becomes ready

                    # 在selector注册的fd有数据信号, 调用回调函数
                    """
                    events = self.__NpWsClient.npsel.select() 
                    for key, mask in events:
                        callback = key.data
                        callback(key.fileobj, mask)

                else: # 连接已断开，重新连接
                    self.__NpWsClient.open()     
                        
                #time.sleep(NPSDKAPI_THREAD_SLEEP_VAL) # 这里不能sleep(), sleep()会让出GIL锁给别的线程
                    
            except Exception as e:
                logprtstr = 'receive_data_pro() exception: %r'% e
                print(logprtstr); logger.warning(logprtstr)

                continue

    """
    发送命令子线程入口函数
    """
    def send_commands_thread(self, stopevent):

        while not stopevent.is_set():

            """同步模式"""
            self.__NpWsClient.send_command() #定时发送业务命令

            time.sleep(NPSDKAPI_THREAD_SLEEP_VAL) 

    """
    发送命令函数
    """
    def send_command_pro(self):

        self.__NpWsClient.send_command() #定时发送业务命令        
    
    """
    注册业务命令
    """
    def register_command(self, command):

        self.__NpWsClient.register_wscommand(command)

    """""""""""""""""""""""""""""""""""""""""""""
    （Websocket 接收消息处理）:
    1. 接收并分析来自websocket的业务数据,
    2. 更新内存数据池中的业务数据字典
    .....
    """""""""""""""""""""""""""""""""""""""""""""

    """
    async def on_message(self, wsmessage):

        loop = asyncio.get_running_loop()

        #print('npdatapool on loop:%s' % id(loop))

        # 2. Run in a custom thread pool:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(
                pool, self.on_message_pro, wsmessage)
            #print('custom thread pool', result)
    """

    def on_message_pro(self, wsmessage): 
        # 纪录历史数据包，以备数据包回测用
        #with open('backtest0611-2.dat','ab') as fo:
        #    fo.write(message)

        #print('K线字典内存len: %s usize: %s'%(len(self.glb_stk_klines_dict), sys.getsizeof(self.glb_stk_klines_dict)))

        try:
            if len(wsmessage) == 0 or len(wsmessage) < sizeof(OEM_DATA_HEAD) :
                # 接收到的消息长度为0 或者 小于OEM_DATA_HEAD结构体长度，则为错误消息，立即返回
                print('错误消息: 接收到的消息 长度为0或者小于OEM_DATA_HEAD结构体长度')
                logger.error('错误消息: 接收到的消息 长度为0或者小于OEM_DATA_HEAD结构体长度')
                return 

            """
            message = HEAD + BODY
            HEAD: (100字节)
            """
            wshead = OEM_DATA_HEAD()
            wshead.decode(wsmessage)  #把字节流 转成 ctypes结构体            
            
            """
            if self._npsdk_debug:
                wsheadinfo = "ws data head info:\r[oemVer:%s] x [type:%s] x [label:%s] x [name:%s] x [flag:%s] x [askId:%s] x [len:%s] x [count:%s]" \
                % (wshead.oemVer, wshead.type, wshead.label, wshead.name, wshead.flag, wshead.askId, wshead.len, wshead.count)
                #print(wsheadinfo)
                logger.info(wsheadinfo)
            """

            """
            message = HEAD + BODY
            BODY: (可变长度)
            """
            wsbody = wsmessage
            wsbody = wsmessage[sizeof(OEM_DATA_HEAD) : ] # 取出 wsbody

            msgtype   = wshead.type            #消息类型

            """
            # 这里默认为websocket server 一次性发送的字节流都是完整的业务数据流，不存在没发完的残留数据
            # 需要大消息体测试 (比如一次性收 几千根Kline数据)
            """ 

            #-"""市场代码表数据处理"""-#
            if msgtype == '代码表':
                len_to_do = sizeof(OEM_MARKETINFO) - 10 * sizeof(OEM_STKINFO) + wshead.count * sizeof(OEM_STKINFO)
                if len(wsbody) == 0 or \
                len(wsbody) != len_to_do:
                    logprtstr = '错误消息: 接收到的市场消息体长度为0 或者 实际长度不等于应收长度'
                    logger.error(logprtstr)
                    return 
                #print('len_indeed: %s  len_to_do: %s'% (len(wsbody), len_to_do))

                #t = time.perf_counter()

                marketbody = OEM_MARKETINFO()
                marketbody.decode(wsbody)               #字节流 转成 ctypes结构体
                wsbody = wsbody[sizeof(OEM_MARKETINFO) - 10 * sizeof(OEM_STKINFO)  : ]  #删除一个结构体 + 额外字节

                """
                # 收盘时间 ctypes SHORT * 8 转 python tuple
                """
                """
                def ctypes_c_short_Array_8_to_int(ct):
                    barray = bytearray(ct)
                    print('==================',barray)
                    count = int(len(barray)/2)
                    intt = struct.unpack('h'*count, barray)
                    print('==================', intt)
                    return intt[0]

                iminutes = ctypes_c_short_Array_8_to_int(marketbody.closeTime)
                print('%d'%iminutes)
                #iminutes = 0X023A 或 570 = '09:30'
                val = time.localtime(iminutes) 
                val = time.strftime('%y-%m-%d %H:%M:%S', val)
                print('=========%s========='%val)
                """

                #print(marketbody.mkId, marketbody.name, marketbody.tmCount, marketbody.openTime, \
                #        marketbody.closeTime, marketbody.date, marketbody.num, marketbody.temp, marketbody.stkInfo)
                #print(marketbody.mkId, marketbody.name, marketbody.tmCount, marketbody.num)

                """过滤无效数据"""
                #print('>>>>>>>>>>>>>>>>>> marketinfo raw_data incoming: mkId(%s) num(%s) wslen(%s)'%(marketbody.mkId, marketbody.num, len(wsbody)))
                if marketbody.mkId == 0 or marketbody.num == 0 or len(wsbody) == 0:
                    return

                #以市场代码为key值 更新市场字典 
                if not marketbody.mkId in self.glb_marketinfo_dict.keys():
                    self.glb_marketinfo_dict[marketbody.mkId] = [marketbody,{}]
                else:
                    subdict = self.glb_marketinfo_dict[marketbody.mkId][1]
                    self.glb_marketinfo_dict[marketbody.mkId] = [marketbody,subdict]

                for i in range(marketbody.num):
                    stkbody = OEM_STKINFO()
                    
                    stkbody.decode(wsbody[sizeof(OEM_STKINFO) * i : ])  #字节流 转成 ctypes结构体
                    #wsbody = wsbody[sizeof(OEM_STKINFO) : ]            #删除一个结构体

                    #print(stkbody.code, stkbody.market, stkbody.block, stkbody.label, stkbody.name)
                    #logger.info('code:%s market:%s label: %s name:%s isStock:%s isIndex:%s pinYin:%s'%(stkbody.code, stkbody.market, stkbody.label, stkbody.name, stkbody.isStock, stkbody.isIndex, stkbody.pinYin))

                    self.glb_marketinfo_dict[marketbody.mkId][1][stkbody.label] = stkbody

                #print(f'代码表 coast:{time.perf_counter() - t:.8f}s')

                """ 向npsdkapi线程发送收到数据信号 
                self._data_received_signal.send(NPSDKAPI_SIGNAL_DATA_CODES) 
                """
            
            #-"""实时行情数据处理"""-#
            elif msgtype == NPSDKAPI_NEZIP_QUOTE:  
                if len(wsbody) == 0 or len(wsbody) != wshead.count * sizeof(OEM_REPORT):
                    logprtstr = '错误消息: 接收到的实时行情消息体长度为0 或者 实际长度不等于count*sizeof(OEM_REPORT)'
                    logger.error(logprtstr)
                    return 

                for i in range(wshead.count):
                    reportbody = OEM_REPORT()
                    reportbody.decode(wsbody[sizeof(OEM_REPORT) * i : ])    #字节流 转成 ctypes结构体
                    #wsbody = wsbody[sizeof(OEM_REPORT) : ]                 #删除一个结构体
                    #print(reportbody.label, reportbody.name)

                    """
                    检查 reportbody.label 是否在命令请求集合里
                    返回实时行情数据：
                    (1) 发送命令请求 
                    (2) 服务器主动推送 
                    (3) 串门的数据：多个策略程序进程同时连接同一个网际风，网际风会广播数据
                    (3) 如果网际风控制面板 同时勾选 股票和期货，服务器会同时推送股票+期货的实时行情，数据量巨大
                    所以需要过滤掉没申请过的实时行情数据，即可节省内存 也少消耗CPU
                    """
                    cmd = '代码=' + reportbody.label + '&类型=实时数据'
                    if not (cmd in self.__NpWsClient._commands_set):
                        #print('没请求过实时行情的证券代码: %s'%reportbody.label)
                        continue
                                        
                    #以证券代码为key值 更新实时行情字典 
                    self.glb_stk_report_dict[reportbody.label] = reportbody   
                    #logger.info('更新实时行情字典: symbol:%s time: %s len of dict：%s'% (reportbody.label, time.strftime('%y-%m-%d %H:%M:%S', time.localtime(reportbody.time)), len(self.glb_stk_report_dict)))

                    """
                    if reportbody.label == 'SH600000' :
                        tm = time.localtime(reportbody.time)
                        tmStr = time.strftime('%Y-%m-%d %H:%M:%S', tm)
                        print("接收到实时行情: %s(%s) : time:%s close : %0.2f" % (reportbody.label, reportbody.name, tmStr, reportbody.close))
                    """
                #print('---------------len(report dict):%s'%len(glb_stk_report_dict))

                """ 向npsdkapi线程发送收到数据信号 
                self._data_received_signal.send(NPSDKAPI_SIGNAL_DATA_QUOTES) 
                """
                                   
            #-"""K线数据处理"""-#
            elif msgtype in NPSDKAPI_NEZIP_KTYPES or msgtype == NPSDKAPI_NEZIP_OEMTRACE: # '分笔'Ticks 3秒:

                unitsize = sizeof(RCV_KLINE)
                if msgtype == NPSDKAPI_NEZIP_OEMTRACE:
                    unitsize = sizeof(OEM_TRACE)

                #logger.info(wsbody) 
                if len(wsbody) == 0 or len(wsbody) != wshead.count * unitsize:
                    logprtstr = '错误消息: 接收到的K线数据消息体长度为0 或者 实际长度不等于count * unitsize'
                    logger.error(logprtstr)
                    return

                symbol = wshead.label       #证券代码
                klines_type = wshead.type   #Klines类型字符串

                """
                检查 symbol，klines_type 是否在命令请求集合里
                返回k线数据：
                (1) 发送命令请求 
                (2) 串门的数据：多个策略程序进程同时连接同一个网际风，网际风会广播数据
                所以需要过滤掉没申请过的k线数据，即可节省内存 也少消耗CPU
                """  
                cmd = '代码=' + symbol + '&类型=' + klines_type
                bfound = False
                for cmdstr in self.__NpWsClient._commands_set:
                    if cmd in cmdstr:
                        bfound = True
                        break
                if not bfound:
                    #print('没请求过K线数据的证券代码: %s:%s'%(symbol,klines_type))
                    return

                #把K线数据原始字节流转成DataFrame
                #df_kline = self._convert_klines_to_dataframe(symbol, klines_type, wsbody) 

                #以证券代码为key值 更新K线数据一级字典。以K线类型为key值 更新K线数据二级字典。
                if  symbol in self.glb_stk_klines_dict.keys():

                    #self.glb_stk_klines_dict[symbol][klines_type] = [wsbody, df_kline] 
                    
                    if klines_type in self.glb_stk_klines_dict[symbol].keys():
                        
                        """
                        判断该证券代码前后二次收到的字节流是否一样
                        如果一样，则不转df,节省CPU消耗  
                        """                      
                        if self.glb_stk_klines_dict[symbol][klines_type][0] != wsbody:

                            #把K线数据原始字节流转成DataFrame
                            #t = time.perf_counter()
                            df_kline = self._convert_klines_to_dataframe(symbol, klines_type, wsbody) 
                            #print(f'_convert_klines_to_dataframe() {klines_type} : {wshead.count}根K线 coast:{time.perf_counter() - t:.8f}s')
                            #logger.info('更新K线数据字典: %r'%df_kline)                     
                            
                            self.glb_stk_klines_dict[symbol][klines_type] = [wsbody, df_kline] 

                        #else:
                        #    print('same klines bytes')
                    else:
                        #把K线数据原始字节流转成DataFrame
                        df_kline = self._convert_klines_to_dataframe(symbol, klines_type, wsbody) 

                        self.glb_stk_klines_dict[symbol][klines_type] = [wsbody, df_kline] 
                    

                else: # 该证券代码第一次传送K线数据

                    #把K线数据原始字节流转成DataFrame
                    df_kline = self._convert_klines_to_dataframe(symbol, klines_type, wsbody) 

                    klines_dict = {}
                    klines_dict[klines_type] = [wsbody, df_kline]
                    self.glb_stk_klines_dict[symbol] = klines_dict

                """ 向npsdkapi线程发送收到数据信号 
                if ischanged:
                    self._data_received_signal.send(NPSDKAPI_SIGNAL_DATA_KLINES) 
                else:
                    print('the same received klines bytes')
                """
                
            elif msgtype == '分时' :
                pass
            
            elif msgtype == '除权':
                pass

            elif msgtype == '财务' :
                pass

            elif msgtype == 'F10资料' :                 
                pass

        
        except Exception as e:
            logger.warning('on_message_pro() exception: %r'% e)

 
    """
    主线程入口函数：调用npdatapoolthread.start()即进入run()
    """
    def run(self):
        self.start_me()  

    # ----------------------------------------------------------------------
    # 把K线数据字节流转成DataFrame
    def _convert_klines_to_dataframe(self, symbol, kline_type, klines_rawdata)->pd.DataFrame:
        try:
            df_kline = None

            if kline_type == NPSDKAPI_NEZIP_OEMTRACE: #'分笔'
                fieldsOfObj = fieldsOfTrace
                unitsize = sizeof(OEM_TRACE)
            else:
                fieldsOfObj = fieldsOfKline
                unitsize = sizeof(RCV_KLINE)

            #t = time.perf_counter()

            count = int(len(klines_rawdata) / unitsize) # 计算多少根K线
            kline_list_des = []
            for i in range(count):
                if kline_type == NPSDKAPI_NEZIP_OEMTRACE:
                    klinebody = OEM_TRACE()
                else:
                    klinebody = RCV_KLINE()
                klinebody.decode(klines_rawdata[unitsize * i : ])  #字节流 转成 ctypes结构体
                #klines_rawdata = klines_rawdata[unitsize : ]      #删除一个结构体
                fieldval=[i, symbol, kline_type]                        # 放入K线序列号id，从0开始，指定证券代码, K线类型
                
                for field in fieldsOfObj:
                    """
                    if field == 'time': #localize the timestamp to local datetime
                        val = time.localtime(klinebody.time)
                        val = time.strftime('%y-%m-%d %H:%M:%S', val)
                    else:
                        val = getattr(klinebody, field)
                    fieldval.append(val)
                    """
                    fieldval.append(getattr(klinebody, field))
                kline_list_des.append(fieldval)

            #print(f'coast:{count} {time.perf_counter() - t:.8f}s')

            #创建DataFrame:行索引默认从0开始，列索引里加入K线序列号id，也是从0开始, 指定证券代码, K线类型
            if len(kline_list_des) == 0: #创建空df
                df_kline = pd.DataFrame(columns=['id', 'label', 'ktype'] + fieldsOfObj)
            else:
                df_kline = pd.DataFrame(kline_list_des, columns=['id', 'label', 'ktype'] + fieldsOfObj)
            #df_kline.set_index(["time"], inplace=True)
            #df_kline['updatetime'] = pd.to_datetime(df_kline.time, origin=pd.Timestamp('1970-01-01 00:00:00'))
            df_kline['updatetime'] = pd.to_datetime(df_kline.time)
            df_kline.set_index(["updatetime"], inplace=True)
            
        except Exception as e:
            logger.warning('_convert_klines_to_dataframe() exception: %r'% e)
        finally:
            return df_kline
    
    # ----------------------------------------------------------------------
    # 比较并且合并二次收到的K线数据DataFrame
    # 返回元组 (bool, pd.DataFrame)
    def _compare_merge_klines_dataframe(self, df1st, df2nd):
        try:
            if not isinstance(df1st, pd.DataFrame) or not isinstance(df2nd, pd.DataFrame):
                #logprtstr = 'invalide DataFrame Obj'
                #print(logprtstr); logger.info(logprtstr)             
                return False, None
            
            if len(df1st.index) == 0 and len(df2nd.index) == 0:
                #logprtstr = 'both dfs are empty DataFrame (df1st: %s )(df1st: %s )'%(len(df1st.index), len(df2nd.index))
                #print(logprtstr); logger.info(logprtstr)  
                return False, None

            """
            Test whether two objects contain the same elements.
            This function allows two Series or DataFrames to be compared against each other to 
            see if they have the same shape and elements. NaNs in the same location are considered equal.
            The row/column index do not need to have the same type, as long as the values are considered equal. 
            Corresponding columns must be of the same dtype.
            """
            if df1st.equals(df2nd): 
                #logprtstr = 'same DataFrame'
                #print(logprtstr); logger.info(logprtstr)  
                return False, None

            """
            # 1. df.empty ，这是DataFrame内置的属性，可以看到虽然调用简单，但他是最耗时的
            # 2. len(df)==0 ，这是通过Python内置len方法判断 DataFrame 的行数，相对来说速度比较快，是第1种的3倍
            # 3. len(df.index)==0 ，这是判断 DataFrame 的行索引的值数量，这已经到达纳秒级别了，是其中最快的方式当然，
            # 如果不是非常密集的调用，那么选哪种都无所谓。当你需要对程序进行性能调优时，就可以考虑选用上述的方式2或3。
  
            DateFrame.copy(deep=True) 
            data=DataFrame.copy(deep=False) 
            复制object的索引和数据

            当deep=True时(默认), 会创建一个新的对象进行拷贝. 修改这份拷贝不会对原有对象产生影响.
            当deep=False时, 新的对象只是原有对象的references. 任何对新对象的改变都会影响到原有对象
            """

            #合并二个不同的Klines DataFrame
            #print('@@@@@@@@@@@@@@@@@@@@@@@id df1st before merge: %s'%id(df1st))
            dfmerged = pd.merge(df1st, df2nd, how='right')   # on='time', how='right') 
            #dfmerged = df1st.append(df2nd)    
            #dfmerged.drop_duplicates(inplace = True)
            #print('@@@@@@@@@@@@@@@@@@@@@@@id df1st after merge: %s'%id(df1st))
            
            return True, dfmerged
        except Exception as e:
            logger.warning('_compare_merge_klines_dataframe() exception: %r'% e)
            return False, None

 
    # ----------------------------------------------------------------------
    # 把K线数据字节流转成Klines结构体属性字段列表
    def _convert_klines_to_datalist(self, symbol, kline_type, klines_rawdata)->list:
        try:
            kline_list_des = []
            count = int(len(klines_rawdata) / sizeof(RCV_KLINE)) # 计算多少根K线

            for i in range(count):
                klinebody = RCV_KLINE()
                klinebody.decode(klines_rawdata)                      #字节流 转成 ctypes结构体
                klines_rawdata = klines_rawdata[sizeof(RCV_KLINE) : ]  #删除一个结构体
                fieldval=[i, symbol, kline_type] # 放入K线序列号id，从0开始，指定证券代码, K线类型
                
                for field in fieldsOfKline:
                    if field == 'time': #localize the timestamp to local datetime
                        val = time.localtime(klinebody.time)
                        val = time.strftime('%y-%m-%d %H:%M:%S', val)
                    else:
                        val = getattr(klinebody, field)
                    fieldval.append(val)
                    #fieldval.append(getattr(klinebody, field))
                kline_list_des.append(fieldval)

        except Exception as e:
            logger.warning('_convert_klines_to_datalist() exception: %r'% e)
        finally:
            return kline_list_des

    # ----------------------------------------------------------------------
    def _compare_update_klines_datalist(self, kline_list_src, kline_list_des)-> list:
        # todo:
        return kline_list_des

    

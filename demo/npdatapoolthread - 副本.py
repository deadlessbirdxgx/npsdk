#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import selectors
from websocket import create_connection, ABNF
from multiprocessing import Process, JoinableQueue
import threading
import configparser
import os
import signal
import time
import struct
import ctypes
import inspect
import psutil
import codecs
from urllib import parse

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
class NpWebsocketClient(object): 
    def __init__(self, playmode = NPSDKAPI_PLAYMODE_NPWEBSOCKET, exePath = ".\\网际风\\"):

        super().__init__()
        
        #os.environ["CUDA_DEVICES_ORDER"]   = "PCI_BUS_IS"
        #os.environ["CUDA_VISIBLE_DEVICES"] = '0'  #表示调用0块GPU
        self.m_ws   = None
        self.m_connected = False
        self.m_url  = "ws://127.0.0.1:39398/"
        #websocket.enableTrace(True)

        file = exePath + "用户\\配置文件.ini"

        config = configparser.ConfigParser() # 创建配置文件对象
        config.read(file, encoding='utf-16') # 读取文件
        ip   = config.get('第三方调用', '连接地址01')
        port = config.get('第三方调用', '端口01')
        self.m_url = 'ws://' + ip +':' + port

        self._playmode = playmode
        if self._playmode != NPSDKAPI_PLAYMODE_NPWEBSOCKET and self._playmode != NPSDKAPI_PLAYMODE_NPBACKTEST:
            self._playmode = NPSDKAPI_PLAYMODE_NPWEBSOCKET

        """
        # 数据包回放Websocket Server
        """
        if self._playmode == NPSDKAPI_PLAYMODE_NPBACKTEST:
            self.m_url  = "ws://127.0.0.1:6889"  # backtestserver

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
                #如果策略程序正常循环运行，这时网际风数据接口程序（网际风.exe异常退出或者手工退出），再发初始化命令
                #后续再收到的数据是否会影响策略程序？？？？？？？
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
            return False

    def open(self):
        logprtstr = "开始连接数据接口......"
        print(logprtstr); logger.info(logprtstr) 

        result = self.connect()
        return result  

    def close(self):
        logprtstr = "关闭数据接口"
        print(logprtstr); logger.warning(logprtstr)

        self.m_connected = False
        self.m_ws.close() # 关闭websocket
        self.m_ws = None

    #发送基于websocket的数据请求命令
    def send_command_over_ws(self, ask):
        if self.m_connected == False:   
            logprtstr = "数据接口连接已断开(s)"
            logger.warning(logprtstr)
            #print(logprtstr); 
            return

        try:
            ask = "股票数据?" + ask + "&版本=20210310"
            #logger.info('wscmd: '+ask)
            self.m_ws.send(ask)       
            if self._playmode == NPSDKAPI_PLAYMODE_NPBACKTEST:
                time.sleep(1)     
        except Exception as e:
            logprtstr = 'send_command_over_ws() exception: %r'% e
            print(logprtstr); logger.warning(logprtstr)

            self.close()

    # return: (bool, bytes)
    def receive_data_over_ws(self):
        if self.m_connected == False:   
            logprtstr = "数据接口连接已断开(r)"
            logger.warning(logprtstr)
            #print(logprtstr); 
            return False, b''

        try:
             # 网际风返回的是bytes, 数据包回测返回的是str
            msg = self.m_ws.recv() 

            """
            # operation code values.
            ABNF.OPCODE_CONT = 0x0
            ABNF.OPCODE_TEXT = 0x1
            ABNF.OPCODE_BINARY = 0x2
            ABNF.OPCODE_CLOSE = 0x8
            ABNF.OPCODE_PING = 0x9
            ABNF.OPCODE_PONG = 0xa

            resp_opcode, msg = self.m_ws.recv_data()
            """
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
            logprtstr = 'receive_data_over_ws() exception: %r'% e
            print(logprtstr); logger.warning(logprtstr)

            self.close()
            return False, b''
        """
        finally:
            self.m_connected = False
            self.m_ws.close()
            return False, b''
        """

    def send_init_commands(self):

        init = "类型=初始化&编码=unicode"  # unicode 比 utf-8 快很多
        self.send_command_over_ws(init)
        logger.info('send ws command: %s' % init)

        market = "类型=市场代码表"
        self.send_command_over_ws(market)
        logger.info('send ws command: %s' % market)

        #当天数据：0 不包括当天数据；
        #格式：0 结构体(默认)、1 Json
        #setPqram  = "类型=K线参数&当天数据=1&复权=0&格式=0&版本=OEM_Ver2020"  #按约定格式传回K线数据
        setPqram = "类型=K线参数&当天数据=1&复权=0&格式=0"  # 按约定格式传回K线数据
        self.send_command_over_ws(setPqram)   
        logger.info('send ws command: %s' % setPqram)

class NpDataPoolThread(threading.Thread): #Process

    
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
                                    # {                      [RCV_KLINE,RCV_KLINE,RCV_KLINE,]
                                    #   'SH600000':{'1分钟线':原始数据字节流,    
                                    #               '5分钟线':原始数据字节流, 
                                    #               ......
                                    #              },
                                    #   ......
                                    #   'SH600007':{'1分钟线':原始数据字节流,
                                    #               '日线':   原始数据字节流, 
                                    #               ......
                                    #              },
                                    # }
    glb_stk_klines_dict.clear()

    glb_wscommands_set = set()       # websocket commands set
    

    def __init__(self,  command_queue, response_queue, playmode, exePath = ".\\网际风\\"):

        super().__init__()

        """ 运行策略程序时，如果网际风.exe未运行，则启动网际风.exe
        # 1.在DOS窗口下，功能正常
        # 2.在Vscode IDE环境下，如果网际风.exe未运行，则启动网际风.exe，
        # 但是如果通过Vscode菜单停止策略程序的同时，网际风.exe也随之结束，有问题！
        """
        def proc_exist(process_name):
            pl = psutil.pids()
            for pid in pl:
                if psutil.Process(pid).name() == process_name:
                    return pid

        if isinstance(proc_exist(NPSDKAPI_NEZIP_EXEFILE),int):
            print('网际风数据接口程序正在运行: %s'%NPSDKAPI_NEZIP_EXEFILE)
        else:
            print('运行网际风数据接口程序: %s......'%NPSDKAPI_NEZIP_EXEFILE)
            os.popen(exePath + '\\' + NPSDKAPI_NEZIP_EXEFILE)
            time.sleep(2)
        
        self._playmode = playmode
        if self._playmode != NPSDKAPI_PLAYMODE_NPWEBSOCKET and self._playmode != NPSDKAPI_PLAYMODE_NPBACKTEST:
            self._playmode = NPSDKAPI_PLAYMODE_NPWEBSOCKET

        # 创建WebSocket Client
        logger.info('create internal websocket client...')
        self._NpWsClient = NpWebsocketClient(self._playmode)

        self._stopped_flag = True 

        self._command_queue = command_queue     # command queue for requesting data
        self._response_queue = response_queue   # response queue for sending data
        
        self._ws_message_queue = JoinableQueue()    # queue for data received from websocket  
        self._ws_command_queue = JoinableQueue()    # queue for command sent to websocket
        logger.info('get and create internal queues...')

        #self._timerthread_stopevent = threading.Event()
        self._processqueuethread_stopevent = threading.Event()
        self._sendwscommandthread_stopevent = threading.Event()
        #self._timerthread_stopevent.clear()
        self._processqueuethread_stopevent.clear()
        self._sendwscommandthread_stopevent.clear()

        logger.info('create internal threads...')
        #self._timerthread = threading.Thread(target=self._timer_thread, args=(self._timerthread_stopevent,))
        self._processqueuethread = threading.Thread(target=self._process_queue_thread, args=(self._processqueuethread_stopevent,))
        self._sendwscommandthread = threading.Thread(target=self._send_wscommand_thread, args=(self._sendwscommandthread_stopevent,))
        

    def start_me(self):

        # 启动Websoket Client连接数据接口
        result = self._NpWsClient.open()
        if result: # 连接数据接口成功
            pass

        # 启动队列处理子线程 
        #self._timerthread.start()  
        self._processqueuethread.start()        
        #self._sendwscommandthread.start()  
        self._stopped_flag = False 
 
        """ 主线程体 """
        while True:
            bret, message = self._NpWsClient.receive_data_over_ws()
            if bret:
                self.on_message(message)

            if not self._NpWsClient.m_connected: 
                self._NpWsClient.open()

            time.sleep(NPSDKAPI_THREAD_SLEEP_VAL)
        #print("websocket close and exit from the loop")

    def stop_me(self):

        self._stopped_flag = True #停止使用Queue发送消息

        self._NpWsClient.close()

        #self._timerthread_stopevent.set() # 停止该线程: 设置Event为True,使之从while loop中退出
        #self._timerthread.join()
        self._processqueuethread_stopevent.set() # 停止该线程: 设置Event为True,使之从while loop中退出
        self._processqueuethread.join()
        self._sendwscommandthread_stopevent.set() # 停止该线程: 设置Event为True,使之从while loop中退出
        self._sendwscommandthread.join()

        # 如果使用了Queue, Python会派生daemon性质的QueueFeederThread, 需要释放Queue
        self._ws_message_queue.join()
        self._ws_message_queue.close()
        self._ws_command_queue.join()
        self._ws_command_queue.close()
        #self._async_raise(self.sendwscommandthread.ident, SystemExit)
        logger.info("当前线程数量: %d  \r\n线程是：%s" % (len(threading.enumerate()), str(threading.enumerate())))

    def on_message(self, message): #实时全推数据处理量大，建议拷贝数据后，扔到队列去 其它线程去处理
        """ 
        把原始数据压入_ws_message_queue队列，留给子线程后续处理
        """

        # 纪录历史数据包，以备数据包回测用
        #with open('backtest0607-2.dat','ab') as fo:
        #    fo.write(message)
        #fo.close()

        #self._df_wsincoming_rawdata.loc[time.time()] = message
        #compression_opts = dict(method='zip', archive_name='out.csv')  
        #self._df_wsincoming_rawdata.to_csv('out.zip', index=False, compression=compression_opts) 

        self.put_ws_message_queue(message)

    """    
    向_ws_message_queue中压入从ws收到的数据
    返回：bool
    """
    def put_ws_message_queue(self, msg):  
        try:
            #with self.internal_lock:                
                if self._stopped_flag: 
                    return False 

                if not self._ws_message_queue.full():
                    self._ws_message_queue.put(msg) #put_nowait()
                    return True
                else:
                    return False
        except Exception as e:
            logger.warning('put_wsmessage_queue() exception: %r'% e)
            return False

    #停止线程函数
    def _async_raise(self, tid, exctype):
        """raises the exception, performs cleanup if needed"""
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        print("res:%s"% res)
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    """
    主线程入口函数：调用npdatapoolthread.start()即进入run()
    """
    def run(self):
        self.start_me()  



    """""""""""""""""""""""""""""""""""""""""""""
    子线程（队列处理线程）: 功能：
    1. 接收并分析来自websocket的业务数据, 
       更新内存数据池中的业务数据字典
    .....
    2. 接收并分析来自npsdkapi的命令
    """""""""""""""""""""""""""""""""""""""""""""
    def _process_queue_thread(self, stop_event):

        """    
        从_ws_message_queue中取出wsmessage
        返回：元组(bool, message)
        """
        def get_wsmessage_queue():   
            wsmessage = ''
            
            try:
                 #with self.internal_lock:    
                    if self._stopped_flag: 
                        return False, wsmessage 
                
                    if not self._ws_message_queue.empty():
                        wsmessage = self._ws_message_queue.get()  #get_nowait() 
                        #self._ws_message_queue.task_done()
                        return True, wsmessage  
                    else:
                        return False, wsmessage  
            except Exception as e:
                logger.warning('get_wsmessage_queue() exception: %r'% e)
                return False, wsmessage 


        """""""""""""""""""""""""""""""""""""""""""""    
        检查来自websocket的数据
        """""""""""""""""""""""""""""""""""""""""""""
        def check_queue_from_npdatapool():

            bret, wsmessage = get_wsmessage_queue()
            if bret: #成功取得一条message   
                
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
                
                wsheadinfo = "ws data head info:\r\n[oemVer:%s] x [type:%s] x [label:%s] x [name:%s] x [flag:%s] x [askId:%s] x [len:%s] x [count:%s]" \
                % (wshead.oemVer, wshead.type, wshead.label, wshead.name, wshead.flag, wshead.askId, wshead.len, wshead.count)
                #print(wsheadinfo)
                logger.info(wsheadinfo)

                """
                message = HEAD + BODY
                BODY: (可变长度)
                """
                wsbody = wsmessage
                wsbody = wsmessage[sizeof(OEM_DATA_HEAD) : ] # 取出 wsbody

                msgtype   = wshead.type            #消息类型
                #headsize   = sizeof(OEM_DATA_HEAD)  #100 字节

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
                        print(logprtstr); logger.error(logprtstr)
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

                    #以市场代码为key值 更新市场字典 
                    if not marketbody.mkId in self.glb_marketinfo_dict.keys():
                        self.glb_marketinfo_dict[marketbody.mkId] = [marketbody,{}]

                    for i in range(marketbody.num):
                        stkbody = OEM_STKINFO()
                        stkbody.decode(wsbody)               #字节流 转成 ctypes结构体
                        wsbody = wsbody[sizeof(OEM_STKINFO) : ]  #删除一个结构体

                        #print(stkbody.code, stkbody.market, stkbody.block, stkbody.label, stkbody.name)

                        self.glb_marketinfo_dict[marketbody.mkId][1][stkbody.label] = stkbody

                    #print(f'代码表 coast:{time.perf_counter() - t:.8f}s')

                
                #-"""实时行情数据处理"""-#
                elif msgtype == '实时数据':  
                    if len(wsbody) == 0 or len(wsbody) != wshead.count * sizeof(OEM_REPORT):
                        logprtstr = '错误消息: 接收到的实时行情消息体长度为0 或者 实际长度不等于count*sizeof(OEM_REPORT)'
                        print(logprtstr); logger.error(logprtstr)
                        return 

                    for i in range(wshead.count):
                        reportbody = OEM_REPORT()
                        reportbody.decode(wsbody)               #字节流 转成 ctypes结构体
                        wsbody = wsbody[sizeof(OEM_REPORT) : ]  #删除一个结构体
                        #print(reportbody.label, reportbody.name)
                        #print('wsbody len%s'% len(wsbody))

                        """
                        待加功能：在代码表里检查 reportbody.label 是否为有效证券代码
                        """
                        
                        #以证券代码为key值 更新实时行情字典 
                        self.glb_stk_report_dict[reportbody.label] = reportbody   
                        #print('更新实时行情数据字典: %s len of dict：%s'% (reportbody.label, len(glb_stk_report_dict)))

                        """
                        if reportbody.label == 'SH600000' :
                            tm = time.localtime(reportbody.time)
                            tmStr = time.strftime('%Y-%m-%d %H:%M:%S', tm)
                            print("接收到实时行情: %s(%s) : time:%s close : %0.2f" % (reportbody.label, reportbody.name, tmStr, reportbody.close))
                        """
                    #print('---------------len(report dict):%s'%len(glb_stk_report_dict))
                
                #-"""K线数据处理"""-#
                elif msgtype in NPSDKAPI_NEZIP_KTYPES or msgtype == NPSDKAPI_NEZIP_OEMTRACE: # '分笔'Ticks 3秒:

                    unitsize = sizeof(RCV_KLINE)
                    if msgtype == NPSDKAPI_NEZIP_OEMTRACE:
                        unitsize = sizeof(OEM_TRACE)

                    #logger.info(wsbody) 
                    if len(wsbody) == 0 or len(wsbody) != wshead.count * unitsize:
                        logprtstr = '错误消息: 接收到的K线数据消息体长度为0 或者 实际长度不等于count * unitsize'
                        print(logprtstr); logger.error(logprtstr)
                        return

                    symbol = wshead.label       #证券代码
                    klines_type = wshead.type   #Klines类型字符串

                    """
                    待加功能：在代码表里检查 wshead.label 是否为有效证券代码
                    """

                    #以证券代码为key值 更新K线数据一级字典。以K线类型为key值 更新K线数据二级字典。
                    if  symbol in self.glb_stk_klines_dict.keys():
                        self.glb_stk_klines_dict[symbol][klines_type] = wsbody 
                    else: # 该证券代码第一次传送K线数据
                        klines_dict = {}
                        klines_dict[klines_type] = wsbody
                        self.glb_stk_klines_dict[symbol] = klines_dict
                    
                    """
                    if wshead.label == 'SH600000' :
                        tm = time.localtime()
                        tmStr = time.strftime('%Y-%m-%d %H:%M:%S', tm)
                        print("接收到K线数据: %s(%s) type:%s: time:%s " % (wshead.label, wshead.name, klines_type, tmStr))
                    """

                elif msgtype == '分时' :
                    pass
                
                elif msgtype == '除权':
                    pass

                elif msgtype == '财务' :
                    pass

                elif msgtype == 'F10资料' :                 
                    pass


        """    
        从command_queue中取出命令
        返回：元组(bool, NpApiDataObj)
        """
        def get_command_queue():   
            npapidataobj = NpApiDataObj()
            
            try:
                #with self.internal_lock:                   
                    if self._stopped_flag: 
                        return False, npapidataobj 

                    if not self._command_queue.empty():
                        npapidataobj = self._command_queue.get()  #get_nowait() 
                        #self._command_queue.task_done()
                        return True, npapidataobj  
                    else:
                        return False, npapidataobj  
            except Exception as e:
                logger.warning('get_command_queue() exception: %r'% e)
                return False, npapidataobj 

        """    
        向response_queue中压入命令数据响应
        返回：bool
        """
        def put_response_queue(npapidataobj):  
            try:
                #with self.internal_lock:                      
                    if self._stopped_flag: 
                        return False 
             
                    if not self._response_queue.full():
                        self._response_queue.put_nowait(npapidataobj) #put_nowait()
                        return True
                    else:
                        return False
            except Exception as e:
                logger.warning('put_response_queue() exception: %r'% e)
                return False

        """    
        向ws_command_queue中压入wscommand命令
        返回：bool
        """
        def put_wscommands_queue(wscommands):  
            try:
                #with self.internal_lock:                      
                    if self._stopped_flag: 
                        return False 
             
                    if not self._ws_command_queue.full():
                        self._ws_command_queue.put(wscommands) #put_nowait()
                        return True
                    else:
                        return False
            except Exception as e:
                logger.warning('put_wscommands_queue() exception: %r'% e)
                return False

        """""""""""""""""""""""""""""""""""""""""""""    
        检查是否有NpSdkApi队列任务
        """""""""""""""""""""""""""""""""""""""""""""
        def check_queue_from_npsdkapi():

            bret, npapidataobj = get_command_queue()
            #print('datapool: cmd_queue(size:%s:%s) msg_queue(size:%s)'%(self._command_queue.qsize(), bret, self._ws_message_queue.qsize()))

            if bret: #成功取得一条命令
                app_id = npapidataobj.app_id

                # 监控队列长度，如果队列长度太大表示消息堆积
                #if self._ws_message_queue.qsize() > NPSDKAPI_QUEUESZIE_WARNING:
                #    logger.warning('(wsmsg_queue: %s) (wscmd_queue: %s) (response_queue: %s) (command_queue: %s)' \
                #        %(self._ws_message_queue.qsize(), self._ws_command_queue.qsize(), self._response_queue.qsize(), self._command_queue.qsize()))
                #print('>>>wsmsg_queue:%s wscmd_queue:%s response_queue:%s command_queue:%s'%(self._ws_message_queue.qsize(), self._ws_command_queue.qsize(), self._response_queue.qsize(), self._command_queue.qsize()))


                if app_id == NPSDKAPI_APPID_NPSDKAPI: #该消息为策略程序请求命令
                    
                    if npapidataobj.message_id == NPSDKAPI_MESSAGEID_GETQUOTE_REQ: 
                        """
                        获取指定代码的实时行情
                        """
                        bsuc, npapiobj = get_quote_remote(npapidataobj) 
                        
                        """
                        返回指定代码的实时行情数据
                        """
                        put_response_queue(npapiobj)

                   
                    elif npapidataobj.message_id == NPSDKAPI_MESSAGEID_GETKLINES_REQ: 
                        """
                        获取指定代码的K线数据
                        """
                        bsuc, npapiobj = get_kline_serial_remote(npapidataobj) 
                        
                        """
                        返回指定代码的K线数据
                        """
                        put_response_queue(npapiobj)

                       
                    elif npapidataobj.message_id == NPSDKAPI_MESSAGEID_WSCOMMAND_REQ: #向websocket server发送命令请求数据
                        
                 
                        logger.info('send ws command: %s' % npapidataobj.request_body)
                        self._NpWsClient.send_command_over_ws(npapidataobj.request_body)

                        self.glb_wscommands_set.add(npapidataobj.request_body) # 汇集websocket commands命令用set集合去除重复命令
                        #print('>>>', glb_wscommands_set)
                        #put_wscommands_queue(glb_wscommands_set)

                    else: #其它命令
                        pass

                else: # 没有收到来自 NPSDKAPI_APPID_NPSDKAPI 的命令
                    pass

        """    
        校验证券代码合法性
        返回：bool
        """
        def is_code_legal(codekey)->bool:  
            bfound = False
            for mkid in self.glb_marketinfo_dict.keys():
                for code in self.glb_marketinfo_dict[mkid][1].keys():
                    if code == codekey:
                        bfound = True
            return bfound

        """
        get_quote_remote函数体
        返回：元组（bool, NpApiDataObj）
        """
        def get_quote_remote(npapiobjsrc: NpApiDataObj):
            """ 
            在数据池里查找实时行情业务数据，
            """         
            npapiobjdes = NpApiDataObj()
            npapiobjdes.app_id = NPSDKAPI_APPID_NPSDKDATA
            npapiobjdes.message_id = NPSDKAPI_MESSAGEID_GETQUOTE_RES
            npapiobjdes.correlation_id = npapiobjsrc.correlation_id
            npapiobjdes.reply_to = npapiobjsrc.reply_to
            npapiobjdes.response_type = ''
            npapiobjdes.response_body = ''

            result = parse.urlparse(npapiobjsrc.request_body)
            query_dict = parse.parse_qs(result.path)
            codekey = query_dict.get('代码').pop()

            logger.info('send ws command: %s' % npapiobjsrc.request_body)
            self._NpWsClient.send_command_over_ws(npapiobjsrc.request_body)
            
            """
            # 校验 证券代码是否合法：
            """
            if not is_code_legal(codekey):
                logprtstr = "在市场代码表字典中没找到codekey：%s"% codekey
                logger.warning(logprtstr)
                npapiobjdes.response_body = logprtstr 
                npapiobjdes.response_type = NPSDKAPI_ERROR
                return False, npapiobjdes

            # 快速查找实时行情字典
            if  codekey in self.glb_stk_report_dict.keys(): 
                reportbody = self.glb_stk_report_dict[codekey]
                #print("found codekey:%s label: %s in 实时行情字典"% (codekey, reportbody.label))
                
                if reportbody:
                    #print("在实时行情字典中找到 %s 开盘价(%s) 最新价(%s)" % (reportbody.label, reportbody.open, reportbody.close))
                    #logger.info("在实时行情字典中找到codekey")
                    
                    npapiobjdes.response_body = reportbody.encode() # 把ctypes结构体转成字节流
                    npapiobjdes.response_type = NPSDKAPI_SUCCESS

                    return True, npapiobjdes 
                else:
                    logprtstr = "在实时行情字典中找到codekey：%s，但是对应的数据体为空"% codekey
                    logger.warning(logprtstr)
                    npapiobjdes.response_body = logprtstr 
                    npapiobjdes.response_type = NPSDKAPI_ERROR
                    return False, npapiobjdes
            
            else:
                logprtstr = "在实时行情字典中没找到codekey:%s "% codekey
                logger.warning(logprtstr)
                npapiobjdes.response_body = logprtstr
                npapiobjdes.response_type = NPSDKAPI_ERROR  
                return False, npapiobjdes

        """
        get_kline_serial_remote函数体
        返回：元组（bool, NpApiDataObj）
        """
        def get_kline_serial_remote(npapiobjsrc: NpApiDataObj):
            """ 
            在数据池里查找K线业务数据，
            """         
            npapiobjdes = NpApiDataObj()
            npapiobjdes.app_id = NPSDKAPI_APPID_NPSDKDATA
            npapiobjdes.message_id = NPSDKAPI_MESSAGEID_GETKLINES_RES
            npapiobjdes.correlation_id = npapiobjsrc.correlation_id
            npapiobjdes.reply_to = npapiobjsrc.reply_to
            npapiobjdes.response_type = ''
            npapiobjdes.response_body = ''
            npapiobjdes.request_body = npapiobjsrc.request_body # 保存并返回原request_body

            result = parse.urlparse(npapiobjsrc.request_body)
            query_dict = parse.parse_qs(result.path)
            codekey = query_dict.get('代码').pop()
            klines_type = query_dict.get('类型').pop()
            data_length = query_dict.get('数量').pop()

            logger.info('send ws command: %s' % npapiobjsrc.request_body)
            self._NpWsClient.send_command_over_ws(npapiobjsrc.request_body)
                           
            """
            # 校验 证券代码是否合法：
            """
            if not is_code_legal(codekey):
                logprtstr = "在市场代码表字典中没找到codekey：%s"% codekey
                logger.warning(logprtstr)
                npapiobjdes.response_body = logprtstr 
                npapiobjdes.response_type = NPSDKAPI_ERROR
                return False, npapiobjdes

            # 快速查找K线字典: 在一级字典找到指定代码且同时在二级字典找到K线类别
            if  codekey in self.glb_stk_klines_dict.keys():
                if klines_type in self.glb_stk_klines_dict[codekey].keys():
                    kline_rawdata = self.glb_stk_klines_dict[codekey][klines_type]
                    
                    if kline_rawdata:
                        npapiobjdes.response_body = kline_rawdata
                        npapiobjdes.response_type = NPSDKAPI_SUCCESS
                        return True, npapiobjdes 
                    else:
                        logprtstr = "在K线数据字典中找到(codekey %s:klinestype %s), 但是对应的数据体为空"% (codekey, klines_type)
                        logger.warning(logprtstr)
                        npapiobjdes.response_body = logprtstr
                        npapiobjdes.response_type = NPSDKAPI_ERROR
                        return False, npapiobjdes
                else:
                    logprtstr = "在K线数据字典中找到codekey %s 但没找到klinestype %s"% (codekey, klines_type)
                    logger.warning(logprtstr)
                    npapiobjdes.response_body = logprtstr
                    npapiobjdes.response_type = NPSDKAPI_ERROR
                    return False, npapiobjdes
            else:
                logprtstr = "在K线数据字典中没找到codekey:%s "% codekey
                logger.warning(logprtstr)
                npapiobjdes.response_body = logprtstr
                npapiobjdes.response_type = NPSDKAPI_ERROR  
                return False, npapiobjdes

        """
        子线程（队列处理线程）线程体
        """
        while not stop_event.is_set():

            # check queue if has ws message from npdata and update data in memory dict
            check_queue_from_npdatapool()

            # check queue from npsdkapi if has data request
            check_queue_from_npsdkapi()

            """
            由于websocket的数据消息可能会很多, check_queue_from_npdatapool()函数会占有大量时间片
            为了尽快处理来自npsdkapi发来的_command_queue任务，sleep值不能太大
            """
            time.sleep(NPSDKAPI_THREAD_SLEEP_VAL)  
        #print('process_queue_thread exit from the loop')


    """
    子线程（定时器线程）: 功能：
    1. 提供设置定时功能
    """
    """
    def _timer_thread(self, stop_event): 
        
        #以超时阀值(秒)为key的定时器任务字典，value为申请时的系统时间戳
        timers_dict = { 
            # 1 : 0,
            # 3 : 0,
            # 5 : 0,
        }

        # 申请一个定时任务
        def _addtimer(timerkey, timervalue): 
            timers_dict[timerkey] = timervalue
        
        # 删除一个定时任务
        def _deltimer(timerkey): 
            timers_dict.pop(timerkey)

        _addtimer(1, time.time())
        _addtimer(3, time.time())
        _addtimer(10, time.time())
        #子线程（定时器线程）线程体
        while not stop_event.is_set():
            #scan the timer dict
            for tk in timers_dict.keys():
                if time.time() - timers_dict[tk] >= tk:
                    print('timer on:',tk)
                    _deltimer(tk)
                    _addtimer(tk,time.time())
                    break
            
            time.sleep(NPSDKAPI_THREAD_SLEEP_VAL)  # sleep(x) x值保持跟processqueuethread一致
    """

    """
    子线程（基于websocket的数据请求命令线程）: 功能：
    1. 根据业务类型设置定时器任务发送数据请求命令 （避免频繁发送命令）
    2. 发送基于websocket的数据请求命令
    """
    def _send_wscommand_thread(self, stop_event): 

        NPSDKAPI_TIMER_HALFSEC = 0.5
        NPSDKAPI_TIMER_1SECS = 1
        NPSDKAPI_TIMER_3SECS = 3
        #.....可以追加任意定时器超时值

        #以超时阀值(秒)为key的定时器任务字典，value为申请时的系统时间戳
        _timers_dict = { 
            # 0.5 : time_set,
            # 1 : time_set,
            # 3 : time_set,
        }

        # 以命令为key的命令任务字典，value为超时阀值
        _commands_dict = { 
            # cmd1: 0.5
            # cmd2: 3
            # cmd3: 1
        }

        # 申请一个定时任务
        def _addtimer(timerkey, timervalue): 
            _timers_dict[timerkey] = timervalue
        
        # 删除一个定时任务
        def _deltimer(timerkey): 
            _timers_dict.pop(timerkey)

        # 扫描定时任务, 返回超时的定时器
        def _scantimer(): 
            for tk in _timers_dict.keys():
                if time.time() - _timers_dict[tk] >= tk:
                    #print('timer on:',tk)
                    _deltimer(tk)
                    _addtimer(tk,time.time())
                    return tk
            return None

        """    
        从_ws_command_queue中取出wscommand
        返回：元组(bool, command)
        """
        def get_wscommands_queue()->set:   
            _wscommands = set()
            
            try:
                #with self.internal_lock:    
                    if self._stopped_flag: 
                        return False, _wscommands 
                
                    if not self._ws_command_queue.empty():
                        _wscommands = self._ws_command_queue.get()  #get_nowait() 
                        #self._ws_command_queue.task_done()
                        return True, _wscommands  
                    else:
                        return False, _wscommands  
            except Exception as e:
                logger.warning('get_wscommands_queue() exception: %r'% e)
                return False, _wscommands 


        # 分析处理client发来的业务命令
        def parse_command_pro(command):
            # 解析command参数
            result = parse.urlparse(command)
            query_dict = parse.parse_qs(result.path)
            type = query_dict.get('类型').pop()

            # 代码=SH600007&类型=实时数据
            if type == '实时数据':
                #symbol =  query_dict.get('代码').pop()
                #print('实时行情请求命令: %s'%symbol)
                _commands_dict[command] = NPSDKAPI_TIMER_3SECS

            # 代码=SH600000&类型=1分钟线&数量=2000
            elif type in NPSDKAPI_NEZIP_KTYPES or type == NPSDKAPI_NEZIP_OEMTRACE:
                #symbol =  query_dict.get('代码').pop()
                #datalength =  query_dict.get('数量').pop() 
                #print('K线数据请求命令: %s : %s : %s'%(symbol, type, datalength))
                _commands_dict[command] = NPSDKAPI_TIMER_3SECS

            else:
                #print('其它命令：', command)
                pass

        
        #预设几个值定时器
        _addtimer(NPSDKAPI_TIMER_HALFSEC, time.time())
        _addtimer(NPSDKAPI_TIMER_1SECS, time.time())
        _addtimer(NPSDKAPI_TIMER_3SECS, time.time())
        #.....可以追加任意定时器

        #子线程（队列处理线程）线程体
        while not stop_event.is_set():
            
            bret, wscommands = get_wscommands_queue() #把wscommand加入集合达到去重的目的
            if bret: #成功取得一个wscommand命令集合
                for cmd in wscommands:
                    parse_command_pro(cmd)
 
            #scan the timers dict
            interval = _scantimer()
            if not interval: continue # 无定时器超时

            #print(_commands_dict)

            for cmd in _commands_dict.keys():
                if _commands_dict[cmd] == interval:
                    #print(cmd, interval)
                    self._NpWsClient.send_command_over_ws(cmd)
                    logger.info('send ws command: %s' % cmd)
                    

            time.sleep(NPSDKAPI_THREAD_SLEEP_VAL)  # sleep(x) x值保持跟processqueuethread一致


 
    






        

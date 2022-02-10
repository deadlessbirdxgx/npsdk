#!/usr/bin/env python
#  -*- coding: utf-8 -*-
"""
NpSdkApi接口的PYTHON封装
"""
__author__ = 'nfjd'

from multiprocessing import Process, JoinableQueue
import os
import sys
import threading
import asyncio
import concurrent.futures
import functools 
import time
from datetime import datetime
from typing import Union, List, Any, Optional
import numpy as np
import pandas as pd
from urllib import parse
import configparser
import psutil
from blinker import signal
from ctypes import *

from npsdkobjs import *
from Stockdrv import *
from npdatapoolthread import NpDataPoolThread
from __version__ import __version__
import logging

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
#logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
formatter = logging.Formatter(LOG_FORMAT)

handler = logging.FileHandler("npsdkapi.log")
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)
logger.addHandler(handler)
#logger.addHandler(console)

class NpSdkApi(object):
    """
    NpSdkApi接口及数据管理类
    """
    internal_lock = threading.Lock()

    def __init__(self, auth: Optional[str] = None, debug: bool = False, playmode: str = NPSDKAPI_PLAYMODE_NPWEBSOCKET) -> None:
        doc=""" 
            创建NpSdkApi接口实例      
        """
        super().__init__()

        #os.environ["CUDA_DEVICES_ORDER"]   = "PCI_BUS_IS"
        #os.environ["CUDA_VISIBLE_DEVICES"] = '0'  #表示调用0块GPU
        #user32 = windll.LoadLibrary('user32.dll')  
        #user32.MessageBoxA(0, str.encode('ctypes is so smart!'), str.encode('ctypes'), 0) 
        #mem = psutil.virtual_memory()
        #logger.info("Memory info: total: %s used: %s free: %s"%(mem.total, mem.used, mem.free))

        "Set the interpreter’s thread switch interval (in seconds)"
        #si = sys.getswitchinterval()  # default : 0.005s
        #print('switchinterval: %s '%si)
        #sys.setswitchinterval(0.005)

        """
        从npsdk.ini配置文件中读取网际风数据接口程序(网际风.exe)的工作目录
        """
        try:
            file = "npsdk.ini"
            config = configparser.ConfigParser()
            config.read(file, encoding='utf-8')
            self._nezip_exe_path = config.get('网际风目录', 'nezipdir')
        except Exception as e:
            print('\r\n在配置文件中查找 网际风目录 参数异常\r\n请在npsdk.ini文件中正确配置参数 网际风目录 %r \r\n'%e) 
            return

        """
        如果网际风数据接口程序(网际风.exe)未运行，则从NpSdk中启动它e
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
            """
            在Python中启动外部进程的几种方法
            """
            os.popen(self._nezip_exe_path + '\\' + NPSDKAPI_NEZIP_EXEFILE) #ok

            #os.system(self._nezip_exe_path + '\\' + NPSDKAPI_NEZIP_EXEFILE)    #nok
            #subprocess.Popen(self._nezip_exe_path + '\\' + NPSDKAPI_NEZIP_EXEFILE) #nok
            time.sleep(1)


        # 内部关键数据

        # 记录历史命令请求 以及 最后一次的数据响应 
        self._npsdkapi_cmd_request_response = {
            NPSDKAPI_MESSAGEID_GETQUOTE_REQ: {}, #      'get_quote':{   证券代码:[命令参数,实时行情quote,读取数据成功与否,策略程序实时行情quote]}  二层字典 
                                                 #      'get_quote':{   'SH600000':[cmdpara,quote,bsuc,quote1st], 
                                                 #                      'SH600007':[cmdpara,quote,bsuc,quote1st],
                                                 #                      ..........
                                                 #                  }
                                                 #    
            NPSDKAPI_MESSAGEID_GETKLINES_REQ: {}, #     'get_klines':{  证券代码:{K线类别：[命令参数,K线数据df,读取数据成功与否,策略程序K线数据df,]}} 三层字典 
                                                #       "get_klines":{  "SH600000": {'1分钟线':[cmdpara,klinesdf,bsuc,klinesdf1st],'5分钟线':[cmdpara,klinesdf,bsuc,klinesdf1st],...,},
                                                #                       "SH600001": {'1分钟线':[cmdpara,klinesdf,bsuc,klinesdf1st],'5分钟线':[cmdpara,klinesdf,bsuc,klinesdf1st],...,},
                                                #                       "SH600002": {'1分钟线':[cmdpara,klinesdf,bsuc,klinesdf1st],'5分钟线':[cmdpara,klinesdf,bsuc,klinesdf1st],...,},
                                                #                       ......
                                                #                    }
        }   


        self._npsdk_debug = debug

        self._npsdk_playmode = playmode

        self._run1st_flag = True

        """创建一个新的event loop"""
        #self._loop = asyncio.get_event_loop()

        """
        # 用户使用npsdk免责提示
        """
        print(NPSDKAPI_LEGAL_TIPS) 

        self.open() #启动datapoolthread
 
    def open(self):

        """创建数据信号，绑定回调函数
        self.data_received_signal = signal('data-received')
 
        self.data_received_signal.connect(self._codes_received_cb, sender = NPSDKAPI_SIGNAL_DATA_CODES)
        self.data_received_signal.connect(self._quotes_received_cb, sender = NPSDKAPI_SIGNAL_DATA_QUOTES)
        self.data_received_signal.connect(self._klines_received_cb, sender = NPSDKAPI_SIGNAL_DATA_KLINES)
        """

        # 创建数据池线程
        self._npdatapoolthread = NpDataPoolThread(self.internal_lock, self._npsdk_debug, self._npsdk_playmode, self._nezip_exe_path)
        #self._npdatapoolthread = NpDataPoolThread(self.data_received_signal, self.internal_lock, self._npsdk_playmode, self._nezip_exe_path)
        self._npdatapoolthread.setDaemon(True)
        self._npdatapoolthread.start()
        #self._npdatapoolthread.join()
        logger.info('create & start npdatapool thread')  

    def close(self) -> None:

        """断开数据信号"""
        #self.data_received_signal.disconnect()

        #self._loop.stop()
        #self._loop.close()

        """
        关闭NpSdkApi接口实例并释放相应资源
        """
        self._npdatapoolthread.stop_me()         
        
        """
        # 退出
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
        """

    """ 接收到数据信号 回调函数
    def _codes_received_cb(self, signum) -> None:
        #print('收到数据更新......%s'%signum)
        pass

    def _quotes_received_cb(self, signum) -> None:
        #print('收到数据更新......%s'%signum)

        self._run_quotes_tasks()   

    def _klines_received_cb(self, signum) -> None:
        #print('收到数据更新......%s'%signum)

        self._run_klines_tasks()
    """

    """    
    打印市场代码表
    返回：None
    """
    def print_market_code_info(self)->None:  

        #threadindex = random.randint(0,NPSDKAPI_NUM_OF_NPDATAPOOLTHREAD-1)

        filename = "市场代码表.txt"
        path = filename  # 文件路径
        if os.path.exists(path):  # 如果文件存在
            # 删除文件，可使用以下两种方法。
            os.remove(path)
            #os.unlink(path)

        logger_mci = logging.getLogger()
        logger_mci.setLevel(level = logging.INFO)
        handler_mci = logging.FileHandler("市场代码表.txt")
        handler_mci.setLevel(logging.INFO)
        #handler_mci.setFormatter(formatter)
        logger_mci.addHandler(handler_mci)

        
        label_str = ''
        label_cnt = 0
        
        for mkid in self._npdatapoolthread.glb_marketinfo_dict.keys():
            mk = OEM_MARKETINFO()
            mk = self._npdatapoolthread.glb_marketinfo_dict[mkid][0]
            logger_mci.info('\r\n')
            logger_mci.info('市场ID: %s 市场名称: %s 交易时段个数: %s 数据日期: %s 该市场的证券个数: %s' \
                                        %(mk.mkId, mk.name, mk.tmCount, mk.date, mk.num))
            logger_mci.info('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')

            for code in self._npdatapoolthread.glb_marketinfo_dict[mkid][1].keys():
                stk = OEM_STKINFO()
                stk = self._npdatapoolthread.glb_marketinfo_dict[mkid][1][code]
                logger_mci.info('代码序号: %s 所属市场: %s 代码: %s 名称: %s 是否指数: %s 是否股票: %s 拼音: %s' \
                                        %(stk.code, stk.market, stk.label, stk.name, stk.isIndex, stk.isStock, stk.pinYin))
        
                
                label_cnt += 1
                if label_cnt % 10 == 0:
                    label_str += '\r\n'
                else:
                    label_str += '\"' + stk.label + '\",' 
                
            
            logger_mci.info(label_str)
            

    """    
    校验证券代码合法性
    返回：bool
    """
    def is_code_legal(self, codekey)->bool:  
        bfound = False
        for mkid in self._npdatapoolthread.glb_marketinfo_dict.keys():
            if codekey in self._npdatapoolthread.glb_marketinfo_dict[mkid][1].keys():
                bfound = True
        return bfound

    """
    fetch_quote_remote函数体
    返回：元组（bool, OEM_REPORT）
    """
    def fetch_quote_remote(self, symbol, command):

        #threadindex = random.randint(0,NPSDKAPI_NUM_OF_NPDATAPOOLTHREAD-1)

        """ 
        在数据池里查找实时行情业务数据，
        """         
        quote = OEM_REPORT()
        quote.label = symbol  

        """
        实时行情无需发送数据请求命令， 网际风数据接口在数据有变化时会主动推送过来
        """
        self._npdatapoolthread.register_command(command)

        """
        # 校验 证券代码是否合法：
        """
        if not self.is_code_legal(symbol):
            logprtstr = "在市场代码表中没找到 symbol：%s (市场代码表下载还未完成 或者 证券代码参数输入错误)"% symbol
            logger.warning(logprtstr)
            #if self._npsdk_debug: print(logprtstr)
            return False, quote

        # 快速查找实时行情字典
        if symbol in self._npdatapoolthread.glb_stk_report_dict.keys(): 

            quote_in_datapool = self._npdatapoolthread.glb_stk_report_dict[symbol]
            
            if quote_in_datapool:
                #logprtstr = "在实时行情字典中找到数据 symbol：%s"% symbol
                #logger.info(logprtstr)
                return True, quote_in_datapool 
            else:
                logprtstr = "在实时行情字典中找到 symbol：%s，但是对应的数据体为空"% symbol
                logger.warning(logprtstr)
                return False, quote
        
        else:
            logprtstr = "在实时行情字典中没找到 symbol:%s "% symbol
            logger.warning(logprtstr)
            return False, quote

    """
    fetch_klines_remote函数体
    返回：元组（bool, dataframe)
    """
    def fetch_klines_remote(self, symbol, klinetype, command):

        #threadindex = random.randint(0,NPSDKAPI_NUM_OF_NPDATAPOOLTHREAD-1)

        """ 
        在数据池里查找K线业务数据，
        """         

        """
        #创建一个空 klines DataFrame  
        """
        #创建空DataFrame:行索引默认从0开始，列索引里加入K线序列号id，也是从0开始, 指定证券代码, K线类型
        df = self._npdatapoolthread._convert_klines_to_dataframe(symbol, klinetype, b'') 

        self._npdatapoolthread.register_command(command)

        """
        # 校验 证券代码是否合法：
        """
        if not self.is_code_legal(symbol):
            logprtstr = "在市场代码表中没找到 symbol：%s (市场代码表下载还未完成 或者 证券代码参数输入错误)"% symbol
            logger.warning(logprtstr)
            #if self._npsdk_debug: print(logprtstr)
            return False, df

        # 快速查找K线字典: 在一级字典找到指定代码且同时在二级字典找到K线类别
        if symbol in self._npdatapoolthread.glb_stk_klines_dict.keys():
            if klinetype in self._npdatapoolthread.glb_stk_klines_dict[symbol].keys():
                df_in_datapool = self._npdatapoolthread.glb_stk_klines_dict[symbol][klinetype][1]
                
                if len(df_in_datapool.index) != 0:
                    #logprtstr = "在K线数据字典中找到数据 symbol：%s klinetype：%s"% (symbol, klinetype)
                    #logger.info(logprtstr)
                    return True, df_in_datapool 
                else:
                    logprtstr = "在K线数据字典中找到 (symbol %s:klinetype %s), 但是对应的数据体为空"% (symbol, klinetype)
                    logger.warning(logprtstr)
                    return False, df
            else:
                logprtstr = "在K线数据字典中找到 symbol %s 但没找到klinestype %s"% (symbol, klinetype)
                logger.warning(logprtstr)
                return False, df
        else:
            logprtstr = "在K线数据字典中没找到 symbol:%s "% symbol
            logger.warning(logprtstr)
            return False, df

    # ----------------------------------------------------------------------
    def fetch_quote(self, symbol: str) -> OEM_REPORT:
        doc="""
        获取指定代码的盘口行情.

        Args:
            symbol (str): 指定证券代码。可用的交易所代码如下：
                         * CFFEX: 中金所
                         * SHFE: 上期所
                         * DCE: 大商所
                         * CZCE: 郑商所
                         * INE: 能源交易所(原油)

        Returns: 
            :py:class:`~npsdk.Stockdrv.OEM_REPORT`: 返回一个盘口行情引用. 
            其内容将在 :py:meth:`~npsdk.api.NpSdkApi.wait_update` 时更新.

            注意: 在 npsdk 还没有收到行情数据包时, 此对象中各项内容为 None 或 0

        Example::

            # 获取 SH600000 的报价
            from nqsdk import NpSdkApi

            api = NpSdkApi()
            quote = api.fetch_quote("SH600000")
            while True:
                api.refreshing()
                if api.is_updated(quote, 'close'):
                    print(quote.close)

            # 预计的输出是这样的:
            0.0
            10.03
            10.03
            ...
        """

        command = '代码=' + symbol + '&类型=实时数据'  # 类型不要写成'实时行情'

        """
        #t = time.perf_counter()
        if self.internal_lock.acquire():
            bsuc, quote = self.fetch_quote_remote(symbol, command)
            self.internal_lock.release()
        else:
            logger.info('npsdkapi acquire thread_lock failed')
        #print(f'fetch_quote_remote() coast:{time.perf_counter() - t:.8f}s')
        """

        bsuc, quote = self.fetch_quote_remote(symbol, command)

        #记录本次执行结果更新, ，以便在is_updated中做对比
        #列表第1项记录本次命令参数，第2项记录结果数据，第3项记录本次命令成功与否, 第4项纪录第一次fetch_quote(指定证券代码symbol)的结果df obj
        if not symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ].keys(): 
            self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol] = [[symbol], quote, bsuc, quote]
        else:
            lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol][3] 
            self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol] = [[symbol], quote, bsuc, lastobj]

        # 不管是成功还是失败，每次均返回quote对象
        return quote


    # ----------------------------------------------------------------------
    def fetch_klines(self, symbol: str, kline_type: str = "1分钟线", data_length: int = 1000) -> pd.DataFrame:
        doc="""
        获取k线序列数据

        请求指定证券代码及周期的K线数据. 序列数据会随着时间推进自动更新 （包括分笔）

        Args:
            symbol (str): 指定证券代码
                * str: 一个证券代码
                * list of str: 证券代码列表 （一次提取多个证券的K线并根据相同的时间向第一个证券代码对齐) (暂时不支持）

            kline_type (str): K线类型字符串, 取值: 
                "1分钟线", "5分钟线", "15分钟线", "30分钟线", "60分钟线", "日线", "周线", "月线", "季线", "年线", "多日线" 
                或者 “分笔”

            data_length (int): 需要获取的序列长度。默认200根, 返回的K线序列数据是从当前最新一根K线开始往回取data_length根。\
            每个序列最大支持请求 8964 ?(待测试确认) 个数据

         Returns::(K线：不含分笔)
            pandas.DataFrame: 本函数总是返回一个 pandas.DataFrame 实例. 行数=data_length, 包含以下列:

            * id:       (k线序列号，从0开始 )    * DataFrame里 新增字段
            * label:    (证券代码)              * DataFrame里 新增字段
            * ktype:    (K线类型)               * DataFrame里 新增字段

            * time:     (K线起点时间戳UTC，单位:秒数)   *** time字段是唯一业务索引 ***
            * open:     (K线起始时刻的最新价)
            * high:     (K线时间范围内的最高价)
            * low:      (K线时间范围内的最低价)
            * close:    (K线结束时刻的最新价)
            * volume:   (K线时间范围内的成交量)
            * amount:   (K线时间范围内的成交额)
            * temp:     (保留)

        Example1:

            # 获取 SH600000 的1分钟线
            from npsdk import NpSdkApi

            api = NpSdkApi()
            klines = api.fetch_klines("SH600000", "1分钟线", 60)
            while True:
                api.refreshing()
                if api.is_updated(klines.iloc[-1], 'close'):
                    print(klines.iloc[-1].close)

            # 预计的输出是这样的:
            10.22
            10.22
            10.22
            ...
        """

        #是否正确的K线类型字符串格式
        if not kline_type in NPSDKAPI_NEZIP_KTYPES and kline_type != NPSDKAPI_NEZIP_OEMTRACE: # 参数2为'分笔'可以继续
            raise Exception("K线类型 %s 错误, 请检查K线类型是否填写正确" % (kline_type))

        data_length = int(data_length)
        if data_length <= 0 and kline_type != NPSDKAPI_NEZIP_OEMTRACE:
            raise Exception("K线数据序列长度 %d 错误, 请检查序列长度是否填写正确" % (data_length))
        if data_length > 8964:
            data_length = 8964
    
        command = '代码=%s&类型=%s&数量=%s'%(symbol, kline_type, data_length) # 包括 '分笔'

        """
        if self.internal_lock.acquire():
            bsuc, df_kline = self.fetch_klines_remote(symbol, kline_type, command)
            self.internal_lock.release()
        else:
            logger.info('npsdkapi acquire thread_lock failed')
        """

        bsuc, df_kline = self.fetch_klines_remote(symbol, kline_type, command)
            
        #记录本次执行结果更新, ，以便在is_updated中做对比
        #列表第1项记录本次命令参数，第2项记录结果数据，第3项记录本次命令成功与否, 第四项纪录第一次fetch_klines(指定证券代码symbol)的结果df obj
        #以K线类型为key值 更新三级字典,以证券代码为key值 更新二级字典
        condition1 = symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ].keys()
        if condition1:
            condition2 = kline_type in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol].keys() 

        if condition1:
            if condition2:
                #证券代码在二级字典并且K线类型在三级字典, 修改原纪录list
                lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][3]
                self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type] = \
                                        [[symbol, kline_type, data_length], df_kline, bsuc, lastobj]   
            else:
                #证券代码在二级字典并且K线类型不在三级字典, 添加一行三级字典记录，不影响别的K线类型list数据
                self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type] =\
                                        [[symbol, kline_type, data_length], df_kline, bsuc, df_kline]              
        else:
            #证券代码不在二级字典, 添加新三级字典
            klines_dict_bytype = {}
            klines_dict_bytype[kline_type] = [[symbol, kline_type, data_length], df_kline, bsuc, df_kline]
            self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol] = klines_dict_bytype


        # 不管是成功还是失败，每次均返回DataFrame对象 
        return df_kline 

    # ----------------------------------------------------------------------
    def fetch_ticks(self, symbol: str, data_length: int = 0) -> pd.DataFrame:
        doc="""
        获取ticks序列数据(网际风分笔数据 3秒周期)

        请求指定证券代码的ticks序列数据. 序列数据会随着时间推进自动更新

        Args:
            symbol (str): 指定证券代码.

            data_length (int): 需要获取的序列长度。每个序列最大支持请求 8964 ?(待测试确认) 个数据

        Returns:
            pandas.DataFrame: 本函数总是返回一个 pandas.DataFrame 实例. 行数=data_length, 包含以下列:

            * id:       (k线序列号，从0开始 )    * DataFrame里 新增字段
            * label:    (证券代码)              * DataFrame里 新增字段
            * ktype:    (K线类型) = '分笔'      * DataFrame里 新增字段 

            * time:     (K线起点时间戳UTC，单位:秒数)   *** time字段是唯一业务索引 ***
            * close:    (现价)
            * volume:   (成交量)
            * amount:   (成交额)
            * traceNum: ()
            * pricebuy: (申买价) 12345
            * volbuy:   (申买量) 12345
            * pricesell:(申卖价) 12345
            * volsell:  (申卖量) 12345

            #####traceNum 是本次几个下单组成，按道理也是不断增加的，两次差值，本次

        Example::

            # 获取 SH600000 的ticks序列
            from npsdk import NpSdkApi

            api = NpSdkApi()
            ticks = api.fetch_ticks("SH600000")
            while True:
                api.refreshing()
                if api.is_updated(ticks.iloc[-1], 'close'):
                    print(ticks.iloc[-1].close)

            # 预计的输出是这样的:
            10.2
            10.0
            10.3
            ...
        """

        kline_type = NPSDKAPI_NEZIP_OEMTRACE #'分笔'
        return self.fetch_klines(symbol, kline_type, data_length)
        

    # ----------------------------------------------------------------------
    # 检查历史命令请求里是否有fetch_quote命令,有则执行
    def _run_quotes_tasks(self) -> None:

        if len(self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ]) > 0:

            for symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ].keys():
                cmdpara = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol][0] #取出历史命令参数:列表
                
                #await loop.run_in_executor(None, self.fetch_quote, cmdpara[0])
                self.fetch_quote(cmdpara[0]) # 找到历史命令，再次执行

                #logger.info("执行历史命令：fetch_quote(%s)......"% cmdpara[0])

        else: # 在历史命令记录里，没有找到一条fetch_quote命令，这种情况为 refreshing()函数前从未执行过fetch_quote命令
            pass

        #await asyncio.sleep(NPSDKAPI_CMD_TIMEOUT_VAL)
        #time.sleep(NPSDKAPI_CMD_TIMEOUT_VAL)

    # ----------------------------------------------------------------------
    # 检查历史命令请求里是否有fetch_klines命令,有则执行
    def _run_klines_tasks(self) -> None:

        if len(self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ]) > 0:

            for symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ].keys():
                for klinetypekey in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol].keys():
        
                    cmdpara = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][klinetypekey][0] #取出历史命令参数:列表

                    #await loop.run_in_executor(pool, self.fetch_klines, cmdpara[0], cmdpara[1], cmdpara[2])
                    self.fetch_klines(cmdpara[0], cmdpara[1], cmdpara[2]) # 找到历史命令，再次执行
                    #logger.info("执行历史命令：fetch_klines(%s,%s,%s)......"% (cmdpara[0], cmdpara[1], cmdpara[2]))

        else: # 在历史命令记录里，没有找到一条fetch_klines命令，这种情况为 refreshing()函数前从未执行过fetch_klines命令
            pass

        #await asyncio.sleep(NPSDKAPI_CMD_TIMEOUT_VAL)
        #time.sleep(NPSDKAPI_CMD_TIMEOUT_VAL)

    def _1st_run_klines_tasks(self) -> None:

        if self._run1st_flag:
            #t = time.perf_counter()
            if len(self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ]) > 0:

                for symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ].keys():
                    for klinetypekey in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol].keys():
                        
                        """
                        策略程序刚开始运行时，npsdk为所有K线数据（包括分笔ticks）伪造了一行时间戳为0的数据行
                        以避免策略程序调用df.iloc[-1]索引值异常错误
                        """
                        if klinetypekey == NPSDKAPI_NEZIP_OEMTRACE: #'分笔'
                            klinebody = OEM_TRACE()
                        else:
                            klinebody = RCV_KLINE()

                        klinebody.time = 0
                        df_kline = self._npdatapoolthread._convert_klines_to_dataframe(symbol, klinetypekey, klinebody.encode()) 
                        lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][klinetypekey][3]
                        self.df_columns_copy(lastobj, df_kline)

                        """
                        # npsdkapi实例第一次运行
                        # 如果策略程序第一次运行后，由于可能没取到数据，lastobj为空df，造成策略程序函数is_updated(df.iloc[-1])参数索引越界
                        # 所以如果lastobj为空df，就死等df_kline不为空df时，复制df_kline到lastobj
                        while 1:
                            #df_kline = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][klinetypekey][1]
                            lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][klinetypekey][3]
                            
                            #if len(lastobj.index) != 0: break
                            if self.is_updated(lastobj): break

                            cmdpara = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][klinetypekey][0] #取出历史命令参数:列表

                            self.fetch_klines(cmdpara[0], cmdpara[1], cmdpara[2]) # 找到历史命令，再次执行

                            #logger.info("执行历史命令：fetch_klines(%s,%s,%s)......"% (cmdpara[0], cmdpara[1], cmdpara[2]))
                        """
            self._run1st_flag = False
            #print(f'(1st)  _run_klines_tasks coast:{time.perf_counter() - t:.8f}s')

    """
    异步协程 入口主函数
    """
    async def _async_run_tasks(self) -> None: 
            """各种异步IO的调用方式"""  

            """ 
            await self._run_quotes_tasks()
            await self._run_klines_tasks()

            await asyncio.gather(asyncio.to_thread(self._run_quotes_tasks),asyncio.sleep(NPSDKAPI_CMD_TIMEOUT_VAL))
            await asyncio.gather(asyncio.to_thread(self._run_klines_tasks),asyncio.sleep(NPSDKAPI_CMD_TIMEOUT_VAL))
            """

            loop = asyncio.get_running_loop()

            #Option 1: run in default executor
            """            
            await loop.run_in_executor(None, self._run_quotes_tasks)  
            await loop.run_in_executor(None, self._run_klines_tasks) 
            """

            #Option 2: run in ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor() as pool: 
                await loop.run_in_executor(pool, self._run_quotes_tasks)  
                await loop.run_in_executor(pool, self._run_klines_tasks)   

            #Option 3: run in ProcessPoolExecutor
            """
            with concurrent.futures.ProcessPoolExecutor(max_workers=5) as pool:
                await loop.run_in_executor(pool, self._npdatapoolthread.send_command_pro)            
                await loop.run_in_executor(pool, self._npdatapoolthread.receive_data_pro)
            """

    # ----------------------------------------------------------------------
    def refreshing(self) -> None:
        doc="""
        执行 调用本次refreshing()函数前的 历史命令，等待业务数据更新:
        * 再次执行历史命令 fetch_quote, fetch_klines, fetch_ticks...（暂时不包括 交易命令）
        * 后续用户可以调用 api.is_updated(参数) 查询用户关心的数据

        Args:
            None

        Returns:
            None

            #bool: 如果收到业务数据更新则返回 True, 如果到截止时间依然没有收到业务数据更新则返回 False
        """

        try:
            #print('等待数据更新......')

            self._1st_run_klines_tasks()

            """异步模式
            self._loop.call_soon_threadsafe(self._run_quotes_tasks)
            self._loop.call_soon_threadsafe(self._run_klines_tasks)
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
 
            future = asyncio.gather(self._run_quotes_tasks(), self._run_klines_tasks())
            self._loop.run_until_complete(future)
            """

            #asyncio.run(self._async_run_tasks())

            """同步模式"""
            # 检查历史命令请求里是否有fetch_quote命令,有则执行
            self._run_quotes_tasks()         

            # 检查历史命令请求里是否有fetch_klines命令,有则执行
            self._run_klines_tasks() 
            
            time.sleep(NPSDKAPI_CMD_TIMEOUT_VAL)              

        except KeyboardInterrupt:

            self.close()

            print('Keyboard Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)

    # ----------------------------------------------------------------------
    def is_updated(self, obj: Any, propkey: list = None) -> bool:
        doc="""
        判定obj最近是否有更新

        对比 获取的数据响应 和 前一次的数据响应，返回业务数据更新标志

        当业务数据更新导致 refreshing() 返回后可以使用该函数判断 **本次业务数据更新是否包含特定obj或其中某个字段** 。

        关于判断K线更新的说明：
        当生成新K线时，其所有字段都算作有更新，若此时执行 api.is_updated(klines.iloc[-1]) 则一定返回True。
        Args:
        obj (any): 任意业务对象, 包括 fetch_quote 返回的 quote, fetch_klines 返回的 DataFrame, fetch_account 返回的 account 等

        propkey (list of str): [必选]需要判断的字段，空列表表示全部属性字段
                                * （空列表）不指定: 当该obj下的任意字段有更新时返回True, 否则返回 False.
                                * list of str: 当该obj下的指定字段中的任何一个字段有更新时返回True, 否则返回 False.
        """

        try:
            #检查参数
            if obj is None:
                raise Exception("参数1为 None Obj, 请检查参数1是否填写正确")
                return False

            if not isinstance(propkey, list):
                propkey = [propkey] if propkey else []
            
            if isinstance(obj, OEM_REPORT):

                return self.is_updated_quote(obj, propkey)
            
            elif isinstance(obj, pd.DataFrame): 

                return self.is_updated_klines_df(obj, propkey)
 
            elif isinstance(obj, pd.Series):
                
                return self.is_updated_klines_sr(obj, propkey)

            else: 
                print("不支持的参数1业务对象：%s"%type(obj))
                return False

        except Exception as e:  
            logger.warning('is_updated() exception: %r'% e)
            return False

    # ----------------------------------------------------------------------
    def is_updated_quote(self, obj: Any, propkey: list = None) -> bool:
        """
        实时行情: 数据结构体定义详见 Stockdrv.py
        """
        # 要读取的属性字段 必须是正确的quote属性字段
        if len(propkey) == 0: #比较所有属性字段
            propkey = fieldsOfQuoteToCompared
        for pkey in propkey:
            if not pkey in fieldsOfQuoteToCompared:
                raise Exception("api.is_updated()函数参数2(%s)里有不正确的实时行情属性字段名"% propkey)
                return False

        symbol = obj.label
        if symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ].keys():

            current_quote = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol][1]
            lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol][3]
            bsuc = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol][2]

            # 如果本次读取数据失败，再继续数据比较就没有意义，应该立即返回False
            if not bsuc:
                logger.warning('本次获取实时行情失败：%s'%symbol)
                return False

            # 如果id(obj)不匹配，再继续数据比较就没有意义，应该立即返回False
            if id(obj) != id(lastobj):
                logger.warning('实时行情对象内存地址已改变 %s %s %s'%(symbol, obj.label, lastobj.label))
                return False

            if lastobj.encode() == current_quote.encode(): #比较前后二个Quote是否完全一致
                #print('>>>>>>>>>>same quote: %s <<<<<<<<<<'%symbol)
                return False
                
            """
            类变量引用中的属性字段比较： 用id(obj)可以获得obj的引用内存地址
            策略程序里的类变量引用的内存值（执行refreshing()前获得的）： quote = api.fetch_quote("SH600000")
            和
            最新获得的类变量引用的内存值（执行refreshing()后获得的）：self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ]["SH600000"]
            """
            if len(propkey) == 0: #比较所有属性字段
                fieldsToCompared = fieldsOfQuoteToCompared
            else: #比较参数2里的属性字段
                fieldsToCompared = propkey

            is_changed = False
            for pkey in fieldsToCompared:
                if getattr(obj, pkey) != getattr(current_quote, pkey): # 如果有属性字段数据更新，则更新obj所有属性字段
                    logprtstr = "   sdk>>> 实时行情 数据更新(代码: %s 名称: %s) (属性字段: %s [o:n] = %s : %s)"% (current_quote.label, current_quote.name, pkey, getattr(obj, pkey), getattr(current_quote, pkey))
                    #if pkey == 'time': print(time.strftime('%y-%m-%d %H:%M:%S', time.localtime(current_quote.time)))
 
                    if self._npsdk_debug: print(logprtstr); logger.info(logprtstr)
                    is_changed = True
            if is_changed:
                #memmove(addressof(obj), addressof(current_quote), sizeof(OEM_REPORT)) # 更新策略程序里的类变量引用的内存值
                memmove(addressof(lastobj), addressof(current_quote), sizeof(OEM_REPORT)) # 更新策略程序里的类变量引用的内存值
                return True 
            else:
                """
                如果用户输入的属性字段值没变化，但是该实时行情别的属性字段值变化了，也应该更新数据，但是返回False
                比如 open值一直不变，但是amount,volume变化了
                """                
                if obj.encode() != current_quote.encode():
                    memmove(addressof(lastobj), addressof(current_quote), sizeof(OEM_REPORT)) # 更新策略程序里的类变量引用的内存值

                return False

        else:
            logger.warning('该证券代码没有读取过数据：%s'%symbol)
            return False

    # ----------------------------------------------------------------------
    def is_updated_klines_df(self, obj: Any, propkey: list = None) -> bool:
        # Pandas Dataframe iloc[]->class 'pandas.core.series.Series'  
        # Pandas Dataframe->class 'pandas.core.frame.DataFrame'
        # DataFrame对象，仅比较最近二次取得的df是否一样，如果不一样，则检查最后一根K线的所有属性字段是否一样
  
        """
        DataFrame: DataFrame 是一个表格型的数据结构, 它可以被看做由Series组成的字典（共同用一个索引）
        Two-dimensional, size-mutable, potentially heterogeneous tabular data.
        1. K线数据
        2. ....
        """
        # 从DataFrame obj里取证券代码和K线类型
        try:
            """
            # K线Dataframe: 
            """
            if 'ktype' in obj.columns: 
                for symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ].keys():
                    for kline_type in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol].keys():
                        
                        #查看历史记录里是否有obj id
                        if id(obj) == id(self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][3]):
                            
                            df_kline = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][1]
                            lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][3]                            
                            bsuc = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][2] 

                            if not bsuc: #本次从服务器读取读取K线失败，没必要继续比较
                                logger.warning('本次获取K线数据失败：%s : %s'%(symbol,kline_type))
                                return False

                            if len(df_kline.index) == 0: #空DataFrame
                                logger.warning('本次获取K线数据为空df：%s : %s'%(symbol,kline_type))
                                return False
                            
                            if df_kline.iloc[-1].equals(lastobj.iloc[-1]): #比较前后二个DataFrame的iloc[-1]是否完全一致
                                #print('>>>>>>>>>>same df: %s:%s <<<<<<<<<<'%(symbol,kline_type))
                                #logger.warning('前后二次收到的DataFrame的iloc[-1]一样')
                            #if df_kline.equals(lastobj): #比较前后二个DataFrame是否完全一致
                                #logger.warning('前后二次收到的DataFrame一样')
                                return False 
                            else:
                                """
                                # # 以下情况判断K线发生变化 更新前一次收到的df
                                # 1. 第一次没取到数据(空df) 这种情况已经不可能（参见_run_klines_tasks()）
                                # 2. 一根没走完的K线：在前后二次df中，该根K线的时间戳没变化但对应的属性字段发生变化
                                # 3. 一根新K线：刚收到的df新增一根K线(时间戳变化了)
                                """
                                condition1 = (len(obj.index) == 0)
                                condition2 = (not condition1) and (obj.iloc[-1]['time'] == df_kline.iloc[-1]['time'] and (not obj.iloc[-1].equals(df_kline.iloc[-1])))
                                condition3 = (not condition1) and (obj.iloc[-1]['time'] != df_kline.iloc[-1]['time'])

                                #print(condition1,condition2,condition3)

                                if condition1 or condition2 or condition3:
                                    logprtstr = "   sdk>>> K线 数据更新(代码: %s K线类型: %s)"% (df_kline.iloc[-1].label, df_kline.iloc[-1].ktype)

                                    if self._npsdk_debug: print(logprtstr); logger.info(logprtstr)

                                    self.df_columns_copy(lastobj, df_kline)

                                    """
                                    if condition2: 
                                        # 同根新值
                                        logprtstr = ">>>>>> K线 数据更新(代码: %s K线类型: %s) (***同根新值***)"% (df_kline.iloc[-1].label, df_kline.iloc[-1].ktype)
                                        print(logprtstr); logger.info(logprtstr)

                                    if condition3: 
                                        # 新增一根
                                        logprtstr = ">>>>>> K线 数据更新(代码: %s K线类型: %s) (***新增一根***)"% (df_kline.iloc[-1].label, df_kline.iloc[-1].ktype)
                                        print(logprtstr); logger.info(logprtstr)
                                    """                                            
                                    return True
                                else:
                                    return False

                            return False
                        else:
                            pass
                logger.warning('在K线历史记录里没找到参数1对应的DataFrame对象')
                return False

            else:
                logger.warning('非K线数据的DataFrame对象')
                return False

        except Exception as e:
            logger.warning('is_updated_klines_df(obj, pd.DataFrame) exception: %r'% e)
            return False

    # ----------------------------------------------------------------------
    def is_updated_klines_sr(self, obj: Any, propkey: list = None) -> bool:

        """
        Series: Series 是一种类似于一维数组的对象.它由一组数据（各种Numpy数据类型）
        以及一组与之相关的数据标签（即索引）组成。
        One-dimensional ndarray with axis labels (including time series).
        1. K线数据
        2. ....
        """      
        # 从Series obj里取证券代码和K线类型
        #print('Series Object: id=%s\r\nname:\r\n%s\r\nindex:\r\n%s\r\nobj:\r\n%r'%(id(obj), obj.name, obj.index, obj))
        try:
            """
            # K线Series: 对应K线Series对象，is_updated函数参数1格式应该输入：df.iloc[-1]，
            # 其它(df.iloc[0], df.iloc[-2]之类的输入)没有意义
            """
            if 'ktype' in obj.index: # K线Series的索引

                if obj['ktype'] == NPSDKAPI_NEZIP_OEMTRACE: #'分笔'
                    fieldsOfObj = fieldsOfTrace
                else:
                    fieldsOfObj = fieldsOfKline

                if len(propkey) == 0: #比较所有属性字段
                    propkey = fieldsOfObj

                # 要读取的属性字段 必须是正确的kline属性字段
                for pkey in propkey:
                    if not pkey in fieldsOfObj:
                        raise Exception("api.is_updated()函数参数2(%s)里有不正确的K线数据属性字段名"% propkey)
                        return False
                
                symbol = obj['label']
                kline_type = obj['ktype']
                if symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ].keys():
                    if kline_type in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol].keys():
                        
                        df_kline = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][1]
                        lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][3]                            
                        bsuc = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][2] 

                        if not bsuc: #本次从服务器读取读取K线失败，没必要继续比较
                            logger.warning('本次获取K线数据失败：%s : %s'%(symbol,kline_type))
                            return False

                        if len(df_kline.index) == 0: #空DataFrame
                            logger.warning('本次获取K线数据为空df：%s : %s'%(symbol,kline_type))
                            return False
                        
                        if df_kline.iloc[-1].equals(lastobj.iloc[-1]): #比较前后二个DataFrame的iloc[-1]是否完全一致
                            #print('>>>>>>>>>>same sr: %s:%s <<<<<<<<<<'%(symbol,kline_type))
                            #logger.warning('前后二次收到的DataFrame的iloc[-1]一样')
                        #if df_kline.equals(lastobj): #比较前后二个DataFrame是否完全一致
                        #    #logger.warning('前后二次收到的DataFrame一样')
                            return False 

                        if obj.values in lastobj.values: #检查Series是否在策略端DataFrame中
                            # obj.name 是 该Series在DataFrame中的索引行号，用if obj.name in lastobj.index:也能判断
                            
                            # 属性字段time时间戳是业务唯一索引
                            timestampe_src = obj['time']  #该obj的属性字段time时间戳
                            if timestampe_src in df_kline['time'].values:
                                index_src = lastobj['time'].tolist().index(timestampe_src)
                                index_des = df_kline['time'].tolist().index(timestampe_src)

                                #print('指定时间戳:(%s) Series Name:(%s) 源索引:(%s) 目的索引:(%s)'%(timestampe_src, obj.name, index_src, index_des))
                                """
                                # 以下情况更新前一次收到的df
                                # 1. 一根没走完的K线：在前后二次df中，该根K线的时间戳没变化但对应的属性字段发生变化
                                # 2. 一根新K线：刚收到的df新增一根K线(时间戳变化了)
                                """
                                condition2 = False
                                condition3 = False
                                condition4 = False

                                if df_kline.iloc[-1]['time'] > df_kline.iloc[index_des]['time']: # 新增一根
                                    condition3 = True
                                else: # 同根比较
                                    if len(propkey) == 0: #比较所有属性字段
                                        fieldsToCompared = fieldsOfObj
                                    else: #比较参数2里的属性字段
                                        fieldsToCompared = propkey

                                    is_changed = False
                                    for filed in fieldsToCompared:
                                        if obj[filed] != df_kline.iloc[index_des][filed]:
                                            logprtstr = "   sdk>>> K线指定行字段 数据更新(代码: %s K线类型: %s) (属性字段: %s [o:n] =  %s : %s)"% (symbol, kline_type, filed, obj[filed], df_kline.iloc[index_des][filed])
                                            
                                            if self._npsdk_debug: print(logprtstr); logger.info(logprtstr)
                                            is_changed = True
                                            condition2 = True
                                    
                                    """
                                    整根K线比较（比较所有属性字段）
                                    """
                                    if not obj.equals(df_kline.iloc[index_des]):
                                        condition4 = True

                                if condition2 or condition3: 
                                    self.df_columns_copy(lastobj, df_kline)

                                    """
                                    if condition2: 
                                        # 同根新值
                                        logprtstr = ">>>>>> K线指定行字段 数据更新(代码: %s K线类型: %s) (***同根新值***)"% (symbol, kline_type)
                                        print(logprtstr); logger.info(logprtstr)

                                    if condition3: 
                                        # 新增一根
                                        logprtstr = ">>>>>> K线指定行字段 数据更新(代码: %s K线类型: %s) (***新增一根***)"% (symbol, kline_type)
                                        print(logprtstr); logger.info(logprtstr)
                                    """
                                        
                                    return True 
                                else:
                                    """
                                    如果用户输入的属性字段参数没有变化，而该K线其他属性字段值变化，也应该进行df列赋值，但是返回False
                                    比如 is_updated(df.iloc[-1],'open') 5分钟内open值一直不变，但是close,high,amount,volume等别的属性字段值变化了
                                    """
                                    if (not condition2) and condition4:
                                        self.df_columns_copy(lastobj, df_kline)
                                    
                                    return False
                            else: 
                               
                                """策略程序刚开始运行时，npsdk为所有K线数据（包括分笔ticks）伪造了一行时间戳为0的数据行"""
                                if timestampe_src == 0:
                                   self.df_columns_copy(lastobj, df_kline) 
                                   return True
                                else:
                                    logger.warning('该K线Series的时间戳不在DataFrame中(df_kline) %s  %s  lastobj %r df_kline %r'%(obj['label'], obj['ktype'], lastobj, df_kline))

                                return False
                        else: 
                            logger.warning('该K线Series对象不在DataFrame中(按Values找) %s  %s'%(obj['label'],obj['ktype']))
                            return False
                    else:
                        logger.warning('该K线Series对象不在DataFrame中(按证券代码+K线类型找) %s  %s'%(obj['label'],obj['ktype']))     
                        return False                       
                else:
                    logger.warning('该K线Series对象不在DataFrame中(按证券代码找) %s '%(obj['label']))     
                    return False

            else: #其它业务类型的Series obj
                logger.warning('非K线数据的Series对象')
                return False
        except Exception as e:
            logger.warning('is_updated_klines_sr(obj, pd.Series) exception: %r'% e)
            return False



    """
    Utilities: for pd.Dataframe
    """
    # ----------------------------------------------------------------------
    #二个相同结构的DF的列赋值copy
    def df_columns_copy(self, lastobj, df_kline):
        try:
            #清空DataFrame, 再按列赋值
            lastobj.drop(lastobj.index,inplace=True) 
            for col in df_kline.columns:
                lastobj[col] = df_kline[col]
            #lastobj.set_index(["time"], inplace=True) 
            #lastobj['updatetime'] = pd.to_datetime(lastobj.time, origin=pd.Timestamp('1970-01-01 00:00:00'))
            lastobj['updatetime'] = pd.to_datetime(lastobj.time)
            lastobj.set_index(["updatetime"], inplace=True)
        except Exception as e:
            logger.warning('df_columns_copy() exception: %r'% e)

#if __name__=='__main__':
#    api = NpSdkApi(debug = True, playmode = 'nezipwebsocket')
#    api.open()
        
            

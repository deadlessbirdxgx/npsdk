#!/usr/bin/env python
#  -*- coding: utf-8 -*-
"""
NpSdkApi接口的PYTHON封装
"""
__author__ = 'xxg'

from multiprocessing import Process, JoinableQueue
import os
import sys
import time
import uuid
from datetime import datetime
from typing import Union, List, Any, Optional
import numpy as np
import pandas as pd
import threading
from urllib import parse
from ctypes import *

from npsdkobjs import *
from Stockdrv import *
from npdatapoolthread import NpDataPoolThread
import logging

from __version__ import __version__

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

        super().__init__()
                 
        doc="""
        创建NpSdkApi接口实例
        """

        # NpSDK Rpc Client
        #self._npsdkrpcclient = NpSdkRpcClient()
        #logger.info('create NpSdkRpcClient...')

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

        self._npsdkapi_requests_dict = { # correlation_id: NpApiDataObj

        }
        # 28个字段定义见 Stockdrv.py文件里的 class OEM_REPORT(Structure):
        # 可以通过语句注释功能来选择字段判断实时行情数据是否更新    
        self._fieldsOfQuoteToCompared = [      #实时行情 400 字节，带*表示网际风内部数据，请跳过
                        'label',            #代码
                        'name',             #名称
                        'time',             #成交时间UTC时间，可以百度搜索相关转换)
                        'foot',             #最后处理分钟数
                        'openDate',         #市场开盘日期零点
                        'openTime',         #市场开盘时间
                        'closeDate',        #市场日线日期零点
                        'open',             #今日开盘
                        'high',             #今日最高
                        'low',              #今日最低
                        'close',            #最新价格
                        'volume',           #总成交量
                        'amount',           #总成交金额
                        #'inVol',            #外盘*
                        'pricesell',        #申卖价1,2,3,4,5
                        'volsell',           #申卖量1,2,3,4,5      
                        #'vsellCha',         #申卖量变化*
                        'pricebuy',         #申买价1,2,3,4,5      
                        'volbuy',            #申买量1,2,3,4,5       
                        #'vbuyCha',           #申买量变化*
                        'jingJia',          #集合竞价
                        'avPrice',          #期货的结算价（平均价）
                        #'isBuy',            #0 下跌；1 上涨或平盘*
                        'nowv',             #现量
                        'nowa',             #分笔成交额期货仓差)
                        #'change',           #换手率*
                        #'weiBi',            #委比*
                        #'liangBi',          #量比*
                        #'temp',       
                    ] # fields

        # 字段定义见 Stockdrv.py文件里的 class RCV_KLINE(Structure): 不含分笔
        self._fieldsOfKline = [      
                    'time',                 #时间 UTC时间戳 单位：s秒
                    'open',                 #开盘
                    'high',                 #最高
                    'low',                  #最低
                    'close',                #现价
                    'volume',               #成交量
                    'amount',               #成交额
                    'temp',                 #预留
                    ] # fields

        # 字段定义见 Stockdrv.py文件里的 class OEM_TRACE(Structure): 分笔
        self._fieldsOfTrace = [      
                    'time',                 #时间 UTC时间戳 单位：s秒
                    'close',                #现价
                    'volume',               #成交量
                    'amount',               #成交额
                    'traceNum',             #

                    #'pricebuy',             #申买价
                    #'volbuy',               #申买量
                    #'pricesell',            #申卖价
                    #'volsell',              #申卖量

                    'pricebuy1',            #申买价 1,2,3,4,5
                    'pricebuy2',
                    'pricebuy3',
                    'pricebuy4',
                    'pricebuy5',
                    'volbuy1',              #申买量 1,2,3,4,5
                    'volbuy2',
                    'volbuy3',
                    'volbuy4',
                    'volbuy5',
                    'pricesell1',           #申卖价 1,2,3,4,5
                    'pricesell2',
                    'pricesell3',
                    'pricesell4',
                    'pricesell5', 
                    'volsell1',             #申卖量 1,2,3,4,5
                    'volsell2', 
                    'volsell3', 
                    'volsell4', 
                    'volsell5', 
                    ] # fields

        self._stopped_flag = False

        self._npsdk_debug = debug

        self._npsdk_playmode = playmode

        """
        # 用户使用npsdk免责提示
        """
        print(NPSDKAPI_LEGAL_TIPS)

        self.run_datapool_thread() #启动datapoolthread

    def run_datapool_thread(self):
        self._command_queue = JoinableQueue()     
        self._response_queue = JoinableQueue()    
        logger.info('create global command queue & response queue')

        # 创建数据仓库进程
        self._npdatapoolthread = NpDataPoolThread(self._command_queue, self._response_queue, self._npsdk_playmode)
        self._npdatapoolthread.start() 

        logger.info('create & start npdatapool thread')


    def close(self) -> None:
        """
        关闭NpSdkApi接口实例并释放相应资源
        """
        self._stopped_flag = True #停止使用Queue发送消息
        
        # 暴力退出
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

        self._npdatapoolthread.stop_me() 

        self._command_queue.join()
        self._command_queue.close()
        self._response_queue.join()
        self._response_queue.close()

        
 
    """    
    向command_queue中压入请求命令
    返回：bool
    """
    def put_command_queue(self, npapidataobj):  
        try:
            with self.internal_lock:                
                if self._stopped_flag: 
                    return False 

                if not self._command_queue.full():
                    self._command_queue.put(npapidataobj) #put_nowait()
                    #print('put command into command_queue ')
                    return True
                else:
                    return False
        except Exception as e:
            logger.warning('put_command_queue() exception: %r'% e)
            return False

    """    
    从response_queue中取出命令数据响应
    返回：元组(bool, NpApiDataObj)
    """
    def get_response_queue(self) -> NpApiDataObj:   
        npapidataobj = NpApiDataObj()
        
        try:
            with self.internal_lock:                
                if self._stopped_flag: 
                    return False, npapidataobj             
                if not self._response_queue.empty():
                    npapidataobj = self._response_queue.get()  #get_nowait() 
                    #print('got response from response_queue ')
                    #self._response_queue.task_done()
                    return True, npapidataobj  
                else:
                    return False, npapidataobj  
        except Exception as e:
            logger.warning('get_response_queue() exception: %r'% e)
            return False, npapidataobj 

    """    
    向response_queue中压入命令数据响应
    返回：bool
    """
    def put_response_queue(self, npapidataobj):  
        try:
            with self.internal_lock:                      
                if self._stopped_flag: 
                    return False 
            
                if not self._response_queue.full():
                    self._response_queue.put(npapidataobj) #put_nowait()
                    return True
                else:
                    return False
        except Exception as e:
            logger.warning('put_response_queue() exception: %r'% e)
            return False


    # ----------------------------------------------------------------------
    def fetch_quote(self, symbol: str) -> OEM_REPORT:
        return self.get_quote(symbol)
     # ----------------------------------------------------------------------
    def get_quote(self, symbol: str) -> OEM_REPORT:
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
            quote = api.get_quote("SH600000")
            print(quote.close)
            while api.wait_update():
                print(quote.close)

            # 预计的输出是这样的:
            0.0
            10.03
            10.03
            ...
        """

        quote = OEM_REPORT() # 初始化结构体  
        quote.label = symbol   
 
        # 向服务器端发送get_quote命令请求
        npapiobj = NpApiDataObj()
        npapiobj.app_id = NPSDKAPI_APPID_NPSDKAPI
        npapiobj.message_id = NPSDKAPI_MESSAGEID_GETQUOTE_REQ
        npapiobj.correlation_id = str(uuid.uuid4())
        npapiobj.reply_to = NPSDKAPI_MESSAGEID_GETQUOTE_RES
        npapiobj.request_body = '代码=' + symbol + '&类型=实时数据'  # 类型不要写成'实时行情'
        self.put_command_queue(npapiobj) 

        # 该证券代码第一次执行get_quote()函数
        if not symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ].keys(): 
            self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol] = [[symbol], quote, False, quote]

        # 记录发送的数据包
        self._npsdkapi_requests_dict[npapiobj.correlation_id] = npapiobj

        # 不管是成功还是失败，每次均返回quote对象
        return quote
  

    # ----------------------------------------------------------------------
    def fetch_klines(self, symbol: str, kline_type: str = "1分钟线", data_length: int = 1000) -> pd.DataFrame:
        return self.get_kline_serial(symbol, kline_type, data_length)
    # ----------------------------------------------------------------------
    def get_kline_serial(self, symbol: str, kline_type: str = "1分钟线", data_length: int = 1000) -> pd.DataFrame:
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
            from npsdkapi import NpSdkApi

            api = NpSdkApi()
            klines = api.get_kline_serial("SH600000", "1分钟线", 60)
            print(klines.iloc[-1].close)
            while True:
                api.wait_update()
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
    
        """
        #创建一个空 klines DataFrame  
        """
        #创建空DataFrame:行索引默认从0开始，列索引里加入K线序列号id，也是从0开始, 指定证券代码, K线类型
        df_kline = self._convert_klines_to_dataframe(symbol, kline_type, b'') 

        # 向服务器端发送get_kline_serial命令请求
        npapiobj = NpApiDataObj()
        npapiobj.app_id = NPSDKAPI_APPID_NPSDKAPI
        npapiobj.message_id = NPSDKAPI_MESSAGEID_GETKLINES_REQ
        npapiobj.correlation_id = str(uuid.uuid4())
        npapiobj.reply_to = NPSDKAPI_MESSAGEID_GETKLINES_RES
        npapiobj.request_body = '代码=%s&类型=%s&数量=%s'%(symbol, kline_type, data_length) # 包括 '分笔'
        self.put_command_queue(npapiobj) 

        condition1 = symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ].keys()
        if condition1:
            condition2 = kline_type in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol].keys() 

        if condition1:
            if condition2:
                # 该证券代码该K线类型 已经存在历史记录
                pass
            else:
                #证券代码在二级字典并且K线类型不在三级字典, 添加一行三级字典记录，不影响别的K线类型list数据
                self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type] =\
                                        [[symbol, kline_type, data_length], df_kline, npapiobj.correlation_id, df_kline]              
        else:
            #证券代码不在二级字典, 添加新三级字典
            klines_dict_bytype = {}
            klines_dict_bytype[kline_type] = [[symbol, kline_type, data_length], df_kline, npapiobj.correlation_id, df_kline]
            self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol] = klines_dict_bytype

        # 不管是成功还是失败，每次均返回DataFrame对象 
        return df_kline 


        time.sleep(NPSDKAPI_CMD_TIMEOUT_VAL)
        bret, npapiobjdes = self.get_response_queue() # 等数据池的数据消息
        if not bret: 
            npapiobjdes.response_type = NPSDKAPI_TIMEOUT
                    
        if  npapiobjdes.response_type == NPSDKAPI_TIMEOUT:  # 向服务器发送命令后，等待命令响应超时。有可能超时后命令响应才到
            nowstr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            logger.info(" [.] err: got command(get_kline_serial) response: %ssec timeout at: %s " %(NPSDKAPI_CMD_TIMEOUT_VAL, nowstr))

        elif npapiobjdes.app_id == NPSDKAPI_APPID_NPSDKDATA and \
            npapiobjdes.message_id == NPSDKAPI_MESSAGEID_GETKLINES_RES  and \
            npapiobjdes.response_type == NPSDKAPI_SUCCESS:  # 向服务器发送命令后，收到命令响应（服务器端正常）。
            
            #把K线数据原始字节流转成pandas.DataFrame
            #t = time.perf_counter()
            df_kline = self._convert_klines_to_dataframe(symbol, kline_type, npapiobjdes.response_body) 
            #print(f'_convert_klines_to_dataframe coast:{time.perf_counter() - t:.8f}s')
            if len(df_kline.index) != 0: # 判断df不为空, 收到数据并且转换成功才能把bsuc设为True
                bsuc = True
            
                nowstr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                logger.info(" [.] suc: got command(get_kline_serial) response: symbol: %s kline_type: %s at: %s" %(symbol, kline_type, nowstr))

        elif npapiobjdes.app_id == NPSDKAPI_APPID_NPSDKDATA and \
            npapiobjdes.message_id == NPSDKAPI_MESSAGEID_GETKLINES_RES  and \
            npapiobjdes.response_type == NPSDKAPI_ERROR:   # 向服务器发送命令后，收到命令响应（服务器端异常）。
            nowstr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            lpoprtstr = " [.] err: got command(get_kline_serial) response: %s at: %s " %(npapiobjdes.response_body, nowstr)
            logger.info(lpoprtstr)
            if self._npsdk_debug: print(lpoprtstr); 

        #记录本次执行结果更新, ，以便在is_changing中做对比
        #列表第1项记录本次命令参数，第2项记录结果数据，第3项记录本次命令成功与否, 第四项纪录第一次get_kline_serial(指定证券代码symbol)的结果df obj
        #以K线类型为key值 更新三级字典,以证券代码为key值 更新二级字典

        condition1 = symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ].keys()
        if condition1:
            condition2 = kline_type in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol].keys() 
        
        if condition1:
            if condition2:
                #证券代码在二级字典并且K线类型在三级字典, 修改原纪录list
                # 取出上一次df数据
                lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][3]
                if bsuc: # 如果成功收到数据
                    pass
                    """
                    #本次成功接收到K线数据，比较并且合并二次收到的K线数据 
                    #t = time.perf_counter()
                    bret, df_kline_merged = self._compare_merge_klines_dataframe(lastobj, df_kline)
                    #print(f'_compare_merge_klines_dataframe coast:{time.perf_counter() - t:.8f}s')
                    if bret: # 如果合并成功 则存放合并后的df
                        #清空DataFrame, 再按列赋值
                        lastobj.drop(lastobj.index,inplace=True) 
                        for col in df_kline_merged.columns:
                            lastobj[col] = df_kline_merged[col]
                        
                    """
                else:
                    logger.info('本次读取K线数据失败 %s:%s'%(symbol, kline_type))

                self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type] = \
                                        [[symbol, kline_type, data_length], df_kline, bsuc, lastobj] 
            else:
                #证券代码在二级字典并且K线类型不在三级字典, 添加新纪录list，不影响别的K线类型list数据
                #该K线类型第一次申请K线数据，收到数据无需比较, 所以保存结果后直接返回  
                self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type] =\
                                        [[symbol, kline_type, data_length], df_kline, bsuc, df_kline]              

        else:
            #证券代码不在二级字典, 添加新纪录list  
            #该证券代码第一次申请K线数据，收到数据无需比较, 所以保存结果后直接返回                 
            klines_dict_bytype = {}
            klines_dict_bytype[kline_type] = [[symbol, kline_type, data_length], df_kline, bsuc, df_kline]
            self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol] = klines_dict_bytype

        # 不管是成功还是失败，每次均返回DataFrame对象：成功返回K线DataFrame，超时或者错误返回空DataFrame     
        return df_kline #如果接收数据失败，则会返回空df

    # ----------------------------------------------------------------------
    def fetch_ticks(self, symbol: str, data_length: int = 0) -> pd.DataFrame:
        return self.get_tick_serial(symbol, data_length)
    # ----------------------------------------------------------------------
    def get_tick_serial(self, symbol: str, data_length: int = 0) -> pd.DataFrame:
        doc="""
        获取tick序列数据(网际风分笔 3秒周期)

        请求指定证券代码的Tick序列数据. 序列数据会随着时间推进自动更新

        Args:
            symbol (str): 指定证券代码.

            data_length (int): 需要获取的序列长度。每个序列最大支持请求 8964 ?(待测试确认) 个数据

        Returns:
            pandas.DataFrame: 本函数总是返回一个 pandas.DataFrame 实例. 行数=data_length, 包含以下列:

            * id:       (k线序列号，从0开始 )    * DataFrame里 新增字段
            * label:    (证券代码)              * DataFrame里 新增字段
            * ktype:    (K线类型) = 分笔        * DataFrame里 新增字段 

            * time:     (K线起点时间戳UTC，单位:秒数)   *** time字段是唯一业务索引 ***
            * close:    (现价)
            * volume:   (成交量)
            * amount:   (成交额)
            * traceNum: (traceNum 是本次几个下单组成，按道理也是不断增加的，两次差值，本次)
            * pricebuy: (申买价) 12345
            * volbuy:   (申买量) 12345
            * pricesell:(申卖价) 12345
            * volsell:  (申卖量) 12345

        Example::

            # 获取 SH600000 的Tick序列
            from tqsdk import TqApi, TqAuth

            api = TqApi()
            serial = api.get_tick_serial("SH600000")
            while True:
                api.wait_update()
                print(serial.iloc[-1].pricebuy, serial.iloc[-1].pricesell)

            # 预计的输出是这样的:
            40860.0 41580.0
            40860.0 41580.0
            40820.0 41580.0
            ...
        """

        kline_type = NPSDKAPI_NEZIP_OEMTRACE #'分笔'
        return self.get_kline_serial(symbol, kline_type, data_length)


    # ----------------------------------------------------------------------
    # 接收npdatapool发来的数据
    def _receive_data_from_npdatapool(self) -> None:

        # 读完npdatapool发过来的所有数据包
        while self._response_queue.qsize() > 0:

            bsuc, npapiobjdes = self.get_response_queue() 
            if not bsuc: 
                continue 

            print('self._npsdkapi_requests_dict: len ',len(self._npsdkapi_requests_dict))
            if not npapiobjdes.correlation_id in self._npsdkapi_requests_dict.keys():
                logger.info('读取数据包异常：correlation_id不在发送命令记录里')
                continue 
            else:
                self._npsdkapi_requests_dict.pop(npapiobjdes.correlation_id) #删除该条命令请求记录

            if npapiobjdes.app_id == NPSDKAPI_APPID_NPSDKDATA and npapiobjdes.message_id == NPSDKAPI_MESSAGEID_GETQUOTE_RES:

                """
                # 实时行情
                """

                bsuc = False
                nowstr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                if npapiobjdes.response_type == NPSDKAPI_ERROR:   
                    # 向服务器发送命令后，收到命令响应（服务器端异常）。
                    logprtstr = " [.] get_quote() >> %s at: %s " %(npapiobjdes.response_body, nowstr)

                elif npapiobjdes.response_type == NPSDKAPI_SUCCESS:  
                    # 向服务器发送命令后，收到命令响应（服务器端正常）。

                    # 把字节流转成ctypes结构体
                    quote = OEM_REPORT() # 初始化结构体  
                    quote.decode(npapiobjdes.response_body)
                    symbol = quote.label
                    if not quote:
                        logprtstr = " [.] get_quote() >> 收到数据，但解码失败 %s(%s) at: %s" %(quote.label, quote.name, nowstr)
                    else:
                        bsuc = True
                        logprtstr = " [.] get_quote() >> 成功 %s(%s) at: %s" %(quote.label, quote.name, nowstr)
        
                        #记录本次结果更新, ，以便在is_changing中做对比
                        #列表第1项记录本次命令参数，第2项记录结果数据，第3项记录本次命令成功与否, 第4项纪录第一次get_quote(指定证券代码symbol)的结果df obj
                        lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol][3] 
                        self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol] = [[symbol], quote, bsuc, lastobj]
                    
                logger.info(logprtstr)
                #if self._npsdk_debug: print(logprtstr);  


            elif npapiobjdes.app_id == NPSDKAPI_APPID_NPSDKDATA and npapiobjdes.message_id == NPSDKAPI_MESSAGEID_GETKLINES_RES:           
                    
                """
                # K线数据
                """

                bsuc = False
                nowstr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                if npapiobjdes.response_type == NPSDKAPI_ERROR:   
                    # 向服务器发送命令后，收到命令响应（服务器端异常）。
                    logprtstr = " [.] get_kline_serial() >> %s at: %s " %(npapiobjdes.response_body, nowstr)

                elif npapiobjdes.response_type == NPSDKAPI_SUCCESS:  
                    # 向服务器发送命令后，收到命令响应（服务器端正常）。

                    result = parse.urlparse(npapiobjdes.request_body)
                    query_dict = parse.parse_qs(result.path)
                    symbol = query_dict.get('代码').pop()
                    kline_type = query_dict.get('类型').pop()
                    data_length = query_dict.get('数量').pop()

                    #把K线数据原始字节流转成pandas.DataFrame
                    #t = time.perf_counter()
                    df_kline = self._convert_klines_to_dataframe(symbol, kline_type, npapiobjdes.response_body) 
                    #print(f'_convert_klines_to_dataframe coast:{time.perf_counter() - t:.8f}s')
                    if len(df_kline.index) == 0: # df为空
                        logprtstr = " [.] get_kline_serial() >> 收到数据，但转DataFrame失败 %s(%s) at: %s" %(symbol, kline_type, nowstr)
                    else:
                        bsuc = True
                        logprtstr = " [.] get_kline_serial() >> 成功 %s(%s) at: %s" %(symbol, kline_type, nowstr)

                        #证券代码在二级字典并且K线类型在三级字典, 修改原纪录list
                        correlation_id = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][2]                       
                        lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][3]
                        if npapiobjdes.correlation_id == correlation_id:
                            self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type] = \
                                                [[symbol, kline_type, data_length], df_kline, correlation_id, lastobj] 
                        else:
                            logprtstr = " [.] get_kline_serial() >> 成功 但correlation_id不一致 %s(%s) (%s:%s) at: %s" %(symbol, kline_type, correlation_id, npapiobjdes.correlation_id, nowstr)

                logger.info(logprtstr)
                #if self._npsdk_debug: print(logprtstr);  

    # ----------------------------------------------------------------------
    # 检查历史命令请求里是否有get_quote命令,有则执行
    def _run_quotes_tasks(self) -> None:
        if len(self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ]) > 0:
            
            for symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ].keys():
                cmdpara = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol][0] #取出历史命令参数:列表
                #self.get_quote(cmdpara[0]) # 找到历史命令，再次执行
                self.fetch_quote(cmdpara[0]) # 找到历史命令，再次执行
                logger.info("执行历史命令：fetch_quote(%s)......"% cmdpara[0])

        else: # 在历史命令记录里，没有找到一条fetch_quote命令，这种情况为 refreshing()函数前从未执行过fetch_quote命令
            pass


    # ----------------------------------------------------------------------
    # 检查历史命令请求里是否有get_klines命令,有则执行
    def _run_klines_tasks(self) -> None:
        if len(self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ]) > 0:

            for symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ].keys():
                for klinetypekey in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol].keys():

                    """
                    
                    # 如果策略程序刚运行后，由于可能没取到数据，lastobj为空df，造成策略程序函数is_updated(df.iloc[-1])参数索引越界
                    # 所以如果lastobj为空df，就死等df_kline不为空df时，复制df_kline到lastobj
                    
                    #t1 = time.perf_counter()
                    bcontinue = False
                    while not bcontinue: 
                        df_kline = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][klinetypekey][1]
                        lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][klinetypekey][3]
                        
                        if len(lastobj.index) == 0 and len(df_kline.index) == 0:
                            pass
                        elif len(lastobj.index) == 0 and len(df_kline.index) != 0:
                            logger.info('df_columns_copy in refreshing(): (%s : %s)'%(symbol, klinetypekey))
                            self.df_columns_copy(lastobj, df_kline)
                            bcontinue = True
                        else:
                            bcontinue = True
                            
                        t = time.perf_counter()
                        cmdpara = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][klinetypekey][0] #取出历史命令参数:列表
                        #self.get_kline_serial(cmdpara[0], cmdpara[1], cmdpara[2]) # 找到历史命令，再次执行
                        self.fetch_klines(cmdpara[0], cmdpara[1], cmdpara[2]) # 找到历史命令，再次执行
                        coast =f'fetch_klines coast:{time.perf_counter() - t:.8f}s'

                        logger.info("执行历史命令：fetch_klines(%s,%s,%s)...coast(%s)...: "% (cmdpara[0], cmdpara[1], cmdpara[2], coast))
                        
                        if bcontinue: break
                    #print(f'111fetch_klines coast:{time.perf_counter() - t1:.8f}s')
                    
                    """
                   
                    #t = time.perf_counter()
                    cmdpara = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][klinetypekey][0] #取出历史命令参数:列表
                    #self.get_kline_serial(cmdpara[0], cmdpara[1], cmdpara[2]) # 找到历史命令，再次执行
                    self.fetch_klines(cmdpara[0], cmdpara[1], cmdpara[2]) # 找到历史命令，再次执行
                    #coast =f'fetch_klines coast:{time.perf_counter() - t:.8f}s'

                    logger.info("执行历史命令：fetch_klines(%s,%s,%s)......"% (cmdpara[0], cmdpara[1], cmdpara[2]))
   

        else: # 在历史命令记录里，没有找到一条fetch_klines命令，这种情况为 refreshing()函数前从未执行过fetch_klines命令
            pass
    
    # ----------------------------------------------------------------------
    def refreshing(self) -> None:
        return self.wait_update()
    # ----------------------------------------------------------------------
    def wait_update(self) -> None:
        doc="""
        执行 调用本次wait_update()函数前的 历史命令，等待业务数据更新:
        * 再次执行历史命令 get_quote, get_klines, get_ticks...（暂时不包括 交易命令）
        * 后续用户可以调用 api.is_changing(参数) 查询用户关心的数据

        Args:
            None

        Returns:
            bool: 如果收到业务数据更新则返回 True, 如果到截止时间依然没有收到业务数据更新则返回 False

        """

        try:
            #t = time.perf_counter()

            #print('等待数据更新......')

            #接收数据
            self._receive_data_from_npdatapool()
                                   
            # 检查历史命令请求里是否有get_quote命令,有则执行
            self._run_quotes_tasks()

            # 检查历史命令请求里是否有get_klines命令,有则执行
            self._run_klines_tasks()

            time.sleep(NPSDKAPI_CMD_TIMEOUT_VAL)

            #print(f'wait_update coast:{time.perf_counter() - t:.8f}s')

        except KeyboardInterrupt:
            print('Keyboard Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)

    """
    二个相同结构的DF的列值copy
    """
    def df_columns_copy(self, lastobj, df_kline):
        try:
            #清空DataFrame, 再按列赋值
            lastobj.drop(lastobj.index,inplace=True) 
            for col in df_kline.columns:
                lastobj[col] = df_kline[col]
            #lastobj.set_index(["time"], inplace=True) 
        except Exception as e:
            logger.warning('df_columns_copy() exception: %r'% e)
        
    # ----------------------------------------------------------------------
    def is_updated(self, obj: Any, propkey: list = None) -> bool:
        return self.is_changing(obj, propkey)
    # ----------------------------------------------------------------------
    def is_changing(self, obj: Any, propkey: list = None) -> bool:
        doc="""
        判定obj最近是否有更新

        对比 获取的数据响应 和 前一次的数据响应，返回业务数据更新标志

        当业务数据更新导致 wait_update 返回后可以使用该函数判断 **本次业务数据更新是否包含特定obj或其中某个字段** 。

        关于判断K线更新的说明：
        当生成新K线时，其所有字段都算作有更新，若此时执行 api.is_changing(klines.iloc[-1]) 则一定返回True。
        Args:
        obj (any): 任意业务对象, 包括 get_quote 返回的 quote, get_kline_serial 返回的 DataFrame, get_account 返回的 account 等

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



                """
                实时行情: 数据结构体定义详见 Stockdrv.py
                """
            if isinstance(obj, OEM_REPORT):
                # 要读取的属性字段 必须是正确的quote属性字段
                for pkey in propkey:
                    if not pkey in self._fieldsOfQuoteToCompared:
                        raise Exception("api.is_changing()函数参数2(%s)里有不正确的实时行情属性字段名"% propkey)
                        return False

                symbol = obj.label
                if symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ].keys():
                    #if  obj.label != symbol: 
                    #   continue

                    # 取出前一次获取的数据响应
                    current_quote = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol][1]
                    lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol][3]
                    bsuc = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][symbol][2]

                    #print('id(obj): %s id(lastobj): %s'%(id(obj),idoflastobj))
                    
                    # 如果前一次读取数据失败，再继续数据比较就没有意义，应该立即返回False
                    #if not bsuc:
                    #    logger.info('本次获取实时行情失败%s'%symbol)
                    #    return False
                    # 如果id(obj)不匹配，再继续数据比较就没有意义，应该立即返回False
                    if id(obj) != id(lastobj):
                        logger.warning('实时行情对象内存地址已改变%s %s %s'%(symbol, obj.label, lastobj.label))
                        return False
                        
                    """
                    类变量引用中的属性字段比较： 用id(obj)可以获得obj的引用内存地址
                    策略程序里的类变量引用的内存值（执行wait_update前获得的）： quote = api.get_quote("SH600000")
                    和
                    最新获得的类变量引用的内存值（执行wait_update后获得的）：self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ]["SH600000"]
                    """
                    if len(propkey) == 0: #比较所有属性字段
                        fieldsToCompared = self._fieldsOfQuoteToCompared
                    else: #比较参数2里的属性字段
                        fieldsToCompared = propkey
                    is_changed = False
                    for pkey in fieldsToCompared:
                        if getattr(obj, pkey) != getattr(current_quote, pkey): # 如果有属性字段数据更新，则更新obj所有属性字段
                            logstrprt = "   sdk>>> 实时行情 数据更新(代码: %s 名称: %s) (属性字段: %s [旧:新] = %s : %s)"% (current_quote.label, current_quote.name, pkey, getattr(obj, pkey), getattr(current_quote, pkey))
                            logger.info(logstrprt)
                            if self._npsdk_debug: print(logstrprt); 
                            is_changed = True
                    if is_changed:
                        #memmove(addressof(obj), addressof(current_quote), sizeof(OEM_REPORT)) # 更新策略程序里的类变量引用的内存值
                        memmove(addressof(lastobj), addressof(current_quote), sizeof(OEM_REPORT)) # 更新策略程序里的类变量引用的内存值
                        return True 
                    else:
                        return False

                else:
                    return False


            
                """
                DataFrame: DataFrame 是一个表格型的数据结构, 它可以被看做由Series组成的字典（共同用一个索引）
                Two-dimensional, size-mutable, potentially heterogeneous tabular data.
                1. K线数据
                2. ....
                """
            # Pandas Dataframe iloc[]->class 'pandas.core.series.Series'  
            # Pandas Dataframe->class 'pandas.core.frame.DataFrame'
            # DataFrame对象，仅比较最近二次取得的df是否一样，如果不一样，则检查最后一根K线的所有属性字段是否一样
            elif isinstance(obj, pd.DataFrame): 
                # 从DataFrame obj里取证券代码和K线类型
                try:
                    """
                    # K线Dataframe: 
                    """
                    if 'ktype' in obj.columns: 
                        for symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ].keys():
                            for kline_type in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol].keys():
                                
                                if id(obj) == id(self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][3]):
                                    
                                    #取出前一次读取K线成功后转成的DataFrame
                                    df_kline = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][1]
                                    lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][3]                            
                                    bsuc = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][2] #前一次读取K线是否成功

                                    if len(df_kline.index) == 0: #空DataFrame
                                        return False

                                    #if not bsuc: #本次从服务器读取读取K线失败，没必要继续比较
                                    #    return False

                                    # 如果id(obj)不匹配，再继续数据比较就没有意义，应该立即返回False
                                    #if id(obj) != id(lastobj):
                                    #    logger.info('is_changing()第一个参数不是数据更新对象或者被赋值过')
                                    #    return False
                                    
                                    #比较前后二个DataFrame是否完全一致
                                    if df_kline.equals(obj): 
                                        logger.info('前后二次收到的DataFrame一样')
                                        return False 
                                    else:
                                        """
                                        # # 以下情况判断K线发生变化 更新前一次收到的df
                                        # 1. 第一次没取到数据(空df) 
                                        # 2. 一根没走完的K线：在前后二次df中，该根K线的时间戳没变化但对应的属性字段发生变化
                                        # 3. 一根新K线：刚收到的df新增一根K线(时间戳变化了)
                                        """
                                        condition1 = (len(obj.index) == 0)
                                        condition2 = (not condition1) and (obj.iloc[-1]['time'] == df_kline.iloc[-1]['time'] and (not obj.iloc[-1].equals(df_kline.iloc[-1])))
                                        condition3 = (not condition1) and (obj.iloc[-1]['time'] != df_kline.iloc[-1]['time'])

                                        #print(condition1,condition2,condition3)

                                        if condition1 or condition2 or condition3:
                                            logstrprt = "   sdk>>> K线 数据更新(代码: %s K线类型: %s)"% (df_kline.iloc[-1].label, df_kline.iloc[-1].ktype)
                                            logger.info(logstrprt)
                                            if self._npsdk_debug: print(logstrprt); 

                                            self.df_columns_copy(lastobj, df_kline)
                                            """
                                            #清空DataFrame, 再按列赋值
                                            lastobj.drop(lastobj.index,inplace=True) 
                                            for col in df_kline.columns:
                                                lastobj[col] = df_kline[col]
                                            #lastobj.set_index(["time"], inplace=True)
                                            """

                                            """
                                            if condition2: 
                                               # 同根新值
                                                logstrprt = ">>>>>> K线 数据更新(代码: %s K线类型: %s) (***同根新值***)"% (df_kline.iloc[-1].label, df_kline.iloc[-1].ktype)
                                                print(logstrprt); logger.info(logstrprt)

                                            if condition3: 
                                                # 新增一根
                                                logstrprt = ">>>>>> K线 数据更新(代码: %s K线类型: %s) (***新增一根***)"% (df_kline.iloc[-1].label, df_kline.iloc[-1].ktype)
                                                print(logstrprt); logger.info(logstrprt)
                                            """                                            

                                            return True
                                    break
                                else:
                                    pass
                        return False

                    else:
                        logger.warning('非K线数据的DataFrame对象')
                        return False

                except Exception as e:
                    logger.warning('isinstance(obj, pd.DataFrame) exception: %r'% e)
                    return False


                """
                #Series: Series 是一种类似于一维数组的对象.它由一组数据（各种Numpy数据类型）
                以及一组与之相关的数据标签（即索引）组成。
                One-dimensional ndarray with axis labels (including time series).
                1. K线数据
                2. ....
                """               
            elif isinstance(obj, pd.Series):
                # 从Series obj里取证券代码和K线类型
                #print('Series Object: id=%s\r\nname:\r\n%s\r\nindex:\r\n%s\r\nobj:\r\n%r'%(id(obj), obj.name, obj.index, obj))
                try:
                    """
                    # K线Series: 对应K线Series对象，is_changing函数参数1格式应该输入：df.iloc[-1]，
                    # 其它(df.iloc[0], df.iloc[-2]之类的输入)没有意义
                    """
                    if 'ktype' in obj.index: 

                        if obj['ktype'] == NPSDKAPI_NEZIP_OEMTRACE: #'分笔'
                            fieldsOfObj = self._fieldsOfTrace
                        else:
                            fieldsOfObj = self._fieldsOfKline

                        if len(propkey) == 0: #比较所有属性字段
                            fieldsToCompared = fieldsOfObj
                        else: #比较参数2里的属性字段
                            fieldsToCompared = propkey

                        # 要读取的属性字段 必须是正确的kline属性字段
                        for pkey in propkey:
                            if not pkey in fieldsOfObj:
                                raise Exception("api.is_changing()函数参数2(%s)里有不正确的K线数据属性字段名"% propkey)
                                return False

                        for symbol in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ].keys():
                            if  obj['label'] != symbol: continue

                            for kline_type in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol].keys():
                                if obj['ktype'] != kline_type: continue

                                #取出前一次读取K线成功后转成的DataFrame
                                df_kline = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][1]
                                lastobj = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][3]                            
                                bsuc = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETKLINES_REQ][symbol][kline_type][2] #前一次读取K线是否成功

                                #print('obj%s %s %s'%(obj["label"], obj["ktype"], obj["time"]))
                                #print('lastobj%s %s %s'%(lastobj.iloc[-1]["label"], lastobj.iloc[-1]["ktype"], lastobj.iloc[-1]["time"]))
                                
                                if len(df_kline.index) == 0: #空DataFrame
                                    return False

                                #if not bsuc: #前一次从服务器读取读取K线失败，没必要继续比较
                                #    return False

                                if obj.values in lastobj.values: #检查Series是否在前一次DataFrame中
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
                                                    logstrprt = "   sdk>>> K线指定行字段 数据更新(代码: %s K线类型: %s) (属性字段: %s [旧:新] =  %s : %s)"% (symbol, kline_type, filed, obj[filed], df_kline.iloc[index_des][filed])
                                                    logger.info(logstrprt)
                                                    if self._npsdk_debug: print(logstrprt); 
                                                    is_changed = True
                                                    condition2 = True


                                        #condition2 = is_changed
                                        #condition3 = df_kline.iloc[-1]['time'] > df_kline.iloc[index_des]['time']

                                        if condition2 or condition3: 
                                            self.df_columns_copy(lastobj, df_kline)

                                            """
                                            #清空DataFrame, 再按列赋值
                                            lastobj.drop(lastobj.index,inplace=True) 
                                            for col in df_kline.columns:
                                                lastobj[col] = df_kline[col]
                                            #lastobj.set_index(["time"], inplace=True) 
                                            """

                                            """
                                            if condition2: 
                                                # 同根新值
                                                logstrprt = ">>>>>> K线指定行字段 数据更新(代码: %s K线类型: %s) (***同根新值***)"% (symbol, kline_type)
                                                print(logstrprt); logger.info(logstrprt)

                                            if condition3: 
                                                # 新增一根
                                                logstrprt = ">>>>>> K线指定行字段 数据更新(代码: %s K线类型: %s) (***新增一根***)"% (symbol, kline_type)
                                                print(logstrprt); logger.info(logstrprt)
                                            """
                                             
                                            return True 

                                    else: 
                                        logger.warning('该K线Series的时间戳不在刚收到的DataFrame中 %s  %s'%(obj['label'],obj['ktype']))
                                        #return False
                                        continue

                                else: 
                                    logger.warning('该K线Series对象不在前一次DataFrame中 %s  %s'%(obj['label'],obj['ktype']))
                                    continue
                                    #return False
                        
                        #print('---is_changing---(%s : %s): No'%(obj['label'], obj['ktype']))      
                        return False

                    else: #其它业务类型的Series obj
                        logger.warning('非K线数据的Series对象')
                        return False
                except Exception as e:
                    logger.warning('isinstance(obj, pd.Series) exception: %r'% e)
                    return False
                

            else: 
                print("不支持的(参数1)业务对象：%s"%type(obj))
                return False

            return False

        except Exception as e:  
            logger.warning('is_changing() exception: %r'% e)
            return False
        #except (KeyError, IndexError):
        #    return False




    """
    Utilities: for pd.Dataframe
    """
    # ----------------------------------------------------------------------
    # 把K线数据字节流转成DataFrame
    def _convert_klines_to_dataframe(self, symbol, kline_type, klines_rawdata)->pd.DataFrame:
        try:
            df_kline = None

            if kline_type == NPSDKAPI_NEZIP_OEMTRACE: #'分笔'
                fieldsOfObj = self._fieldsOfTrace
                unitsize = sizeof(OEM_TRACE)
            else:
                fieldsOfObj = self._fieldsOfKline
                unitsize = sizeof(RCV_KLINE)
            
            count = int(len(klines_rawdata) / unitsize) # 计算多少根K线
            kline_list_des = []
            for i in range(count):
                if kline_type == NPSDKAPI_NEZIP_OEMTRACE:
                    klinebody = OEM_TRACE()
                else:
                    klinebody = RCV_KLINE()
                klinebody.decode(klines_rawdata)                        #字节流 转成 ctypes结构体
                klines_rawdata = klines_rawdata[unitsize : ]   #删除一个结构体
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

            #创建DataFrame:行索引默认从0开始，列索引里加入K线序列号id，也是从0开始, 指定证券代码, K线类型
            if len(kline_list_des) == 0: #创建空df
                df_kline = pd.DataFrame(columns=['id', 'label', 'ktype'] + fieldsOfObj)
            else:
                df_kline = pd.DataFrame(kline_list_des, columns=['id', 'label', 'ktype'] + fieldsOfObj)
            #df_kline.set_index(["time"], inplace=True)
            
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
                
                for field in self._fieldsOfKline:
                    if field == 'time': #localize the timestamp to local datetime
                        val = time.localtime(klinebody.time)
                        val = time.strftime('%y-%m-%d %H:%M:%S', val)
                    else:
                        val = getattr(klinebody, field)
                    fieldval.append(val)
                    #fieldval.append(getattr(klinebody, field))
                kline_list_des.append(fieldval)

            #df_kline = pd.DataFrame(kline_list_des, columns=['id', 'label', 'ktype'] + self._fieldsOfKline)

        except Exception as e:
            logger.warning('_convert_klines_to_datalist() exception: %r'% e)
        finally:
            return kline_list_des

    # ----------------------------------------------------------------------
    def _compare_update_klines_datalist(self, kline_list_src, kline_list_des)-> list:
        # todo:
        return kline_list_des



    """
    # ----------------------------------------------------------------------
    # 检查历史命令请求里是否有get_quote命令,有则执行并检查是否有数据更新
    def _is_quotes_changing(self) -> bool:
 
        if len(self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ]) > 0:
            
            print("历史实时行情命令：%s条"% len(self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ]))
            
            for key in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ].keys():
                # 逐条执行历史实时行情命令，并检查是否有更新

                # 取出最近一次获取的数据响应
                last_quote = self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ][key]
                
                current_quote = self.get_quote(key) # 找到历史命令，再次执行，获取最新数据

                print("执行历史命令：get_quote(%s)......: "% key)

                # 初始化 Quote是否有数据更新标志字典
                d = {}; 
                for field in self._fieldsOfQuoteToCompared:
                    d[field] = False
                self._quotes_items_changed[key] = d

                # 逐项比较二个Quote对应的属性字段值是否一样
                for field in self._fieldsOfQuoteToCompared:
                    if getattr(last_quote, field) != getattr(current_quote, field):
                       self._quotes_items_changed[key][field] = True
                       print("实时行情有更新...(%s): (%s) changed"% (current_quote.label, field))

                
            for key in self._npsdkapi_cmd_request_response[NPSDKAPI_MESSAGEID_GETQUOTE_REQ].keys():
                    for field in self._fieldsOfQuoteToCompared:
                        if self._quotes_items_changed[key][field]:
                            return True
 
            print("实时行情没有更新...(%s): "% current_quote.label)
                

        else: # 在历史命令记录里，没有找到一条get_quote命令，这种情况为 wait_update()函数前从未执行过get_quote命令
            return False

        return False
    """
        
            

#:!/usr/bin/env python
#:  -*- coding: utf-8 -*-
__author__ = 'nfjd'

"""
NPSDK 全局常量定义
"""
NPSDKAPI_LEGAL_TIPS ='友情提示：在使用npsdk之前，默认您已经知晓并同意以下免责条款，如果不同意请立即停止使用 http://wwww.npsdk.com'

NPSDKAPI_APPID_NPSDKAPI  = 'npsdkapi'
NPSDKAPI_APPID_NPSDKDATA = 'npsdkdata'

NPSDKAPI_MESSAGEID_GETQUOTE_REQ     = 'get_quote'
NPSDKAPI_MESSAGEID_GETQUOTE_RES     = 'get_quote_response'
NPSDKAPI_MESSAGEID_GETKLINES_REQ    = 'get_klines'
NPSDKAPI_MESSAGEID_GETKLINES_RES    = 'get_klines_response'
#NPSDKAPI_MESSAGEID_GETTICKS_REQ    = 'get_ticks'
#NPSDKAPI_MESSAGEID_GETTICKS_RES    = 'get_ticks_response'

NPSDKAPI_MESSAGEID_WSCOMMAND_REQ    = 'ws_command_request'
NPSDKAPI_MESSAGEID_WSCOMMAND_RES    = 'ws_command_response'    

NPSDKAPI_SIGNAL_DATA_CODES = 1
NPSDKAPI_SIGNAL_DATA_QUOTES = 2
NPSDKAPI_SIGNAL_DATA_KLINES = 3

NPSDKAPI_SUCCESS        = 'npsdkapi_success'
NPSDKAPI_ERROR          = 'npsdkapi_error'
NPSDKAPI_TIMEOUT        = 'npsdkapi_timeout'

NPSDKAPI_CMD_TIMEOUT_VAL        = 0.5 # 单位:secs, 命令请求超时阈值

NPSDKAPI_THREAD_SLEEP_VAL       = 0.01 # 单位:secs, npdatapoolthread sleep值

#NPSDKAPI_NUM_OF_NPDATAPOOLTHREAD = 2 # 启动多少个npdatapoolthread

#NPSDKAPI_WS_CONNECTED           = 10
#NPSDKAPI_WS_DISCONNECTED        = 0

NPSDKAPI_QUEUESZIE_WARNING      = 5000 # Websocket接收数据队列大小监控阈值，队列长度太大表示消息已经堆积

NPSDKAPI_PLAYMODE_NPWEBSOCKET     = 'nezipwebsocket'   
NPSDKAPI_PLAYMODE_NPBACKTEST     = 'backtestwebsocket'

NPSDKAPI_NEZIP_EXEFILE = '网际风.exe'

NPSDKAPI_NEZIP_QUOTE = '实时数据'
NPSDKAPI_NEZIP_KTYPES = ["1分钟线", "5分钟线", "15分钟线", "30分钟线", "60分钟线", "日线", "周线", "月线", "季线", "年线", "多日线"]
NPSDKAPI_NEZIP_OEMTRACE = '分笔'

NPSDKAPI_TIMER_1SECS = 1  # secs
NPSDKAPI_TIMER_3SECS = 3  # secs


""" Pika BasicProperties  详细说明https://www.pianshen.com/article/19451397954/
        content_type            用于描述消息内容的数据格式，如：text/plain
        content_encoding        消息内容编码
        headers                 设置消息的header,类型为Map<String,Object>
        delivery_mode           1（nopersistent）非持久化，2（persistent）持久化
        priority                消息的优先级
        correlation_id          关联ID
        reply_to                用于指定回复的队列的名称
        expiration              消息的失效时间
        message_id              消息ID
        timestamp               消息的时间戳
        type                    类型
        user_id                 用户ID
        app_id                  应用程序ID
        cluster_id              集群ID

"""

class AplEventContentObject(object):
    """ 
    NpSdkApi user-defined event content object class (application level)
    """
    def __init__(self):
        self.apl_tid_source = ''        # application level ID,such as 'DATA_POOL_ID','WS_SOCKET_ID','NPSDKAPI_ID'
        self.apl_tid_destination = ''
        self.apl_event_name = None
        self.apl_event_body = ''


class NpApiDataObj(object):
    """ 
    NpApi user-defined data object class 
    """
    def __init__(self):
        
        """
        应用ID, 用来标识应用 
                * NPSDKAPI_APPID_NPSDKAPI   策略程序
                * NPSDKAPI_APPID_NPSDKDATA  数据服务器
                * ......
        """
        self.app_id = None

        """
        消息ID，用来描述函数命令： 
                * NPSDKAPI_MESSAGEID_GETQUOTE_REQ            获取实时数据命令
                * NPSDKAPI_MESSAGEID_GETQUOTE_RES            获取实时数据响应
                * NPSDKAPI_MESSAGEID_GETKLINES_REQ           获取K线数据命令
                * NPSDKAPI_MESSAGEID_GETKLINES_RES           获取K线数据响应
                *
                * NPSDKAPI_MESSAGEID_WSCOMMAND_REQ           ws command 请求命令
                * NPSDKAPI_MESSAGEID_WSCOMMAND_RES           ws command 请求命令响应
                * .....
        """
        self.message_id = None 

        # 回复队列
        self.reply_to = None

        #: 关联ID
        self.correlation_id = None

        #: 请求消息参数体
        self.request_body = None

        """
        响应消息类型, 对应BasicProperties里的tpye
                * NPSDKAPI_SUCCESS:     命令请求成功
                * NPSDKAPI_ERROR:       命令请求出错
                * NPSDKAPI_TIMEOUT:     命令请求超时
                * ....
        """
        self.response_type = None 

        #: 响应消息体
        self.response_body = None




# 28个字段定义见 Stockdrv.py文件里的 class OEM_REPORT(Structure):
# 可以通过语句注释功能来选择字段判断实时行情数据是否更新    
fieldsOfQuoteToCompared = [      #实时行情 400 字节，带*表示网际风内部数据，请跳过
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
fieldsOfKline = [      
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
fieldsOfTrace = [      
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


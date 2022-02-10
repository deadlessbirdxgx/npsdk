import selectors
import socket
import hashlib
import base64
import struct
import sys
import os
import time
from urllib import parse

from npsdkobjs import *
from Stockdrv import *

sel = selectors.DefaultSelector()
clients = {}
clients_count = 0

# 准备回放的数据包
glb_marketinfo_dict = {} # 市场表 数据字典
                                # {  data = head+body                    
                                #   mkId: [data,data...,data],
                                #   mkId: [data,data...,data],        
                                # }
glb_marketinfo_dict.clear()

glb_stk_report_dict = {} # 证券实时行情 数据字典 每个证券代码 对应一个历史数据列表
                                # {  data = head+body
                                #   'SH600000':[data,data...,data],
                                #   'SH600007':[data,data...,data],
                                #   ......
                                #   'SH600100':[data,data...,data],
                                #   'SH600101':[data,data...,data],
                                # }
glb_stk_report_dict.clear()

#["1分钟线", "5分钟线", "15分钟线", "30分钟线", "60分钟线", "日线", "周线", "月线", "季线", "年线", "多日线"] 还包括 "分笔"
glb_stk_klines_dict = {} # 证券K线数据 数据字典 每个证券代码的不同K线类型 对应一个历史数据列表
                                # {   data = head+body                     
                                #   'SH600000':{'1分钟线':[data,data...,data],    
                                #               '5分钟线':[data,data...,data],  
                                #               ......
                                #              },
                                #   ......
                                #   'SH600007':{'1分钟线':[data,data...,data], 
                                #               '日线':   [data,data...,data],  
                                #               ......
                                #              },
                                # }
glb_stk_klines_dict.clear()

kline_types_list = ["分笔","1分钟线", "5分钟线", "15分钟线", "30分钟线", "60分钟线", "日线", "周线", "月线", "季线", "年线", "多日线"]


report_playindex_dict = {} # 实时行情数据 数据字典 每次取一个数据包，playindex = 0 ~ len(dict)
                        # {                  
                        #   'SH600000':playindex,    
                        #   'SH600007':playindex, 
                        #               ......
                        # }

klines_playindex_dict = {} # 证券K线数据 数据字典 每次取一个数据包，playindex = 0 ~ len(ktype dict)
                        # {                  
                        #   'SH600000':{'1分钟线':playindex,    
                        #               '5分钟线':playindex,  
                        #               ......
                        #              },
                        #   ......
                        #   'SH600007':{'1分钟线':playindex, 
                        #               '日线':   playindex,  
                        #               ......
                        #              },
                        # }
                        
"""
初始化数据包回放 索引值字典
""" 
def init_playindex_dicts():        
    #初始化实时行情数据包回放 索引值字典
    t = time.perf_counter()
    for symbol in glb_stk_report_dict.keys():
        report_playindex_dict[symbol] = 0
    print(f'report coast:{time.perf_counter() - t:.8f}s')

    #初始化K线数据包回放 索引值字典
    t = time.perf_counter()
    for symbol in glb_stk_klines_dict.keys():
        if not symbol in klines_playindex_dict.keys():
            klines_playindex_dict[symbol] = {}
        
            for type in glb_stk_klines_dict[symbol].keys():
                klines_playindex_dict[symbol][type] = 0
    print(f'klines coast:{time.perf_counter() - t:.8f}s')


"""
从文件里读取回测数据包
""" 
def prepare_backtest_dicts(filename):
    # 数据包回放测试。把数据包压入回测队列queue
    print('>>>开始准备装载数据......')
    filesize = os.path.getsize(filename)  
    with open(filename,'rb') as fo:
        while True:
            #t = time.perf_counter()
            datahead = fo.read(sizeof(OEM_DATA_HEAD))
            if datahead:
                wshead_unit = OEM_DATA_HEAD()
                wshead_unit.decode(datahead)  #把字节流 转成 ctypes结构体
                #wsheadinfo = "ws data head info:\r\n[oemVer:%s] x [type:%s] x [label:%s] x [name:%s] x [flag:%s] x [askId:%s] x [len:%s] x [count:%s]" \
                #    % (wshead_unit.oemVer, wshead_unit.type, wshead_unit.label, wshead_unit.name, wshead_unit.flag, wshead_unit.askId, wshead_unit.len, wshead_unit.count)
                #print(wsheadinfo)
                    
                databody = fo.read(wshead_unit.len)
                
                if databody:
            
                    #分析数据包：市场代码表，实时行情 和 K线数据

                    #-"""市场代码表数据处理"""-#
                    if wshead_unit.type == '代码表':
                        len_to_do = sizeof(OEM_MARKETINFO) - 10 * sizeof(OEM_STKINFO) + wshead_unit.count * sizeof(OEM_STKINFO)
                        if len(databody) == 0 or \
                        len(databody) != len_to_do:
                            print('错误消息: 接收到的市场消息体长度为0 或者 实际长度不等于应收长度')
                            return 

                        marketbody = OEM_MARKETINFO()
                        marketbody.decode(databody)               #字节流 转成 ctypes结构体
 
                        if not marketbody.mkId in glb_marketinfo_dict.keys():
                            glb_marketinfo_dict[marketbody.mkId] = []
                        glb_marketinfo_dict[marketbody.mkId].append(datahead + databody)  


                    #-"""实时行情数据处理"""-#
                    elif wshead_unit.type == '实时数据':
                        if len(databody) == 0 or len(databody) != wshead_unit.count * sizeof(OEM_REPORT):
                            print('错误消息: 接收到的实时行情消息体长度为0 或者 实际长度不等于count*sizeof(OEM_REPORT)')
                            return 
                        for i in range(wshead_unit.count):
                            reportbody = OEM_REPORT()
                            reportbody.decode(databody)               #字节流 转成 ctypes结构体

                            if not reportbody.label in glb_stk_report_dict.keys():
                                glb_stk_report_dict[reportbody.label] = []
                            glb_stk_report_dict[reportbody.label].append(datahead + databody)  

                            break # 只循环一次，为了获取证券代码

                    #-"""K线数据处理"""-#
                    elif wshead_unit.type in kline_types_list: 
                        unitsize = sizeof(RCV_KLINE)
                        if wshead_unit.type == NPSDKAPI_NEZIP_OEMTRACE:
                            unitsize = sizeof(OEM_TRACE)

                        if len(databody) == 0 or len(databody) != wshead_unit.count * unitsize:
                            print('错误消息: 接收到的K线数据消息体长度为0 或者 实际长度不等于count * unitsize')
                            return

                        symbol = wshead_unit.label       #证券代码
                        klines_type = wshead_unit.type   #Klines类型字符串

                        #以证券代码为key值 更新K线数据一级字典。以K线类型为key值 更新K线数据二级字典。
                        if  symbol in glb_stk_klines_dict.keys():
                            if not klines_type in glb_stk_klines_dict[symbol].keys():
                                glb_stk_klines_dict[symbol][klines_type] = []
                            glb_stk_klines_dict[symbol][klines_type].append(datahead + databody) 
                        else: # 该证券代码第一次传送K线数据
                            klines_dict = {}
                            klines_dict[klines_type] = []
                            klines_dict[klines_type].append(datahead + databody) 
                            glb_stk_klines_dict[symbol] = klines_dict                   

                else: # 已经读到文件尾
                    break

            else: # 已经读到文件尾
                break
            #print(f'coast:{time.perf_counter() - t:.8f}s')
    fo.close()

    print('>>>数据装载完成。数据情况汇总：')
    #print('实时行情字典keys:%s'%glb_stk_report_dict.keys())

    """
    # 在for...字典keys()循环里，用key去比较 (错误的方式) 
    # Python3 字典 keys() 方法返回一个视图对象。
    # 注意：Python2.x 是直接返回列表

    for sybmol in glb_stk_report_dict.keys():
        if sybmol == 'SH600000' or sybmol == 'SH600007' or sybmol == 'SH600010': # 错误的比较方式， 这里 if symbol == 'SH600000' 失效
        if str(sybmol) == 'SH600000' or str(sybmol) == 'SH600007' or str(sybmol) == 'SH600010': # 正确的比较方式
            print('代码: %s  数据长度: %s'%(sybmol, len(glb_stk_report_dict[sybmol])))
    """
    
    for mkid in glb_marketinfo_dict.keys():
        print('>>>市场代码表: %s  数据长度: %s'%(mkid, len(glb_marketinfo_dict[mkid])))

    for sybmol, value in glb_stk_report_dict.items():
        if sybmol == 'SH600000' or sybmol == 'SH600007' or sybmol == 'SH600010': 
            print('>>>实时行情 代码: %s  数据长度: %s'%(sybmol, len(glb_stk_report_dict[sybmol])))

    print('>>>K线数据字典keys: %s'%glb_stk_klines_dict.keys())
    for sybmol in glb_stk_klines_dict.keys():
        print(glb_stk_klines_dict[sybmol].keys())
        for ktype in glb_stk_klines_dict[sybmol].keys():
            print('>>>代码: %s K线类型: %s 数据长度: %s'%(sybmol, ktype, len(glb_stk_klines_dict[sybmol][ktype])))

"""
处理client发来的业务命令
""" 
def process_command_pro(conn, command):

    # 解析参数
    result = parse.urlparse(command)
    query_dict = parse.parse_qs(result.query)
    
    type = query_dict.get('类型').pop()

    # 股票数据?类型=初始化&编码=unicode&版本=20210310
    if type == '初始化':
        print('初始化请求命令')

        # 发送 全部市场代码表
        for mkid in glb_marketinfo_dict.keys():
            for i in range(len(glb_marketinfo_dict[mkid])):
                raw_data = glb_marketinfo_dict[mkid][i]
                send_1(conn, raw_data)

    # 股票数据?代码=SH600007&类型=实时数据&版本=20210310
    elif type == '实时数据':
        symbol =  query_dict.get('代码').pop()
        print('实时行情请求命令: %s'%symbol)

        if symbol in glb_stk_report_dict.keys():
            rindex = report_playindex_dict[symbol]

            if rindex == len(glb_stk_report_dict[symbol]): #该代码实时行情的数据包已经回放结束
                print('(%s) 实时行情数据包回放结束'%(symbol,type))
            else: 
                print('(%s) 实时行情数据包回放第 %s个包'%(symbol,rindex))
                raw_data = glb_stk_report_dict[symbol][rindex]
                send_1(conn, raw_data)
                report_playindex_dict[symbol] = report_playindex_dict[symbol] + 1            

    # 股票数据?代码=SH600000&类型=1分钟线&数量=2000&版本=20210310
    elif type in kline_types_list:
        symbol =  query_dict.get('代码').pop()
        datalength =  query_dict.get('数量').pop() 
        print('K线数据请求命令: %s : %s : %s'%(symbol, type, datalength))

        if symbol in glb_stk_klines_dict.keys():
            if type in glb_stk_klines_dict[symbol].keys():
                kindex = klines_playindex_dict[symbol][type]

                if kindex == len(glb_stk_klines_dict[symbol][type]): #该代码该K线类型的数据包已经回放结束
                    print('(%s : %s) K线数据包回放结束'%(symbol,type))
                else: 
                    print('(%s : %s) K线数据包回放第 %s个包'%(symbol,type,kindex))
                    raw_data = glb_stk_klines_dict[symbol][type][kindex]
                    send_1(conn, raw_data)
                    klines_playindex_dict[symbol][type] = klines_playindex_dict[symbol][type] + 1
    else:
        print('其它命令：', command)
        pass

"""
utilities: websocket handshake / receive / send
"""
def send_1(conn, message):
    try:
        reply = parse_send_data_str(message) 
        conn.sendall(reply)

    except Exception as e:
        print('send_1() exception: %r'% e)


def send_all(clients, message):
    reply = parse_send_data_str(message) 
    for x in clients.values():
        x.sendall(reply)

def parse_send_data_str(data):
    if data:
        data = str(data)
    else:
        return ''#False
    token = b"\x81"
    length = len(data.encode('utf-8')) 
    if length < 126:
        token += struct.pack("B", length)
    elif length <= 0xFFFF:
        token += struct.pack("!BH", 126, length)
    else:
        token += struct.pack("!BQ", 127, length)

    data = b'%s%s' % (token, data.encode('utf-8'))   

    return data

def parse_send_data_bytes(data):
    if data:
        pass
    else:
        return b''
    token = b"\x81"
    length = len(data) 
    if length < 126:
        token += struct.pack("B", length)
    elif length <= 0xFFFF:
        token += struct.pack("!BH", 126, length)
    else:
        token += struct.pack("!BQ", 127, length)

    data = token + data

    return data

def parse_recv_data(msg):
    v = msg[1] & 0x7f
    if v == 0x7e:
        p = 4
    elif v == 0x7f:
        p = 10
    else:
        p = 2
    mask = msg[p:p + 4]
    data = msg[p + 4:]
    #return ''.join([chr(v ^ mask[k % 4]) for k, v in enumerate(data)])  # 原始语句，因为中文乱码，改成以下方式

    str_bytearray = bytearray()
    for k, v in enumerate(data):
        str_bytearray.append(v ^ mask[k % 4])
    raw_str = str(str_bytearray, encoding='utf-8')
    return raw_str  

def generate_token(msg):
    key = str(msg) + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
    key = key.encode()
    ser_key = hashlib.sha1(key).digest()
    return base64.b64encode(ser_key)

def handshake(conn):
    print('start hanshake !')
    headers = {}
    shake = conn.recv(1024).decode('utf-8')
    if not len(shake):
        return False
    print(' len  ')
    header, data = shake.split('\r\n\r\n', 1)
    for line in header.split('\r\n')[1:]:
        key, value = line.split(': ', 1)
        headers[key] = value
    if 'Sec-WebSocket-Key' not in headers:
        print('no Sec-WebSocket-Key')
        conn.close()
        return False

    sec_key = headers['Sec-WebSocket-Key']
    res_key = generate_token(sec_key)
    ret_key = res_key.decode()
    ret_origin = headers['Origin']
    ret_host = headers['Host']
    handshake_string = "HTTP/1.1 101 Switching Protocols\r\n" \
                       "Upgrade:websocket\r\n" \
                       "Connection: Upgrade\r\n" \
                       "Sec-WebSocket-Accept: {1}\r\n" \
                       "WebSocket-Origin: {2}\r\n" \
                       "WebSocket-Location: ws://{3}/\r\n\r\n"

    str_handshake = handshake_string.replace('{1}', ret_key).replace('{2}', ret_origin).replace('{3}', ret_host)
    b_handshake = str_handshake.encode()
    conn.sendall(b_handshake)

    print(' Socket handshaken with success')
    return True

"""
one client connected
"""
def accept(sock, mask):
    conn, addr = sock.accept()  # Should be ready

    global clients
    global clients_count

    handshake(conn)
    client_name = 'C' + str(clients_count)
    print('accept new client: ', client_name)

    clients[client_name] = conn
    clients_count += 1

    print('accepted', conn, 'from', addr)
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

    init_playindex_dicts()



"""
one client received data
"""
def read(conn, mask):
    try:

        for k, v in clients.items():
            if v == conn:
                #print('client: (%s) received data'%k)
                pass

        data = conn.recv(1000)  # Should be ready
        if data:
            recv = parse_recv_data(data)
            #print('Receiving', recv)
            process_command_pro(conn, recv)

            #print('echoing', repr(data), 'to', conn)
            #conn.send(data)  # Hope it won't block
        else:
            print('closing', conn)
            sel.unregister(conn) # 对于linux系统: 如果收到的消息为空,注销conn对象,
            conn.close()

    #except ConnectionResetError as e:
    except Exception as e:
        print('read() exception: %r'% e)
        sel.unregister(conn)  # 对于windows系统: 连接突然中断,注销conn对象,
        conn.close()


"""
main routine
"""

prepare_backtest_dicts('backtest0607-1.dat')   


sock = socket.socket()
#sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
print('socket set up')
sock.bind(('localhost', 6889))
print('socket binded')
sock.listen(10)
sock.setblocking(False)
sel.register(sock, selectors.EVENT_READ, accept)

while True:
    events = sel.select()
    for key, mask in events:
        callback = key.data
        callback(key.fileobj, mask)



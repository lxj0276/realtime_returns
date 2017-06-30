
import threading

from gmsdk import md
import numpy as np

# from global_vars import *
from portfolio_class import *
from new_thread.new_thread import *
from raw_holding_process import rawholding_stocks


# def wind2gm(undllst,endmark='.tick'):
#     """ 转换为标的代码wind代码 为掘金代码 """
#     gm_dict = {'SH':'SHSE','SZ':'SZSE','CFE':'CFFEX'}
#     undl_new = []
#     for undl in undllst:
#         temp = undl.split('.')
#         undl_new.append(''.join([gm_dict[temp[1]],'.',temp[0],endmark]))
#     return undl_new

def addfix(undl):
    if undl[0] in ('0','3'):
        return  '.'.join([undl,'SZ'])
    elif undl[0] in ('6'):
        return  '.'.join([undl,'SH'])
    elif undl[0:2] in ('IF','IC','IH'):
        return  '.'.join([undl,'CFE'])

def data_subscribe(source):
    """  数据源订阅 , pool_columns 提供需要订阅的字段，需要更新 undl_pool_info """
    COLNUM = len(POOL_COLUMNS)
    if source=='wind':
        # 定义数据源对应 callback 函数
        def wind_callback(indata):
            if indata.ErrorCode!=0:
                raise Exception('Error in callback with ErrorCode %d' %indata.ErrorCode)  # 实际使用时，为防止中断可改为log输出
            for dumi in range(len(indata.Codes)):
                fieldlen=len(indata.Fields)
                if fieldlen==COLNUM:   # 只有在所有field都有数据的时候才存储
                    tempdata = []
                    for dumj in range(fieldlen):
                        tempdata.append(indata.Data[dumj][dumi])
                    UNDL_POOL_INFO[indata.Codes[dumi]] = tempdata
        w.start()
        underlyings = list(UNDL_POOL['total'])
        w.wsq(','.join(underlyings),','.join(POOL_COLUMNS),func = wind_callback)
        print('data subscribed with source %s' % source)
    elif source=='goldmine':
        vars = {'rt_last':'tick.last_price','rt_time':'tick.str_time'}
        def on_tick(tick):
            tempdata = []
            for col in POOL_COLUMNS:
                tempdata.append(eval(vars[col]))
            UNDL_POOL_INFO[addfix(tick.sec_id)] = tempdata
        # 提取当前资产池中的代码，并转换为gm所需格式
        underlyings = list(UNDL_POOL['total'])
        ret = md.init(
                username="18201141877",
                password="Wqxl7309",
                mode= 3)
        if ret != 0:
            raise Exception('Error in initiation with ErrorCode %d' % ret)
        ret = md.subscribe(','.join(underlyings))
        if ret != 0:
            raise Exception('Error in subscribe with ErrorCode %d' % ret)
        # 添加回调函数
        md.ev_tick += on_tick
        # 初始填充POOL，确保不会出现 NAN
        fillundl = ','.join(underlyings).replace('.tick','')
        ticks = md.get_last_ticks(fillundl)
        for tick in ticks:
            tempdata = []
            for col in POOL_COLUMNS:
                tempdata.append(eval(vars[col]))
            #UNDL_POOL_INFO[addfix(tick.sec_id)] = tempdata
            UNDL_POOL_INFO[rawholding_stocks.addfix(tick.sec_id)] = tempdata
        # 加入线程
        data_thread = threading.Thread(target=md.run)
        if not PRE_THREADS.get(source):  # 如果是第一次建立线程则创建，否则只要重新订阅
            PRE_THREADS[source]=data_thread
            data_thread.start()
        else:
            ret = md.resubscribe(','.join(underlyings))
            if ret != 0:
                raise Exception('Error in subscribe with ErrorCode %d' % ret)
        print(threading.enumerate())
        print('data subscribed with source %s' % source)
    elif source=='goldmine_snapshot':
        vars = {'rt_last':'tick.last_price','rt_time':'tick.str_time'}
        def pull_ticks():
            underlyings = list(UNDL_POOL['total'])
            ticks = md.get_last_ticks(','.join(underlyings))
            for tick in ticks:
                tempdata = []
                for col in POOL_COLUMNS:
                    tempdata.append(eval(vars[col]))
                #UNDL_POOL_INFO[addfix(tick.sec_id)] = tempdata
                UNDL_POOL_INFO[rawholding_stocks.addfix(tick.sec_id)] = tempdata
        ret = md.init(
                username="18201141877",
                password="Wqxl7309",
                mode= 1)
        if ret != 0:
            raise Exception('Error in initiation with ErrorCode %d' % ret)
        data_thread = NewThread(target=pull_ticks)
        if not PRE_THREADS.get(source):  # 如果是第一次建立线程则创建，否则先关闭老线程，再开启新线程
            PRE_THREADS[source]=data_thread
        else:
            PRE_THREADS[source].stop()
            PRE_THREADS[source]=data_thread
        data_thread.start()
        print(threading.enumerate())
        print('data subscribed with source %s' % source)
    elif source=='simulation':
        def simugen(pathtype = 'Brownian',step=1):
            """ 模拟行情数据生成器 """
            colnum = len(POOL_COLUMNS)
            holdings = UNDL_POOL['total']
            for undl in holdings:
                if undl not in UNDL_POOL_INFO:
                    UNDL_POOL_INFO[undl] = np.random.rand(1,colnum)[0]
                else:
                    if pathtype == 'Geometric':
                        trend = np.random.randn(1,colnum)[0]
                        sigma = np.random.rand(1,colnum)[0]
                        UNDL_POOL_INFO[undl] *= np.exp(trend*step + sigma*np.sqrt(step)*np.random.randn(1,colnum))[0]
                    else:
                        UNDL_POOL_INFO[undl] += np.sqrt(step) * np.random.randn(1,colnum)[0]
        data_thread = NewThread(target=simugen)
        if not PRE_THREADS.get(source):  # 如果是第一次建立线程则创建，否则先关闭老线程，再开启新线程
            PRE_THREADS[source] = data_thread
        else:
            PRE_THREADS[source].stop()
            PRE_THREADS[source] = data_thread
        data_thread.start()
        print(threading.enumerate())
        print('data subscribed with source: %s' % source)
        print(UNDL_POOL['total'])
    else:
        print('No source infomation provided, can not subscribe!')
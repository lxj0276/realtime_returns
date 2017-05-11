import datetime as dt
import math
import os
import time
import threading

from gmsdk import md
import numpy as np
import sqlite3
from WindPy import w

from src.global_vars import *



def clear_dir(pathdir):
    """ 清空制定文件夹,删除其中 文件 和 文件夹 """
    if os.path.isfile(pathdir):
        raise Exception(u'当前路径为文件，应该提供一个文件夹')
    inpath = os.listdir(pathdir)
    for fl in inpath:
        fldir = os.path.join(pathdir,fl)
        if os.path.isfile(fldir):
            os.remove(fldir)
        if os.path.isdir(fldir):
            os.rmdir(fldir)


def calc_trd_levels(prestat,currstat):
    """ 根据cwstate文件计算持仓挡位情况 """
    statchg = currstat - prestat
    inlvs = np.sum(statchg < 0)
    outlvs = np.sum(statchg > 0)
    holdlvs = np.sum(statchg == 0)
    return {'inlvs' : inlvs, 'outlvs': outlvs, 'holdlvs':holdlvs }


def calc_shape(num):
    """ 根据子图总数量 计算画图子图的分布 """
    if num<= 0 :
        raise Exception ('the input number must be greater then zero')
    else:
        sq = math.sqrt(num)
        lower = math.floor(sq)
        upper = math.ceil(sq)
        level1 = lower*lower
        level2 = lower*upper
        if num>level2:
            shape = (upper,upper)
        elif level2>=num>level1:
            shape = (lower,upper)
        else:
            shape = (lower,lower)
    return shape


def wind2gm(undllst,endmark='.tick'):
    """ 转换为标的代码wind代码 为掘金代码 """
    gm_dict = {'SH':'SHSE','SZ':'SZSE','CFE':'CFFEX'}
    undl_new = []
    for undl in undllst:
        temp = undl.split('.')
        undl_new.append(''.join([gm_dict[temp[1]],'.',temp[0],endmark]))
    return undl_new

def addfix(undl):
    if undl[0] in ('0','3'):
        return  '.'.join([undl,'SZ'])
    elif undl[0] in ('6'):
        return  '.'.join([undl,'SH'])
    elif undl[0:2] in ('IF','IC','IH'):
        return  '.'.join([undl,'CFE'])

# def data_subscribe(source):
#     """  数据源订阅 , pool_columns 提供需要订阅的字段，需要更新undl_pool_info """
#     #  params = [underlyings, POOL_COLUMNS, Portfolio.undlpool_callback]
#
#     global UNDL_POOL
#     global UNDL_POOL_INFO
#     global POOL_COLUMNS
#     COLNUM = len(POOL_COLUMNS)
#
#     if source=='wind':
#         # 定义数据源对应 callback 函数
#         def wind_callback(indata):
#             if indata.ErrorCode!=0:
#                 raise Exception('Error in callback with ErrorCode %d' %indata.ErrorCode)  # 实际使用时，为防止中断可改为log输出
#             for dumi in range(len(indata.Codes)):
#                 fieldlen=len(indata.Fields)
#                 if fieldlen==COLNUM:   # 只有在所有field都有数据的时候才存储
#                     tempdata = []
#                     for dumj in range(fieldlen):
#                         tempdata.append(indata.Data[dumj][dumi])
#                     UNDL_POOL_INFO[indata.Codes[dumi]] = tempdata
#         w.start()
#         underlyings = list(UNDL_POOL['total'])
#         w.wsq(','.join(underlyings),','.join(POOL_COLUMNS),func = wind_callback)
#
#     elif source=='goldmine':
#         # 定义数据源对应 callback 函数
#         vars = {'rt_last':'tick.last_price','rt_time':'tick.str_time'}
#         def on_tick(tick):
#             tempdata = []
#             for col in POOL_COLUMNS:
#                 tempdata.append(eval(vars[col]))
#             UNDL_POOL_INFO[addfix(tick.sec_id)] = tempdata
#
#         underlyings = wind2gm(list(UNDL_POOL['total']))
#         ret = md.init(
#                 username="18201141877",
#                 password="Wqxl7309",
#                 mode= 3)
#         if ret != 0:
#             raise Exception('Error in initiation with ErrorCode %d' % ret)
#         ret = md.subscribe(','.join(underlyings))
#         if ret != 0:
#             raise Exception('Error in subscribe with ErrorCode %d' % ret)
#         # 添加回调函数
#         md.ev_tick += on_tick
#         # 出事填充POOL，确保不会出现 NAN
#         fillundl = ','.join(underlyings).replace('.tick','')
#         ticks = md.get_last_ticks(fillundl)
#         for tick in ticks:
#             tempdata = []
#             for col in POOL_COLUMNS:
#                 tempdata.append(eval(vars[col]))
#             UNDL_POOL_INFO[addfix(tick.sec_id)] = tempdata
#         # 加入线程
#         threading.Thread(target=md.run).start()
#
#     elif source=='goldmime_snapshot':   # 快照数据
#         vars = {'rt_last':'tick.last_price','rt_time':'tick.str_time'}
#         def pull_ticks():
#             while END_TIME>= dt.datetime.now() >= START_TIME:
#                 underlyings = wind2gm(list(UNDL_POOL['total']),endmark='')
#                 ticks = md.get_last_ticks(','.join(underlyings))
#                 for tick in ticks:
#                     tempdata = []
#                     for col in POOL_COLUMNS:
#                         tempdata.append(eval(vars[col]))
#                     UNDL_POOL_INFO[addfix(tick.sec_id)] = tempdata
#                 time.sleep(0.5)
#         ret = md.init(
#                 username="18201141877",
#                 password="Wqxl7309",
#                 mode= 1)
#         if ret != 0:
#             raise Exception('Error in initiation with ErrorCode %d' % ret)
#         threading.Thread(target=pull_ticks).start()
#         print(threading.enumerate())
#
#     elif source=='simulation':
#         def simugen(pathtype = 'Brownian'):
#             """ 模拟行情数据生成器 """
#             step = 1
#             colnum = len(POOL_COLUMNS)
#             holdings = UNDL_POOL['total']
#             while END_TIME>= dt.datetime.now() >= START_TIME:
#                 for undl in holdings:
#                     if undl not in UNDL_POOL_INFO:
#                         UNDL_POOL_INFO[undl] = np.random.rand(1,colnum)[0]
#                     else:
#                         if pathtype == 'Geometric':
#                             trend = np.random.randn(1,colnum)[0]
#                             sigma = np.random.rand(1,colnum)[0]
#                             UNDL_POOL_INFO[undl] *= np.exp(trend*step + sigma*np.sqrt(step)*np.random.randn(1,colnum))[0]
#                         else:
#                             UNDL_POOL_INFO[undl] += np.sqrt(step) * np.random.randn(1,colnum)[0]
#                 time.sleep(0.5)
#         threading.Thread(target=simugen).start()
#         print(threading.enumerate())
#     else:
#         print('No source infomation provided, can not subscribe!')


def data_subscribe(source):
    """  数据源订阅 , pool_columns 提供需要订阅的字段，需要更新undl_pool_info """
    #  params = [underlyings, POOL_COLUMNS, Portfolio.undlpool_callback]
    global UNDL_POOL
    global UNDL_POOL_INFO
    global POOL_COLUMNS
    global PRE_THREADS
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

    elif source=='goldmine':
        vars = {'rt_last':'tick.last_price','rt_time':'tick.str_time'}
        def on_tick(tick):
            tempdata = []
            for col in POOL_COLUMNS:
                tempdata.append(eval(vars[col]))
            UNDL_POOL_INFO[addfix(tick.sec_id)] = tempdata
        # 提取当前资产池中的代码，并转换为gm所需格式
        underlyings = wind2gm(list(UNDL_POOL['total']))
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
            UNDL_POOL_INFO[addfix(tick.sec_id)] = tempdata
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
    elif source=='goldmine_snapshot':
        vars = {'rt_last':'tick.last_price','rt_time':'tick.str_time'}
        def pull_ticks():
            underlyings = wind2gm(list(UNDL_POOL['total']),endmark='')
            ticks = md.get_last_ticks(','.join(underlyings))
            for tick in ticks:
                tempdata = []
                for col in POOL_COLUMNS:
                    tempdata.append(eval(vars[col]))
                UNDL_POOL_INFO[addfix(tick.sec_id)] = tempdata
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
            PRE_THREADS[source]=data_thread
        else:
            PRE_THREADS[source].stop()
            PRE_THREADS[source]=data_thread
        data_thread.start()
    else:
        print('No source infomation provided, can not subscribe!')



class NewThread(threading.Thread):
    """ 能够暂停和终止的线程类 """
    def __init__(self,group=None, target=None, name=None, args=(), kwargs={}, daemon=None ,frequency=0.5):
        super(NewThread,self).__init__(group=group, name=name, daemon=daemon)
        self._target = target
        self._name = name
        self._args = args
        self._kwargs = kwargs
        self.frequency = frequency
        # 设置 event
        self.__onoff = threading.Event()
        self.__onoff.set()
        self.__gohold = threading.Event()
        self.__gohold.set()

    def run(self):
        while self.__onoff.is_set():
            self._target(*self._args,**self._kwargs)
            time.sleep(self.frequency)

    def pause(self):
        self.__gohold.clear()

    def resume(self):
        self.__gohold.set()

    def stop(self):
        self.__gohold.set()
        self.__onoff.clear()
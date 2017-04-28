import os
import math
import time
import threading
import numpy as np
from WindPy import w
from src.global_vars import *


def holdlist_format():
    # 过滤停牌股
    # 返回项 : windcode, name, num, prc,val
    pass


def trdlist_format():
    # 做多 数量为正、金额为正， 做空为负、金额为负， 价格恒正, 交易成本恒为负
    # 返回项 : windcode, name, num, prc,val,transaction_cost, inout
    pass


def clear_dir(pathdir):
    # 清空制定文件夹,删除其中 文件 和 文件夹
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
    statchg = currstat - prestat
    inlvs = np.sum(statchg < 0)
    outlvs = np.sum(statchg > 0)
    holdlvs = np.sum(statchg == 0)
    return {'inlvs' : inlvs, 'outlvs': outlvs, 'holdlvs':holdlvs }


def calc_shape(num):
    # 计算画图子图的分布
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


def request_func(type,params):
    if type=='wind':
        w.start()
        w.wsq(params[0],params[1],func = params[2])
    if type=='goldmine':
        pass
    if type=='simulation':
        threading.Thread(target=simugen).start()


def simugen(type = 'Brownian'):
    global UNDL_POOL
    global UNDL_POOL_INFO
    global POOL_COLUMNS

    step = 1
    colnum = len(POOL_COLUMNS.split(','))
    holdings = UNDL_POOL['total']

    while True:
        for undl in holdings:
            if undl not in UNDL_POOL_INFO:
                UNDL_POOL_INFO[undl] = np.random.rand(colnum)
            else:
                if type == 'Geometric':
                    trend = np.random.randn(colnum)
                    sigma = np.random.rand(colnum)
                    UNDL_POOL_INFO[undl] *= np.exp(trend*step + sigma*np.sqrt(step)*np.random.randn(colnum))
                else:
                    UNDL_POOL_INFO[undl] += np.sqrt(step) * np.random.randn(colnum)
        time.sleep(0.5)



if __name__ == '__main__':
    dr = r"C:\Users\Jiapeng\Desktop\test.py"
    clear_dir(dr)
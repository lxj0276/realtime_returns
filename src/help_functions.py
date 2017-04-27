import math
import time
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
        pass


def simugen():
    global UNDL_POOL
    global UNDL_POOL_INFO
    global POOL_COLUMNS

    step = 1
    colnum = len(POOL_COLUMNS.split(','))
    holdings = UNDL_POOL['total']

    while True:
        for undl in holdings:
            if undl not in UNDL_POOL_INFO:
                UNDL_POOL_INFO[undl] = np.random.randn(colnum)*step
            else:
                UNDL_POOL_INFO[undl] += np.random.randn(colnum)*step
        time.sleep(0.5)
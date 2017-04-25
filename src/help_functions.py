import math
import numpy as np
import pandas as pd

def holdlist_format():
    # 做多 数量为正、金额为正， 做空为负、金额为负， 价格衡正
    # 过滤停牌股
    pass


def trdlist_format():
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


if __name__ == '__main__':
    t = np.array([[1,0,'c'],[0,0,'d'],[-1,0,'e'],[-1,0,'f']])
    p = pd.DataFrame(t,columns=['a','b','cd'],index=['c','d','e','f'])
    print(p)
    p.loc[p['cd'],'a']+= t[:,1]
    print(p)
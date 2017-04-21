import math

def holdlist_format():
    # 做多 数量为正， 做空为负， 价格衡正
    # 过滤停牌股
    pass


def trdlist_format():
    pass


def calc_shape(num):
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

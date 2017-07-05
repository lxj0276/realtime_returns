
import csv
import math
import os


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

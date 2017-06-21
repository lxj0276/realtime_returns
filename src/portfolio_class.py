import datetime as dt
import os
import time

import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import matplotlib.ticker as mtick
import matplotlib as mpl
import numpy as np
import pandas as pd
from WindPy import w

from src.data_subscribe import data_subscribe
from src.holding_generate import *
from src.global_vars import *
from src.help_functions import *



class Portfolio:

    CHARGE_TIME = 1    # 当日交易添加到 POOL 后，等待的时间  是否必要？
    PLOT_OBJ = {}      # 需要画图的对象，结构为{plotid:[object, True/False]}
    REGI_OBJ = []      # 所有已创建的实例
    PLOT_NUM = 0       # 需要画图的数量
    FIGDIR = r'.\saved_figures'   # 图像存储路径

    global UNDL_POOL           # 存储所有需要更新的标的代码，其结构见 add_pool 函数
    global UNDL_POOL_INFO      # 存储所有标的的最新推送信息，，结构为 {标的1:[v1,v2], 标的2:[v1,v2],...}
    global POOL_COLUMNS        # 订阅数据字段 列表
    global SUBSCRIBE_SOURCE    # 订阅数据源

    @classmethod
    def update_undlpool(cls):
        """ 更新实例状态并画图 """
        data_subscribe(SUBSCRIBE_SOURCE)
        today = dt.date.today()
        start = START_TIME
        end = END_TIME
        # 画图配置
        shape=calc_shape(len(Portfolio.REGI_OBJ))
        mpl.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
        plt.ion()
        fig=plt.figure(figsize=(20,20))
        fig.canvas.set_window_title('  '.join([str(today),'VERSION : '+VERSION]))
        x = {}
        y = {}
        axes = {}
        while( end>= dt.datetime.now() >= start):
            time.sleep(FLUSH_CWSTAT)
            for obj in Portfolio.REGI_OBJ:
                obj.update_object()
            count = 1   # 第几个子图
            for id in sorted( Portfolio.PLOT_OBJ ):    #  Portfolio.PLOT_OBJ 是以增加过的画图 obj 的 regid
                plobj = Portfolio.PLOT_OBJ[id]
                obj = plobj[0]
                ploting = plobj[1]
                if obj not in x:
                    x[obj] = [dt.datetime.now()]
                    y[obj] = [(obj.addvalue['floated']+obj.addvalue['fixed'])/obj.pofvalue]
                    ax=fig.add_subplot(str(shape[0])+str(shape[1])+str(count))
                    ax.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))#设置时间标签显示格式 '%Y-%m-%d %H:%M:%S'
                    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.4f%%'))
                    ax.set_title(obj.pofname)
                    axes[obj] = ax
                if ploting:
                    y[obj].append((obj.addvalue['floated']+obj.addvalue['fixed'])/obj.pofvalue * 100)
                else:
                    y[obj].append(y[obj][-1])
                x[obj].append(dt.datetime.now())
                axes[obj].set_xlim(x[obj][0], x[obj][-1])
                plt.pause(0.01)
                count +=1
            for id in sorted(Portfolio.PLOT_OBJ):
                plobj = Portfolio.PLOT_OBJ[id]
                obj = plobj[0]
                axes[obj].legend(('return : %.4f%%' % y[obj][-1],))
                axes[obj].plot(x[obj][1:], y[obj][1:], linewidth=1, color='r')
            print(UNDL_POOL['total'])
            print(UNDL_POOL_INFO)
        print('plot finished')
        plt.savefig(os.path.join(Portfolio.FIGDIR,str(today)+'.png'))

    @classmethod
    def add_pool(cls,lst,pofname):
        """ 把lst对应标的加入资产池 """
        # lst 结构为{交易品种：对应数据表} 如{stocks:pd.dtfm, futures: pd.dtfm}
        # UNDL_POOL 必须包含 'total' 关键字
        if pofname not in UNDL_POOL:  # 如果pool中没有该产品则新创建
            tempdict = {}
            for k in lst:
                toadd = set(lst[k]['code'])
                tempdict[k] = toadd
                UNDL_POOL['total'] |= toadd
            UNDL_POOL[pofname]=tempdict
        else:
            for k in lst:
                toadd = set(lst[k]['code'])
                if k not in UNDL_POOL[pofname]:
                    UNDL_POOL[pofname][k] = toadd
                else:
                    UNDL_POOL[pofname][k] |= toadd
                UNDL_POOL['total'] |= toadd

    @classmethod
    def pop_pool(cls,pofname,poplst):
        """ 从 pool 中删除对应组合，提取该组合特有的股票删除，不能删除 total 中与其他组合共有的股票 """
        # 根据特定 Portfolio pop
        if pofname in UNDL_POOL:
            holdonly = {}
            for k in poplst:
                holdonly[k]=set(poplst[k]['code'].values)
            for k in UNDL_POOL:
                if k not in ( 'total', pofname):
                    for undl in poplst:
                        holdonly[undl] -= UNDL_POOL[k][undl]
            for und in holdonly:      # 删除 total 中对应部分
                UNDL_POOL['total'] -= holdonly[und]
            del UNDL_POOL[pofname]  # 删除对应产品, 可能仍有部分碎股会存在


    def __init__(self,pofname,pofval_dir,holdlst_dir,trdlst_dir,handlst_dir,cwstatus_dir):
        self.pofname = pofname                               # 产品名称
        #-----------------------  基本路径 --------------------------------------
        self.pofval_dir = pofval_dir                           # 产品资产存储文件路径
        self.holdlst_dir = holdlst_dir                         # 组合持仓文件路径，数据结构为 基于标的的字典 ex. {'stocks':_dir1,futures:_dir2}
        self.trdlst_dir = trdlst_dir                           # 当日交易单子文件路径，类似于holdlst_dir 是基于标的的字典
        self.handlst_dir = handlst_dir                         # 手动交易单子路径
        self.cwstatus_dir = cwstatus_dir                       # 交易状态文件路径
        #-----------------------  变量初始化 --------------------------------------
        self.pofvalue = self.get_pofvalue()                  # 初始化总资产
        self.holdlist = {'T1':{},'T0':{}}                   # 初始化组合持仓，T0对应今日买入的资产,T1对应今日之前买入的资产 , T+0/T+1 部分分别存储一个基于品种的、类似lst的dict
        self.addvalue = {'fixed':0 ,'floated':0 }          # 组合收益数值，若在当天出场则为fixed收益，否则为floated
        self.trdlist = {}                                    # 初始化交易单子
        self.handlist = {}                                   # 初始化手动交易单子
        for undl in trdlst_dir:                              # 清空所有交易单存储所在的文件夹，避免前一日的交易单对今天造成影响
            clear_dir(trdlst_dir[undl])
        for undl in self.handlst_dir:                        # 清空手动交易单子路径
            clear_dir(handlst_dir[undl])
        #-----------------------  交易状态设置 --------------------------------------
        self.plotid = 0                                      # 初始化该实例的plotid，如果需要画图的话该值应该为大于0的值，不需要的话则为0
        self.noposition = {}
        self.lasttrdstat = self.get_trdstat(predaystat=True)       # 初始化交易状态,使用前一日的cwstate交易状态，改文件路径可由cwstate路径推出
        for strategy in self.lasttrdstat:  # 检查是否有持仓
            self.noposition[strategy] = not np.any(self.lasttrdstat[strategy][:,0])
        if not np.all(list(self.noposition.values())):                  # 交易开始前就有持仓的情况下，加入holdlist, 当天开始时无持仓则不必
            self.holdlist['T1'] = self.read_holdlist()
            if self.holdlist['T1']:
                Portfolio.add_pool(self.holdlist['T1'],self.pofname)    # 将持仓增加到 POOL中
                Portfolio.PLOT_NUM += 1                                  # 有持仓则确定该对象需要画图，增加类需要的画图数目
                self.plotid = Portfolio.PLOT_NUM                         # 设定 plotid 为第几个需要画的图
                Portfolio.PLOT_OBJ[self.plotid] = [self,True]            # 第二个布尔值为画图开关，当停止画图时会被设置为False
        Portfolio.REGI_OBJ.append(self)                      # 将该实例对象添加到类的实例记录列表
        print(' %s portfolio created ! ' % self.pofname)

    def get_pofvalue(self):
        with open(self.pofval_dir, 'r') as pofinfo:
            val = float(pofinfo.readlines()[0].strip())
        return val

    def get_trdstat(self,predaystat=False):
        """ 返回从cwstate.txt读取的内容，类型为np array
            如果需要前一日的交易状态，则需从当前cwstate路径推出前一日交易状态的路径
            可以这样推是因为目前cwstate_history 文件夹 和 cwstate.txt 在同一目录下 ！
        """
        contents = {}
        length = 6
        for strategy in self.cwstatus_dir:
            cw = self.cwstatus_dir[strategy]
            if predaystat:   # 提取前一日交易状态路径
                cwdir = os.path.join(cw,''.join(['cwstate_history\cwstate_',YESTERDAY,'.txt']))
            else:
                cwdir = os.path.join(cw,'cwstate.txt')
            with open(cwdir,'r') as cwinfo:
                temp = cwinfo.readlines()
                contents_temp = [c.strip().split(',') for c in temp]
                contents[strategy] = np.array([([float(c) for c in t]) for t in contents_temp if len(t)==length])   # 确保足够长，过滤掉意外空行的情况
        return contents

    def flush_trdstat(self):
        """ 根据持仓状态变更，更新 holdlist以及资产池，并更新 lasttrdstat
            如果交易状态改变，会不断扫描交易单文件夹，直到成功读入 对应交易记录
        """
        currtrdstat = self.get_trdstat()
        updtstat = False
        # 提取基于策略的交易情况
        trdstat = {}
        for strategy in currtrdstat:
            totlevels = currtrdstat[strategy].shape[0]
            chglevels = calc_trd_levels(self.lasttrdstat[strategy][:,0],currtrdstat[strategy][:,0])
            if chglevels['holdlvs'] < totlevels:   # 持仓有变动
                if chglevels['poslvs']==0 and chglevels['outlvs']>0: # 从有持仓变为空仓
                    trdstat[strategy] = 'hold_to_empty'
                elif self.noposition[strategy] and chglevels['invls']>0:  # 从无持仓变为有持仓
                    trdstat[strategy] = 'empty_to_hold'
                else: # 有持仓，且持仓变动
                    if chglevels['inlvs'] == 0:   # 只有卖出
                        trdstat[strategy] = 'out_only'
                    elif chglevels['outlvs'] == 0: # 只有买入
                        trdstat[strategy] = 'in_only'
                    else:  # 买卖都有
                        trdstat[strategy] = 'in_and_out'

        for strategy in currtrdstat:
            totlevels = currtrdstat[strategy].shape[0]
            chglevels = calc_trd_levels(self.lasttrdstat[strategy][:,0],currtrdstat[strategy][:,0])
            if chglevels['holdlvs'] < totlevels:   # 持仓有变动
                print('%s %s: trade happend with InLevels:%d -- OutLevels:%d -- HoldLevels:%d' %(self.pofname,strategy,chglevels['inlvs'],chglevels['outlvs'],chglevels['holdlvs']))
                trdlst = self.read_trdlist(strategy=strategy)
                if chglevels['poslvs']==0 and chglevels['outlvs']>0: # 从有持仓变为空仓
                    if trdlst['out']:
                        self.update_holdlist(trdlst['out'],'T1')  # 当天只能卖出T+1的股票
                        self.holdlist = {'T0':{},'T1':{}}
                        Portfolio.pop_pool(self.pofname,trdlst['out'])
                        self.noposition[strategy] = True
                        updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                        print('%s %s: trading list found : Out' % (self.pofname,strategy))
                    else:
                        print('%s %s: waiting for trading list ...' % (self.pofname,strategy))
                elif self.noposition[strategy] and chglevels['invls']>0:  # 从无持仓变为有持仓
                    if trdlst['in']:
                        #self.holdlist['T0'] = trdlst['in']     # 当天买入的股票算作 T+0
                        self.update_holdlist(trdlst['in'],'T0')
                        Portfolio.add_pool(self.holdlist['T0'],self.pofname)
                        self.noposition[strategy] = False
                        updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                        print('%s %s: trading list found : In' % (self.pofname,strategy))
                    else:
                        print('%s %s: waiting for trading list ...' % (self.pofname,strategy))
                else:  # 有持仓，且持仓变动
                    if chglevels['inlvs'] == 0:   # 只有卖出
                        if trdlst['out']:
                            self.update_holdlist(trdlst['out'],'T1')
                            updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                            print('%s %s: trading list found : Out' % (self.pofname,strategy))
                        else:
                            print('%s %s: waiting for trading list ...' % (self.pofname,strategy))
                    elif chglevels['outlvs'] == 0: # 只有买入
                        if trdlst['in']:
                            self.update_holdlist(trdlst['in'],'T0')
                            Portfolio.add_pool(trdlst['in'],self.pofname)
                            updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                            print('%s %s: trading list found : In' % (self.pofname,strategy))
                        else:
                            print('%s %s: waiting for trading list ...' % (self.pofname,strategy))
                    else:  # 买卖都有
                        if trdlst['in'] and trdlst['out']:
                            self.update_holdlist(trdlst['in'],'T0')
                            self.update_holdlist(trdlst['out'],'T1')
                            Portfolio.add_pool(trdlst['in'],self.pofname)
                            updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                            print('%s %s: trading list found : In & Out' % (self.pofname,strategy))
                        else:
                            print('%s %s: waiting for trading list ...' % (self.pofname,strategy))
            if updtstat:  # 持仓更新成功，单子已经到达录入成功
                data_subscribe(SUBSCRIBE_SOURCE)
                self.lasttrdstat[strategy] = currtrdstat[strategy]
                time.sleep(Portfolio.CHARGE_TIME)

    def read_trdlist(self,strategy, handtrd = False):
        """ 读取标准格式的交易单子 ， 返回 买入 卖出两个方向的单子 """
        if handtrd:
            lst_dir = self.handlst_dir
            scanedlst = self.handlist
        else:
            lst_dir = self.trdlst_dir
            scanedlst = self.trdlist
        trdlist = {}
        trdlist['in'] = {}
        trdlist['out'] = {}
        for k in lst_dir:
            if k not in scanedlst:
                scanedlst[k] = []
            files=os.listdir(lst_dir[k])
            newfiles = set(files)-set( scanedlst[k])
            if newfiles:
                templist = pd.DataFrame()
                for f in newfiles:
                    templist = templist.append( pd.read_csv(os.path.join(lst_dir[k],f),encoding='gb2312') , ignore_index=True)
                    scanedlst[k].append(f)    # 把已经读取过的 file 加入到记录
                tempin = templist[templist['inout'] == 'in']
                tempout = templist[templist['inout'] == 'out']
                groupedin=tempin.groupby('code').sum()
                groupedin['code'] = groupedin.index
                groupedin['prc'] = groupedin['val'].values/groupedin['num'].values
                groupedout=tempout.groupby('code').sum()
                groupedout['code'] = groupedout.index
                groupedout['prc'] = groupedout['val'].values/groupedout['num'].values
                trdlist['in'][k] = groupedin
                trdlist['out'][k] = groupedout
        return trdlist

    def check_handtrd(self):
        """ 扫描手动交易单子 """
        # 暂定为不断扫描handlst_dir
        hastrd = False
        handlst = self.read_trdlist(handtrd=True)
        if handlst['in']:
            self.update_holdlist(handlst['in'], 'T0')
            Portfolio.add_pool(self.holdlist['T0'], self.pofname)
            self.noposition = False
            hastrd = True
            print('%s : trading by hand found : In' % self.pofname)
        if handlst['out']:
            self.update_holdlist(handlst['out'], 'T1')
            hastrd = True
            print('%s ：trading by hand found : Out' % self.pofname)
        if hastrd:  # 持仓更新成功，单子已经到达录入成功
            data_subscribe(SUBSCRIBE_SOURCE)
            time.sleep(Portfolio.CHARGE_TIME)

    def read_holdlist(self,source = 'file'):
        """ 读取标准格式的持仓单，
            返回字典结构,字典的key是各个不同品种的标的，如stocks\futures\options etc
            注意 ： 此处不涉及 T0 T1
        """
        holdlist={}
        for k in self.holdlst_dir:
            if os.path.exists(self.holdlst_dir[k]):
                holdlist[k] = pd.read_csv(self.holdlst_dir[k],encoding='gb2312') #,names=['code','name','num','prc','val'],header=1)
                holdlist[k].index = holdlist[k]['code'].tolist()
            else:
                print('%s : No holdlist dir for %s' %(self.pofname, k))
        return holdlist

    def update_holdlist(self,lst,type):
        """ 只有在有交易的时候才调用此函数 更新holdlist , 对象初始化时不必调用 """
        # 做多数量为正，做空数量为负 lst应为带有标的作为key的dict
        if type not in ('T0','T1'):
            raise Exception('Need to specify the type of holdlist to be updated!')
        if not lst:   # lst 为空
            return
        ts = 0  # 交易成本
        if type=='T1':     # 只在出场的时候更新T+1
            for k in lst:
                codes = lst[k]['code']
                num = lst[k]['num']
                self.holdlist['T1'][k].loc[codes,'num'] += num
                addsgl = (self.holdlist['T1'][k].loc[codes,'prc'].values-lst[k]['prc'].values)*num.values
                self.addvalue['fixed'] += np.sum( addsgl )
                self.holdlist['T1'][k].loc[codes,'val'] = self.holdlist['T1'][k].loc[codes,'prc'] * self.holdlist['T1'][k].loc[codes,'num']
                ts += lst[k]['tscost'].sum()
        else:      # 只在当日入场时更新 T+0
            if len(self.holdlist['T0'])==0:
                self.holdlist['T0'] = lst
            else:
                for k in lst:
                    if k in self.holdlist['T0']:
                        temp=pd.concat([self.holdlist['T0'][k],lst[k]],ignore_index=True)   # type:pd.DataFrame
                        grouped = temp.groupby('code').sum()
                        grouped['code'] = grouped.index
                        grouped['prc'] = grouped['val'].values / grouped['num'].values
                        self.holdlist['T0'][k] = grouped
                    else:
                        self.holdlist['T0'][k] = lst[k]
                    ts += lst[k]['tscost'].sum()
        self.addvalue['fixed'] += ts
    
    def update_addvalue(self):
        # 只计算尚未卖出的收益，即floated 收益
        # 当日卖出的部分属于 fixed 收益，在 update_holdlist 中计算
        if len(self.holdlist['T0'])==0 and len(self.holdlist['T1'])==0:  # 调用时没有持仓
            return
        newinfo = pd.DataFrame( UNDL_POOL_INFO, index = POOL_COLUMNS).T
        addval = 0
        for tp in self.holdlist:
            holding=self.holdlist[tp]
            if len(holding) == 0:
                continue
            for k in holding:
                holdingK=holding[k]
                holding_code = holdingK['code'].values
                #holding_code = holdingK['code'].tolist()
                lastprc = newinfo['rt_last'][holding_code].values
                addval += np.sum( ( lastprc - holdingK['prc'].values) * holdingK['num'].values )
        self.addvalue['floated'] = addval

    def startplot(self):   # 如需画图，则将该产品 对象 添加到 Portfolio 类画图列表中
        added_plots = Portfolio.PLOT_OBJ
        if self.plotid not in added_plots:
            Portfolio.PLOT_NUM += 1
            self.plotid = Portfolio.PLOT_NUM
            Portfolio.PLOT_OBJ[self.plotid] = [self,True]
        else:
            Portfolio.PLOT_OBJ[self.plotid][1] = True

    def stopplot(self):
        Portfolio.PLOT_OBJ[self.plotid][1] = False

    def update_object(self):
        """ 定时扫描交易状态 trdlist_dir handtrd_dir 并根据交易更新 holdlist，同时更新收益 """
        self.pofvalue = self.get_pofvalue()    # 更新总资产，应对当日转账情况
        #self.check_handtrd()    # 检查手动交易
        self.flush_trdstat()
        if not np.all(list(self.noposition.values())):  # 有持仓
            self.startplot()
        elif np.all(list(self.noposition.values())):
            self.stopplot()
        if UNDL_POOL_INFO:
            self.update_addvalue()

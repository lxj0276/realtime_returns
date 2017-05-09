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

from src.help_functions import *
from src.global_vars import *



class Portfolio:

    FLUSH_CWSTAT = 1   # 画图更新时间将而 秒
    CHARGE_TIME = 1    # 当日交易添加到 POOL 后，等待的时间  是否必要？
    PLOT_OBJ = {}      # 需要画图的对象，结构为{plotid:[object, True/False]}
    REGI_OBJ = []      # 所有已创建的实例
    PLOT_NUM = 0       # 需要画图的数量
    # REGED_NUM = 0      # 创建过的实力的数量
    FIGDIR = r'.\saved_figures'   # 图像存储路径

    global UNDL_POOL           # 存储所有需要更新的标的代码，其结构见 add_pool 函数
    global UNDL_POOL_INFO      # 存储所有标的的最新推送信息，，结构为 {标的1:[v1,v2], 标的2:[v1,v2],...}
    global POOL_COLUMNS        # 订阅数据字段 列表
    global SUBSCRIBE_SOURCE    # 订阅数据源

    @classmethod
    def update_undlpool(cls):
        """ 更新实例状态并画图 """
        data_subscribe(SUBSCRIBE_SOURCE)
        today=dt.date.today()
        start=dt.datetime(year=today.year, month=today.month,day=today.day,hour= 8,minute=30,second=0)
        end=dt.datetime(year=today.year, month=today.month,day=today.day,hour= 19,minute=45,second=0)
        # 画图配置
        shape=calc_shape(len(Portfolio.REGI_OBJ))
        mpl.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
        plt.ion()
        fig=plt.figure(figsize=(20,20))
        fig.canvas.set_window_title(str(today))
        x = {}
        y = {}
        axes = {}
        while( end>= dt.datetime.now() >= start):
            time.sleep(Portfolio.FLUSH_CWSTAT)
            for obj in Portfolio.REGI_OBJ:
                obj.update_object()
            count = 1
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


    def __init__(self,pofname,pofvaldir,hldlstdir,trdlstdir,handlstdir,cwstatusdir,databasedir):
        self.pofname = pofname                               # 产品名称
        self.pofvaldir = pofvaldir                           # 产品资产存储文件路径
        self.pofvalue = self.get_pofvalue()                  # 初始化总资产
        self.cwstatusdir = cwstatusdir                       # 交易状态文件路径
        self.lasttrdstat = self.get_trdstat()                # 初始化交易状态
        self.noposition = not np.any(self.lasttrdstat[:,0])  # 检查是否有持仓
        self.addvalue = {'fixed':0 ,'floated':0 }          # 组合收益数值，若在当天出场则为fixed收益，否则为floated
        self.hldlstdir = hldlstdir                           # 组合持仓文件路径，数据结构为 基于标的的字典 ex. {'stocks':dir1,futures:dir2}
        self.holdlist = {'T1':{},'T0':{}}                   # 初始化组合持仓，T0对应今日买入的资产,T1对应今日之前买入的资产 , T+0/T+1 部分分别存储一个基于品种的、类似lst的dict
        self.trdlstdir = trdlstdir                           # 当日交易单子文件路径，类似于hldlstdir 是基于标的的字典
        for undl in trdlstdir:                              # 清空所有交易单存储所在的文件夹，避免前一日的交易单对今天造成影响
            clear_dir(trdlstdir[undl])
        self.trdlist = {}                                    # 初始化交易单子
        self.handlstdir = handlstdir                         # 手动交易单子路径
        for undl in self.handlstdir:                        # 清空手动交易单子路径
            clear_dir(handlstdir[undl])
        self.handlist = {}                                   # 初始化手动交易单子
        self.databasedir = databasedir                       # 存储每日持仓记录、交易记录的数据库 格式类似于hldlstdir 为基于标的的字典 ex. {'stocks':dir1,futures:dir2}
        # Portfolio.REGED_NUM += 1                             # 对象实例 数量增加
        # self.regid = Portfolio.REGED_NUM                     ################### 可能不需要
        Portfolio.REGI_OBJ.append(self)                      # 将该实例对象添加到类的实例记录列表
        self.plotid = 0                                      # 初始化该实例的plotid，如果需要画图的话该值应该为大于0的值，不需要的话则为0
        if not self.noposition:                             # 交易开始前就有持仓的情况下，加入holdlist, 当天开始时无持仓则不必
            self.holdlist['T1'] = self.read_holdlist()
            if self.holdlist['T1']:
                Portfolio.add_pool(self.holdlist['T1'],self.pofname)    # 将持仓增加到 POOL中
                Portfolio.PLOT_NUM += 1                                  # 有持仓则确定该对象需要画图，增加类需要的画图数目
                self.plotid = Portfolio.PLOT_NUM                         # 设定 plotid 为第几个需要画的图
                Portfolio.PLOT_OBJ[self.plotid] = [self,True]            # 第二个布尔值为画图开关，当停止画图时会被设置为False


    def get_pofvalue(self):
        with open(self.pofvaldir, 'r') as pofinfo:
            val = float(pofinfo.readlines()[0])
        return val

    def get_trdstat(self):
        # 返回从cwstate.txt读取的内容，类型为np array
        with open(self.cwstatusdir,'r') as cwinfo:
            temp = cwinfo.readlines()
            contents_temp = [c.strip().split(',') for c in temp]
            contents = [[int(c) for c in t] for t in contents_temp]
        return np.array(contents)

    def flush_trdstat(self):
        # 根据持仓状态变更，更新 holdlist以及资产池，并更新 lasttrdstat
        currtrdstat = self.get_trdstat()
        updtstat = False
        statchg = 0
        totlevels = currtrdstat.shape[0]
        chglevels = calc_trd_levels(self.lasttrdstat[:,0],currtrdstat[:,0])
        if chglevels['holdlvs'] < totlevels:   # 持仓有变动
            trdlst = self.read_trdlist()
            if not np.any(currtrdstat[:,0]): # 从有持仓变为空仓
                if trdlst['out']:
                    self.update_holdlist(trdlst['out'],'T1')  # 当天只能卖出T+1的股票
                    self.holdlist = {'T0':{},'T1':{}}
                    Portfolio.pop_pool(self.pofname,trdlst['out'])
                    self.noposition = True
                    statchg = -1
                    updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
            #elif not np.any(self.lasttrdstat[:,0]): # 从无持仓变为有持仓
            elif self.noposition:
                if trdlst['in']:
                    #self.holdlist['T0'] = trdlst['in']     # 当天买入的股票算作 T+0
                    self.update_holdlist(trdlst['in'],'T0')
                    Portfolio.add_pool(self.holdlist['T0'],self.pofname)
                    self.noposition = False
                    statchg = 1
                    updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
            else:  # 有持仓，且持仓变动
                if chglevels['inlvs'] == 0:   # 只有卖出
                    if trdlst['out']:
                        self.update_holdlist(trdlst['out'],'T1')
                        updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                elif chglevels['outlvs'] == 0: # 只有买入
                    if trdlst['in']:
                        self.update_holdlist(trdlst['in'],'T0')
                        Portfolio.add_pool(trdlst['in'],self.pofname)
                        updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                else:  # 买卖都有
                    if trdlst['in'] and trdlst['out']:
                        self.update_holdlist(trdlst['in'],'T0')
                        self.update_holdlist(trdlst['out'],'T1')
                        Portfolio.add_pool(trdlst['in'],self.pofname)
                        updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
        if updtstat:  # 持仓更新成功，单子已经到达录入成功
            data_subscribe(SUBSCRIBE_SOURCE)
            self.lasttrdstat=currtrdstat
            time.sleep(Portfolio.CHARGE_TIME)
        return statchg

    def check_handtrd(self):
        # 暂定为不断扫描handlstdir
        hastrd = False
        handlst = self.read_trdlist(handtrd=True)
        if handlst['in']:
            self.update_holdlist(handlst['in'], 'T0')
            Portfolio.add_pool(self.holdlist['T0'], self.pofname)
            self.noposition = False
            hastrd = True
        if handlst['out']:
            self.update_holdlist(handlst['out'], 'T1')
            hastrd = True
        if hastrd:  # 持仓更新成功，单子已经到达录入成功
            data_subscribe(SUBSCRIBE_SOURCE)
            time.sleep(Portfolio.CHARGE_TIME)
            print('has handtrd in %s' % self.pofname)

    def read_trdlist(self, handtrd = False):  # 返回 买入 卖出两个方向的单子
        if handtrd:
            lstdir = self.handlstdir
            scanedlst = self.handlist
        else:
            lstdir = self.trdlstdir
            scanedlst = self.trdlist
        trdlist = {}
        trdlist['in'] = {}
        trdlist['out'] = {}
        for k in lstdir:
            if k not in scanedlst:
                scanedlst[k] = []
            files=os.listdir(lstdir[k])
            newfiles = set(files)-set( scanedlst[k])
            if newfiles:
                templist = pd.DataFrame()
                for f in newfiles:
                    templist = templist.append( pd.read_csv(os.path.join(lstdir[k],f),encoding='gb2312',names=['code','name','num','prc','val','tscost','side']) , ignore_index=True)
                    scanedlst[k].append(f)    # 把已经读取过的 file 加入到记录
                tempin = templist[templist['side'] == 'in']
                tempout = templist[templist['side'] == 'out']
                groupedin=tempin.groupby('code').sum()
                groupedin['code'] = groupedin.index
                groupedin['prc'] = groupedin['val'].values/groupedin['num'].values
                groupedout=tempout.groupby('code').sum()
                groupedout['code'] = groupedout.index
                groupedout['prc'] = groupedout['val'].values/groupedout['num'].values
                trdlist['in'][k] = groupedin
                trdlist['out'][k] = groupedout
        return trdlist

    def read_holdlist(self):
        # 返回字典结构,字典的key是各个不同品种的标的，如stocks\futures\options etc  注意 ： 此处不涉及 T0 T1
        holdlist={}
        for k in self.hldlstdir:
            holdlist[k] = pd.read_csv(self.hldlstdir[k],encoding='gb2312',names=['code','name','num','prc','val'])
            holdlist[k].index = holdlist[k]['code'].tolist()
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
            Portfolio.PLOT_NUM+=1
            self.plotid = Portfolio.PLOT_NUM
            Portfolio.PLOT_OBJ[self.plotid] = [self,True]
        else:
            Portfolio.PLOT_OBJ[self.plotid][1] = True

    def stopplot(self):
        Portfolio.PLOT_OBJ[self.plotid][1] = False

    def update_object(self):  # 定时扫描交易状态 trdlistdir 并更新 holdlist
        self.pofvalue = self.get_pofvalue()    # 更新总资产，应对当日转账情况
        self.check_handtrd()    # 检查手动交易
        statchg = self.flush_trdstat()
        if statchg == 1:
            self.startplot()
        elif statchg == -1:
            self.stopplot()
        if UNDL_POOL_INFO:
            self.update_addvalue()








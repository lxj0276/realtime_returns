import os
import time
import threading
import datetime as dt
import numpy as np
import pandas as pd
from WindPy import w
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import matplotlib.ticker as mtick
import matplotlib as mpl
from src.help_functions import *
from src.global_vars import *


class portfolio:

    FLUSH_CWSTAT = 0.5
    FLUSH_TICK = 1
    PLOT_SHAPE ={}
    PLOT_OBJ = {}   # 需要画图的对象
    REGI_OBJ = [] # 所有已创建的对象
    PLOT_NUM = 0
    REGED_NUM = 0
    TODAY = dt.date.today()
    START = dt.datetime(year=TODAY.year, month=TODAY.month, day=TODAY.day, hour=9, minute=30, second=0)
    END = dt.datetime(year=TODAY.year, month=TODAY.month, day=TODAY.day, hour=19, minute=55, second=0)
    FIG = plt.figure(figsize=(20,20))
    FIGDIR = r'.'

    global UNDL_POOL
    global UNDL_POOL_INFO
    global POOL_COLUMNS
    global CALLBACK_TYPE
    COLNUM = len(POOL_COLUMNS.split(','))


    @staticmethod
    def undlpool_callback(indata):
        print(indata)
        if indata.ErrorCode!=0:
            raise Exception('Error in callback with ErrorCode %d' %indata.ErrorCode)  # 实际使用时，为防止中断可改为log输出
        for dumi in range(len(indata.Codes)):
            fieldlen=len(indata.Fields)
            if fieldlen==portfolio.COLNUM:   # 只有在所有field都有数据的时候才存储
                tempdata = []
                for dumj in range(fieldlen):
                    tempdata.append(indata.Data[dumj][dumi])
                UNDL_POOL_INFO[indata.Codes[dumi]] = tempdata


    @classmethod
    def update_undlpool(cls):

        tot=list(UNDL_POOL['total'])
        underlyings=''.join(tot)
        underlyings=underlyings.replace("SH","SH,")
        underlyings=underlyings.replace("SZ","SZ,")
        underlyings=underlyings.replace("CFE","CFE,")

        request_func(CALLBACK_TYPE,[underlyings,POOL_COLUMNS,portfolio.undlpool_callback])

        # 画图配置
        portfolio.PLOT_SHAPE = calc_shape(len(portfolio.REGI_OBJ))
        mpl.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
        plt.ion()
        portfolio.FIG.canvas.set_window_title(str(portfolio.TODAY))

        for obj in portfolio.REGI_OBJ:
            obj.ax = portfolio.FIG.add_subplot(str(portfolio.PLOT_SHAPE[0]) + str(portfolio.PLOT_SHAPE[1]) + str(obj.plotid))
            obj.ax.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))  # 设置时间标签显示格式 '%Y-%m-%d %H:%M:%S'
            obj.ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.4f%%'))
            obj.ax.set_title(obj.pofname)

        threading.Thread(target = portfolio.run).start()
        preplot = set()
        while (portfolio.START <= dt.datetime.now() <= portfolio.END):
            curplot = set(portfolio.PLOT_OBJ.keys())
            difplot = curplot - preplot
            if len(difplot)>0:
                for obj in portfolio.REGI_OBJ:
                    if obj.plotid in difplot:
                        threading.Thread(target=obj.ploting).start()
                preplot = curplot
            time.sleep(portfolio.FLUSH_CWSTAT)

        print('plot finished')
        plt.savefig(os.path.join(portfolio.FIGDIR,str(portfolio.TODAY)+'.png'))


    @classmethod
    def run(cls):
        while( portfolio.START<= dt.datetime.now() <= portfolio.END):
            time.sleep(portfolio.FLUSH_CWSTAT)
            for obj in portfolio.REGI_OBJ:
                obj.update_object()


    @classmethod
    def add_pool(cls,lst,pofname):
        # 把list加入资产池
        # lst 结构为{交易品种：对应数据表} 如{stocks:pd.dtfm, futures: pd.dtfm}
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
        # 从 pool 中删除
        # 根据特定 portfolio pop
        if pofname in UNDL_POOL:
            #holdonly = UNDL_POOL[pofname]  # 提取该组合特有的股票删除，不能删除 total 中与其他组合共有的股票
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


    def __init__(self,pofname,pofvalue,hldlstdir,trdlstdir,cwstatusdir):
        self.pofname = pofname
        self.pofvalue = pofvalue                 # 产品前一日总资产
        self.hldlstdir = hldlstdir               # 基于标的的字典 ex. {'stocks':dir1,futures:dir2}
        self.trdlstdir = trdlstdir               # 基于标的的字典
        self.cwstatusdir = cwstatusdir
        self.lasttrdstat = self.get_trdstat()   # 创建对象的时间应该在 开始交易时间之前 （开盘前 或 9：35 之前）
        self.noposition = not np.any(self.lasttrdstat[:,0])
        self.addvalue = {'fixed':0 ,'floated':0 }  # 若在当天出场则为fixed收益，否则为floated
        self.holdlist = {'T1':{},'T0':{}}     # holdlist数据结构 ： { 'T1':{}, 'T0':{}} , T+0/T+1 部分分别存储一个基于品种的、类似lst的dict
        self.trdlist = {}

        portfolio.REGI_OBJ.append(self)
        portfolio.REGED_NUM += 1
        self.regid = portfolio.REGED_NUM
        self.plotid = 0

        if not self.noposition: # 交易开始前就有持仓的情况下，加入holdlist, 当天开始时无持仓则不必
            self.holdlist['T1'] = self.read_holdlist()
            if self.holdlist['T1']:
                portfolio.add_pool(self.holdlist['T1'],self.pofname)
                portfolio.PLOT_NUM += 1
                self.plotid = portfolio.PLOT_NUM
                portfolio.PLOT_OBJ[self.plotid] = [True,False]    # plotid注册画图， 第一个参数启动画图，第二个参数是否 建过图


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
                    portfolio.pop_pool(self.pofname,trdlst['out'])
                    self.noposition = True
                    statchg = -1
                    updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
            elif not np.any(self.lasttrdstat[:,0]): # 从无持仓变为有持仓
                if trdlst['in']:
                    #self.holdlist['T0'] = trdlst['in']     # 当天买入的股票算作 T+0
                    self.update_holdlist(trdlst['in'],'T0')
                    portfolio.add_pool(self.holdlist['T0'],self.pofname)
                    time.sleep(1)
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
                        portfolio.add_pool(trdlst['in'],self.pofname)
                        time.sleep(1)
                        updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                else:  # 买卖都有
                    if trdlst['in'] and trdlst['out']:
                        self.update_holdlist(trdlst['in'],'T0')
                        self.update_holdlist(trdlst['out'],'T1')
                        portfolio.add_pool(trdlst['in'],self.pofname)
                        time.sleep(1)
                        updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
        if updtstat:  # 持仓更新成功，单子已经到达录入成功
            tot=list(UNDL_POOL['total'])
            underlyings=''.join(tot)
            underlyings=underlyings.replace("SH","SH,")
            underlyings=underlyings.replace("SZ","SZ,")
            underlyings=underlyings.replace("CFE","CFE,")
            # w.start()
            # w.wsq(underlyings,POOL_COLUMNS,func=portfolio.undlpool_callback)  # 有持仓更新的话需要重新订阅
            request_func(CALLBACK_TYPE, [underlyings, POOL_COLUMNS, portfolio.undlpool_callback])
            self.lasttrdstat=currtrdstat
        return statchg


    def read_trdlist(self):  # 返回 买入 卖出两个方向的单子
        trdlist = {}
        trdlist['in'] = {}
        trdlist['out'] = {}
        for k in self.trdlstdir:
            if k not in self.trdlist:
                self.trdlist[k] = []
            files=os.listdir(self.trdlstdir[k])
            newfiles = set(files)-set( self.trdlist[k])
            if newfiles:
                templist = pd.DataFrame()
                for f in newfiles:
                    templist = templist.append( pd.read_csv(os.path.join(self.trdlstdir[k],f),encoding='gb2312',names=['code','name','num','prc','val','tscost','side']) , ignore_index=True)
                    self.trdlist[k].append(f)    # 把已经读取过的 file 加入到记录
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
        # 只有在有交易的时候才调用此函数 更新holdlist , 对象初始化时不必调用
        # 做多数量为正，做空数量为负 lst应为带有标的作为key的dict
        if type not in ('T0','T1'):
            raise Exception('Need to specify the type of holdlist to be updated!')
        if len(lst) == 0:
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
        newinfo = pd.DataFrame( UNDL_POOL_INFO, index = POOL_COLUMNS.split(',')).T
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


    def startplot(self):   # 如需画图，则将该产品 对象 添加到 portfolio 类画图列表中
        if self.plotid not in portfolio.PLOT_OBJ:
            portfolio.PLOT_NUM += 1
            self.plotid = portfolio.PLOT_NUM  # 分配 plotid
            portfolio.PLOT_OBJ[self.plotid] = [True,False]  # 注册 plotid，启动画图，并设置 startplot 调用次数为1
        else:
            portfolio.PLOT_OBJ[self.plotid][0] = True


    def stopplot(self):
        # 只有在已经注册过画图的对象才能进行画图停止 ： 停止不是不动，而是画直线
        portfolio.PLOT_OBJ[self.plotid][1] = False


    def ploting(self):
        # 只有在已经注册过 plotid 画图的对像才能 ploting
        while (portfolio.END >= dt.datetime.now() >= portfolio.START):
            if not portfolio.PLOT_OBJ[self.plotid][1]:   # 还没有建过图
                x = [dt.datetime.now()]
                y = [(self.addvalue['floated'] + self.addvalue['fixed']) / self.pofvalue]
                # ax = portfolio.FIG.add_subplot(str(portfolio.PLOT_SHAPE[0]) + str(portfolio.PLOT_SHAPE[1]) + str(self.plotid))
                # ax.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))  # 设置时间标签显示格式 '%Y-%m-%d %H:%M:%S'
                # ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.4f%%'))
                # ax.set_title(self.pofname)
                portfolio.PLOT_OBJ[self.plotid][1] = True

            if portfolio.PLOT_OBJ[self.plotid][0]:  # 是否启动
                y.append((self.addvalue['floated'] + self.addvalue['fixed']) / self.pofvalue * 100)
            else:
                y.append(y[-1])
            x.append(dt.datetime.now())
            self.ax.legend(('return : %.4f%%' % y[-1],))
            self.ax.set_xlim(x[0], x[-1])
            self.ax.plot(x[1:], y[1:], linewidth=1, color='r')
            plt.pause(0.01)
            time.sleep(portfolio.FLUSH_TICK)

    def update_object(self):  # 定时扫描交易状态 trdlistdir 并更新 holdlist
        statchg = self.flush_trdstat()
        if statchg == 1:
            self.startplot()
        elif statchg == -1:
            self.stopplot()
        if UNDL_POOL_INFO:
            self.update_addvalue()








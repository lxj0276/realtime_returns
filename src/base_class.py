import os
import time
import datetime as dt
import numpy as np
import pandas as pd
from WindPy import w
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import matplotlib.ticker as mtick
import matplotlib as mpl
from src.help_functions import *


class portfolio:

    FLUSH_POOL = 1
    FLUSH_CWSTAT=1
    UNDL_POOL={'total':set([])}   # 数据结构 dict： {'total' : 所有标的 set , 'pofname1': {'stocks': set , 'futures': set , ...} , 'pofname2': {...} , ... }
    POOL_COLUMNS='rt_time,rt_last'  #,rt_pct_chg'
    UNDL_POOL_INFO={}   # 存储以各个品种code为key的字典
    PLOT_OBJ=[]   # 需要画图的对象
    REGI_OBJ = [] # 所有已创建的对象


    @staticmethod
    def undlpool_callback(indata):
        maxlen=len(portfolio.POOL_COLUMNS.split(','))
        if indata.ErrorCode!=0:
            raise Exception('Error in callback with ErrorCode %d' %indata.ErrorCode)  # 实际使用时，为防止中断可改为log输出
        for dumi in range(len(indata.Codes)):
            fieldlen=len(indata.Fields)
            if fieldlen==maxlen:   # 只有在所有field都有数据的时候才存储
                tempdata=[]
                for dumj in range(fieldlen):
                    tempdata.append(indata.Data[dumj][dumi])
                portfolio.UNDL_POOL_INFO[indata.Codes[dumi]]=tempdata


    @classmethod
    def update_undlpool(cls):

        w.start()
        tot=list(portfolio.UNDL_POOL['total'])
        underlyings=''.join(tot)
        underlyings=underlyings.replace("SH","SH,")
        underlyings=underlyings.replace("SZ","SZ,")
        underlyings=underlyings.replace("CFE","CFE,")

        w.wsq(underlyings,portfolio.POOL_COLUMNS,func=portfolio.undlpool_callback)   # 订阅POOL里面所有的undelryings

        today=dt.date.today()
        start=dt.datetime(year=today.year, month=today.month,day=today.day,hour= 9,minute=30,second=0)
        end=dt.datetime(year=today.year, month=today.month,day=today.day,hour= 18,minute=00,second=0)


        # 画图配置
        shape=calc_shape(len(portfolio.REGI_OBJ))
        #mpl.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
        plt.ion()
        fig=plt.figure(figsize=(20,20))
        fig.canvas.set_window_title(str(today))
        #plt.xticks(pd.date_range(start,end,freq='H'))#时间间隔

        x = {}
        y = {}
        axes = {}

        while( end>= dt.datetime.now() >= start):
            time.sleep(portfolio.FLUSH_CWSTAT)
            for obj in portfolio.REGI_OBJ:
                obj.update_object()

            xkeys=x.keys()
            count=1
            for obj in portfolio.PLOT_OBJ:
                if obj not in xkeys:
                    x[obj]=[dt.datetime.now()]
                    y[obj]=[(obj.addvalue['floated']+obj.addvalue['fixed'])/obj.pofvalue]
                    ax=fig.add_subplot(str(shape[0])+str(shape[1])+str(count))
                    ax.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))#设置时间标签显示格式 '%Y-%m-%d %H:%M:%S'
                    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.4f%%'))
                    ax.set_title(obj.pofname)
                    #ax.set_ylim(-0.02,0.02)
                    axes[obj]=ax
                y[obj].append((obj.addvalue['floated']+obj.addvalue['fixed'])/obj.pofvalue)
                x[obj].append(dt.datetime.now())
                axes[obj].set_xlim(x[obj][0],x[obj][-1])
                axes[obj].plot(x[obj],y[obj],linewidth=1,color='r')
                plt.pause(0.05)
                count+=1

        print('temp: update finished')
        ######   保存图像 ######


    @classmethod
    def add_pool(cls,lst,pofname):
        # 把list加入资产池
        # lst 结构为{交易品种：对应数据表} 如{stocks:pd.dtfm, futures: pd.dtfm}
        poolkeys=portfolio.UNDL_POOL.keys()
        if pofname not in poolkeys:  # 如果pool中没有该产品则新创建
            tempdict = {}
            for k in lst.keys():
                toadd = set(lst[k]['code'])
                tempdict[k] = toadd
                portfolio.UNDL_POOL['total'] |= toadd
            portfolio.UNDL_POOL[pofname]=tempdict
        else:
            for k in lst.keys():
                toadd = set(lst[k]['code'])
                portfolio.UNDL_POOL[pofname][k] |= toadd
                portfolio.UNDL_POOL['total'] |= toadd


    @classmethod
    def pop_pool(cls,pofname,poplst):
        # 从 pool 中删除
        # 根据特定 portfolio pop
        poolkeys=portfolio.UNDL_POOL.keys()
        if pofname in poolkeys:
            #holdonly = portfolio.UNDL_POOL[pofname]  # 提取该组合特有的股票删除，不能删除 total 中与其他组合共有的股票
            holdonly = poplst
            for k in poolkeys:
                if k not in ( 'total', pofname):
                    for undl in holdonly.keys():
                        holdonly[undl] -= portfolio.UNDL_POOL[k][undl]
            for und in holdonly.keys():      # 删除 total 中对应部分
                portfolio.UNDL_POOL['total'] -= holdonly[und]
            del portfolio.UNDL_POOL[pofname]  # 删除对应产品, 可能仍有部分碎股会存在


    def __init__(self,pofname,pofvalue,hldlstdir,trdlstdir,cwstatusdir):
        self.starttime = time.time()
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

        if not self.noposition: # 交易开始前就有持仓的情况下，加入holdlist, 当天开始时无持仓则不必
            self.holdlist['T1'] = self.read_holdlist()
            if self.holdlist['T1']:
                portfolio.add_pool(self.holdlist['T1'],self.pofname)
                portfolio.PLOT_OBJ.append(self)
        else:
            self.holdlist = {}
        portfolio.REGI_OBJ.append(self)


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
        statschg = currtrdstat[:,0]-self.lasttrdstat[:,0]
        updtstat = False
        statchg = 0
        if np.any(statschg):   # 持仓有变动
            if not np.any(currtrdstat[:,0]): # 从有持仓变为空仓
                trdlst = self.read_trdlist()
                if trdlst['out']:
                    self.update_holdlist(trdlst['out'],'T1')  # 当天只能卖出T+1的股票
                    portfolio.pop_pool(self.pofname,trdlst['out'])
                    self.noposition = True
                    updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                    statchg = -1
            elif not np.any(self.lasttrdstat[:,0]): # 从无持仓变为有持仓
                trdlst = self.read_trdlist()
                if trdlst['in']:
                    self.holdlist['T0'] = trdlst['in']     # 当天买入的股票算作 T+0
                    portfolio.add_pool(self.holdlist['T0'],self.pofname)
                    self.noposition = False
                    updtstat = True
                    statchg = 1
                    self.starttime = time.time()
            else:  # 有持仓，且持仓变动  #####　逻辑有缺失　！没有考虑
                trdlst=self.read_trdlist()
                print(trdlst)
                if trdlst['in'] and trdlst['out']:
                    self.update_holdlist(trdlst['in'],'T0')
                    self.update_holdlist(trdlst['out'],'T1')
                    portfolio.add_pool(trdlst['in'],self.pofname)
                    updtstat = True
                    self.starttime = time.time()

            w.start()
            tot=list(portfolio.UNDL_POOL['total'])
            underlyings=''.join(tot)
            underlyings=underlyings.replace("SH","SH,")
            underlyings=underlyings.replace("SZ","SZ,")
            underlyings=underlyings.replace("CFE","CFE,")
            w.wsq(underlyings,portfolio.POOL_COLUMNS,func=portfolio.undlpool_callback)
        if updtstat:
            self.lasttrdstat=currtrdstat
        return statchg


    def read_trdlist(self):  # 返回 买入 卖出两个方向的单子
        trdlist = {}
        trdlist['in'] = {}
        trdlist['out'] = {}
        for k in self.trdlstdir.keys():
            files=os.listdir(self.trdlstdir[k])
            if k not in self.trdlist.keys():
                self.trdlist[k] = []
            newfiles = set(files)-set( self.trdlist[k])
            if newfiles:
                templist = pd.DataFrame()
                for f in newfiles:
                    templist = templist.append( pd.read_csv(os.path.join(self.trdlstdir[k],f),encoding='gb2312',names=['code','name','num','prc','val','side']) , ignore_index=True)
                    self.trdlist[k].append(f)
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
        # 返回字典结构,字典的key是各个不同品种的标的，如stocks\futures\options etc
        holdlist={}
        for k in self.hldlstdir.keys():
            holdlist[k] = pd.read_csv(self.hldlstdir[k],encoding='gb2312',names=['code','name','num','prc','val'])
            holdlist[k].index = holdlist[k]['code'].tolist()
        return holdlist


    def update_holdlist(self,lst,type):  # 做多数量为正，做空数量为负
        if type not in ('T0','T1'):
            raise Exception('Need to specify the type of holdlist to be updated!')
        if type=='T1':     # 只在出场的时候更新T+1
            for k in lst.keys():
                codes = lst[k]['code']
                num = lst[k]['num']
                self.holdlist['T1'][k].loc[codes,'num'].values += num
                self.addvalue['fixed'] += (self.holdlist['T1'][k].loc[codes,'prc'].values-lst[k]['prc'])*num
        else:      # 只在当日入场时更新 T+0
            if len(self.holdlist['T0'])==0:
                self.holdlist['T0'] = lst
            else:
                for k in lst.keys():
                    temp=pd.concat([self.holdlist['T0'][k],lst[k]],ignore_index=True)   # type:pd.DataFrame
                    grouped=temp.groupby('code').sum()
                    grouped['code'] = grouped.index
                    grouped['prc'] = grouped['val'].values/grouped['num'].values
                    self.holdlist['T0'] = grouped

    
    def update_addvalue(self):
        # 只计算尚未卖出的收益，即floated 收益
        # 当日卖出的部分属于 fixed 收益，在 update_holdlist 中计算
        if not self.holdlist:  # 调用时没有持仓
            return
        newinfo = pd.DataFrame( portfolio.UNDL_POOL_INFO, index = portfolio.POOL_COLUMNS.split(',')  ).T
        addval = 0
        for tp in self.holdlist.keys():
            holding=self.holdlist[tp]
            for k in holding.keys():
                holdingK=holding[k]
                addval += np.sum( (newinfo.loc[holdingK['code'].tolist(),'rt_last'].values - holdingK['prc'].values) * holdingK['num'].values )
        self.addvalue['floated'] = addval


    def startplot(self):   # 如需画图，则将该产品 对象 添加到 portfolio 类画图列表中
        portfolio.PLOT_OBJ.append(self)


    def stopplot(self):
        portfolio.PLOT_OBJ.remove(self)


    def update_object(self):  # 定时扫描交易状态 trdlistdir 并更新 holdlist
        statchg = self.flush_trdstat()
        if statchg == 1:
            self.startplot()
        elif statchg == -1:
            self.stopplot()
        if time.time() - self.starttime > 1:  # 自上次变更 POOL后，留点时间进行订阅数据送达 不是很必要
            self.update_addvalue()




if __name__=='__main__':
    pofname1='test1'
    pofvalue1=820386
    hldlstdir1={'stocks' : '..\BQ1ICLong20170421.csv'}
    trdlstdir1=''
    cwstatusdir1=r'..\cwstate1.txt'

    t1=portfolio(pofname1,pofvalue1,hldlstdir1,trdlstdir1,cwstatusdir1)

    pofname2='test2'
    pofvalue2=820386
    hldlstdir2={'stocks' : '..\BQ1ICLong20170421.csv'}
    trdlstdir2=''
    cwstatusdir2=r'..\cwstate2.txt'

    t2=portfolio(pofname2,pofvalue2,hldlstdir2,trdlstdir2,cwstatusdir2)

    pofname3='test3'
    pofvalue3=820386
    hldlstdir3={'stocks' : '..\BQ1ICLong20170421.csv'}
    trdlstdir3={'stocks' : r'E:\realtime_monitors\realtime_returns\testfiles'}
    cwstatusdir3=r'..\cwstate3.txt'

    t3=portfolio(pofname3,pofvalue3,hldlstdir3,trdlstdir3,cwstatusdir3)

    portfolio.update_undlpool()
    w.cancelRequest(0)




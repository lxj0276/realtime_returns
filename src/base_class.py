import threading
import time
import datetime as dt
import numpy as np
import pandas as pd
from WindPy import w


class portfolio:

    FLUSH_POOL = 5
    FLUSH_CWSTAT=1
    UNDL_POOL={'total':set([])}   # 数据结构 dict： {'total' : 所有标的 set , 'pofname1': {'stocks': set , 'futures': set , ...} , 'pofname2': {...} , ... }
    POOL_COLUMNS='rt_time,rt_last,rt_pct_chg'
    UNDL_POOL_INFO={}   # 存储以各个品种code为key的字典


    @staticmethod
    def undlpool_callback(indata):
        maxlen=3
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
        underlyings=''
        for undl in portfolio.UNDL_POOL['total']:
            underlyings=underlyings.join(undl)
        w.wsq(underlyings,portfolio.POOL_COLUMNS,func=portfolio.undlpool_callback)
        today=dt.date.today()
        start=dt.datetime(year=today.year, month=today.month,day=today.day,hour= 8,minute=0,second=0)
        end=dt.datetime(year=today.year, month=today.month,day=today.day,hour= 15,minute=15,second=0)
        while( end>= dt.datetime.now() >= start):
            info="这个while循环主要是防止IDE在运行或者debug时，运行w.wsq()语句后就退出，从而导致行情推送过来后，回调函数无法运行！"
            time.sleep(1)
            print(portfolio.UNDL_POOL_INFO)
        print('temp: update finished')


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
    def pop_pool(cls,pofname):
        # 从 pool 中删除
        # 根据特定 portfolio pop
        poolkeys=portfolio.UNDL_POOL.keys()
        if pofname in poolkeys:
            holdonly=portfolio.UNDL_POOL[pofname]  # 提取该组合特有的股票删除，不能删除 total 中与其他组合共有的股票
            for k in poolkeys:
                if k not in ( 'total', pofname):
                    for undl in holdonly.keys():
                        holdonly[undl] -= portfolio.UNDL_POOL[k][undl]
            for und in holdonly.keys():      # 删除 total 中对应部分
                portfolio.UNDL_POOL['total'] -= holdonly[und]
            del portfolio.UNDL_POOL[pofname]  # 删除对应产品


    def __init__(self,pofname,pofvalue,hldlstdir,trdlstdir,cwstatusdir):
        self.pofname=pofname
        self.pofvalue=pofvalue                 # 产品前一日总资产
        self.hldlstdir=hldlstdir               # 基于标的的字典 ex. {'stocks':dir1,futures:dir2}
        self.trdlstdir=trdlstdir               # 基于标的的字典
        self.cwstatusdir=cwstatusdir
        self.lasttrdstat=self.get_trdstat()   # 创建对象的时间应该在 开始交易时间之前 （开盘前 或 9：35 之前）
        self.noposition=not np.any(self.lasttrdstat[:,0])

        self.addvalue={'fixed':0 ,'floated':0 }  # 若在当天出场则为fixed收益，否则为floated
        self.holdlist={'T1':{},'T0':{}}     # holdlist数据结构 ： { 'T1':{}, 'T0':{}} , T+0/T+1 部分分别存储一个基于品种的、类似lst的dict

        if not self.noposition: # 交易开始前就有持仓的情况下，加入holdlist, 当天开始时无持仓则不必
            self.holdlist['T1']=self.read_holdlist()
            if self.holdlist['T1']:
                portfolio.add_pool(self.holdlist['T1'],self.pofname)
        else:
            self.holdlist=None


    def get_trdstat(self):
        # 返回从cwstate.txt读取的内容，类型为np array
        with open(self.cwstatusdir,'r') as cwinfo:
            temp=cwinfo.readlines()
            contents_temp=[c.strip().split(',') for c in temp]
            contents=[[int(c) for c in t] for t in contents_temp]
        return np.array(contents)


    def flush_trdstat(self):
        # 根据持仓状态变更，更新 holdlist以及资产池，并更新 lasttrdstat
        currtrdstat=self.get_trdstat()
        statchg=currtrdstat[:,0]-self.lasttrdstat[:,0]
        if np.any(statchg):   # 持仓有变动
            if not np.any(currtrdstat[:,0]): # 从有持仓变为空仓
                trdlst=self.read_trdlist()
                self.update_holdlist(trdlst['out'],'T1')  # 当天只能卖出T+1的股票
                self.noposition=True
                portfolio.pop_pool(self.pofname)
            elif not np.any(self.lasttrdstat[:,0]): # 从无持仓变为有持仓
                trdlst=self.read_trdlist()
                self.holdlist['T0']=trdlst['in']     # 当天买入的股票算作 T+0
                self.noposition=False
                portfolio.add_pool(self.holdlist['T0'],self.pofname)
            else:  # 有持仓，且持仓变动  # need more contents ,pause for now
                trdlst=self.read_trdlist()
                self.update_holdlist(trdlst['in'],'T0')
                self.update_holdlist(trdlst['out'],'T1')
                portfolio.add_pool(trdlst['in'],self.pofname)
        self.lasttrdstat=currtrdstat


    def read_holdlist(self):
        # 返回字典结构,字典的key是各个不同品种的标的，如stocks\futures\options etc
        holdlist={}
        for k in self.hldlstdir.keys():
            holdlist[k]=pd.read_csv(self.hldlstdir[k],encoding='gb2312',names=['code','name','num','prc','val'])
            holdlist[k].index=holdlist[k]['code'].tolist()
        return holdlist


    def update_holdlist(self,lst,type):  # 做多数量为正，做空数量为负
        if type not in ('T0','T1'):
            raise Exception('Need to specify the type of holdlist to be updated!')
        if type=='T1':     # 只在出场的时候更新T+1
            for k in lst.keys():
                codes = lst[k]['code']
                num = lst[k]['num']
                self.holdlist['T1'][k].loc[codes,'num'].values += num
                self.addvalue['fixed']+=(self.holdlist['T1'][k].loc[codes,'prc'].values-lst[k]['prc'])*num
        else:      # 只在当日入场时更新 T+0
            if len(self.holdlist['T0'])==0:
                self.holdlist['T0']=lst
            else:
                for k in lst.keys():
                    temp=pd.concat([self.holdlist['T0'][k],lst[k]],ignore_index=True)   # type:pd.DataFrame
                    grouped=temp.groupby('code').sum()
                    grouped['code']=grouped.index
                    grouped['prc']=grouped['val'].values/grouped['num'].values
                    self.holdlist['T0']=grouped


    def read_trdlist(self):  # 返回 买入 卖出两个方向的单子
        trdlist={}
        trdlist['in']={}
        trdlist['out']={}
        pass

    
    def take_from_pool(self):
        if self.noposition:
            pass

    
    def update_addvalue(self):
        # 只计算尚未卖出的收益，即floated 收益
        # 当日卖出的部分属于 fixed 收益，在 update_holdlist 中计算
        newinfo=pd.DataFrame(portfolio.UNDL_POOL_INFO,index=portfolio.UNDL_POOL_INFO.keys())
        addval=0
        for tp in self.holdlist.keys():
            holding=self.holdlist[tp]
            for k in holding.keys():
                addval+=(newinfo[holding[k]['code'].values].values - holding[k]['prc']) * holding[k]['num'].values
        self.addvalue['floated']=addval

    
    def run(self):
        pass


    def startplot(self):
        pass




class test:
    a=0

    def __init__(self,v):
        test.a+=1
        self.b=v


if __name__=='__main__':
    pofname='test'
    pofvalue=1000
    hldlstdir={'stocks' : '..\m_JQ1ICLong20170412.csv'}
    trdlstdir=''
    cwstatusdir=r'..\cwstate.txt'

    t=portfolio(pofname,pofvalue,hldlstdir,trdlstdir,cwstatusdir)

    portfolio.update_undlpool()
    # w.cancelRequest(0)




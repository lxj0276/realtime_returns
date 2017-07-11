
import datetime as dt
import os
import time

import matplotlib as mpl
import matplotlib.animation as man
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd

from src.data_subscribe import *
from src.global_vars import *
from src.help_functions import *
from raw_trading_process import *


class Portfolio:

    CHARGE_TIME = 1    # 当日交易添加到 POOL 后，等待的时间  是否必要？
    PLOT_OBJ = {}      # 需要画图的对象，结构为{plotid:[object, True/False]}
    REGI_OBJ = []      # 所有已创建的实例
    PLOT_NUM = 0       # 需要画图的数量
    FIGDIR = r'.\saved_figures'   # 图像存储路径

    @classmethod
    def update_undlpool(cls):
        """ 更新实例状态并画图 """
        # 订阅数据源
        data_subscribe(SUBSCRIBE_SOURCE)
        # 图像基本配置
        shape=calc_shape(len(Portfolio.REGI_OBJ))
        mpl.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
        fig=plt.figure(figsize=(20,20))
        fig.canvas.set_window_title('  '.join([TODAY,'VERSION : '+VERSION]))
        xaxis = {}
        yaxis = {}
        axes = {}
        ptscount = {}
        loopcount = 0
        while(  START_TIME<= dt.datetime.now()<= END_TIME):
            if MID1_TIME < dt.datetime.now() < MID2_TIME:  # 午休时间
                continue
            ################# PART1 : 更新所有实例 ,暂停画图的也要更新，因为x轴为时间正常更新 #####################
            t1 = time.time()
            for obj in Portfolio.REGI_OBJ:
                obj.update_object()
            print('t1: %f' % (time.time()-t1))
            ################# PART2 : 设置画图 并 画图 #####################
            t2 = time.time()
            count = 1   # 第几个子图
            for id in sorted( Portfolio.PLOT_OBJ ):    #  Portfolio.PLOT_OBJ 是以增加过的画图 obj 的 regid
                plobj = Portfolio.PLOT_OBJ[id]
                obj = plobj[0]
                ploting = plobj[1]
                ######### 计算坐标点,如果是初次画图还需设置图像对象 ##############
                if obj not in xaxis:   # 初次画图,只有一个点
                    xaxis[obj] = [0]*PLOT_POINTS
                    yaxis[obj] = [0]*PLOT_POINTS
                    xaxis[obj][0] = dt.datetime.now()
                    yaxis[obj][0] = (obj._addvalue['floated']+obj._addvalue['fixed'])/obj._pofvalue * 100
                    ptscount[obj] = 0
                    ax = fig.add_subplot(str(shape[0])+str(shape[1])+str(count))
                    ax.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))
                    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.4f%%'))
                    ax.set_title(obj._pofname)
                    axes[obj] = ax
                    count +=1
                else:  # 此前画过图，有两个以上点 计算画图坐标点
                    if ploting:   # 正常画图的情况
                        yaxis[obj][ptscount[obj]] = (obj._addvalue['floated']+obj._addvalue['fixed'])/obj._pofvalue * 100
                    else:        # 画图暂停的情况，y轴延续上一个数值
                        yaxis[obj][ptscount[obj]] = yaxis[obj][ptscount[obj]-1]
                    xaxis[obj][ptscount[obj]] = dt.datetime.now()
                # 设置图例，因为每次都是画一张新图
                if ptscount[obj]>=1:  # 至少两个点以上才能画图
                    axes[obj].set_xlim(xaxis[obj][0], xaxis[obj][ptscount[obj]])
                    axes[obj].legend(('return : %.4f%%' % yaxis[obj][ptscount[obj]],))
                    axes[obj].plot(xaxis[obj][0:ptscount[obj]], yaxis[obj][0:ptscount[obj]], linewidth=1, color='r')
                ptscount[obj] += 1
            print('t2: %f' % (time.time()-t2))
            ############## PART3 ： 更新图表，所有子图一起更新 ##############################
            t3 = time.time()
            if loopcount>=1:
                plt.pause(FLUSH_CWSTAT)
            loopcount+=1
            print('t3: %f' % (time.time()-t3))
        #################### 画图完成，保存图像 ########################
        print('plot finished')
        figpath = os.path.join(Portfolio.FIGDIR,TODAY+'.png')
        if not os.path.exists(figpath):
            plt.savefig(figpath)
        print('plots saved')





    ####################################################################################################################
    ####################################################################################################################
    @classmethod
    def update_undlpool_v2(cls):
        """ 更新实例状态并画图 """
        # 订阅数据源
        data_subscribe(SUBSCRIBE_SOURCE)
        # 图像基本配置
        shape=calc_shape(len(Portfolio.REGI_OBJ))
        mpl.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
        fig=plt.figure(figsize=(20,20))
        fig.canvas.set_window_title('  '.join([TODAY,'VERSION : '+VERSION]))

        xaxis = {}
        yaxis = {}
        axes = {}
        lns = {}
        count = 1
        for id in sorted( Portfolio.PLOT_OBJ ):    #  Portfolio.PLOT_OBJ 是以增加过的画图 obj 的 regid
            plobj = Portfolio.PLOT_OBJ[id]
            obj = plobj[0]
            if obj not in xaxis:   # 初次画图,只有一个点
                xaxis[obj] = [dt.datetime.now()]
                yaxis[obj] = [(obj._addvalue['floated']+obj._addvalue['fixed'])/obj._pofvalue * 100]
                ax = fig.add_subplot(str(shape[0])+str(shape[1])+str(count))
                ax.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))
                ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.4f%%'))
                ax.set_title(obj._pofname)
                ln, = ax.plot(xaxis[obj], yaxis[obj], linewidth=1, color='r')
                axes[obj] = ax
                lns[obj] = ln
                count +=1

        def init():
            for id in sorted( Portfolio.PLOT_OBJ ):
                plobj = Portfolio.PLOT_OBJ[id]
                obj = plobj[0]
                axes[obj].set_xlim(xaxis[obj][0], xaxis[obj][-1])
                axes[obj].legend(('return : %.4f%%' % yaxis[obj][-1],))

        def data_gen():
            while(  START_TIME<= dt.datetime.now()<= END_TIME):
                if MID1_TIME < dt.datetime.now() < MID2_TIME:  # 午休时间
                    continue
                else:
                    time.sleep(FLUSH_CWSTAT)
                    for id in sorted( Portfolio.PLOT_OBJ ):
                        plobj = Portfolio.PLOT_OBJ[id]
                        obj = plobj[0]
                        ploting = plobj[1]
                        if ploting:   # 正常画图的情况
                            yaxis[obj].append( (obj._addvalue['floated']+obj._addvalue['fixed'])/obj._pofvalue * 100 )
                        else:        # 画图暂停的情况，y轴延续上一个数值
                            yaxis[obj].append( yaxis[obj][-1] )
                        xaxis[obj].append(dt.datetime.now())
                yield [xaxis,yaxis]

        def update(data):
            for id in sorted( Portfolio.PLOT_OBJ ):
                plobj = Portfolio.PLOT_OBJ[id]
                obj = plobj[0]
                lns[obj].set_data(data[0][obj],data[1][obj])

        man.FuncAnimation(fig=fig,func=update,frames=data_gen,init_func=init)
        plt.show()

        print('plot finished')
        figpath = os.path.join(Portfolio.FIGDIR,TODAY+'.png')
        if not os.path.exists(figpath):
            plt.savefig(figpath)
        print('plots saved')

    ###################################################################################################################
    ####################################################################################################################




    @classmethod
    def add_pool(cls,pofname,addcodes):
        """ 把codes对应标的加入资产池 """
        # UNDL_POOL 必须包含 'total' 关键字
        if pofname not in UNDL_POOL:  # 如果pool中没有该产品则新创建
            toadd = set(addcodes)
            UNDL_POOL['total'] |= toadd
            UNDL_POOL[pofname] = toadd
        else:
            toadd = set(addcodes)
            UNDL_POOL[pofname] |= toadd
            UNDL_POOL['total'] |= toadd

    @classmethod
    def pop_pool(cls,pofname,popcodes):
        """ 从 pool 中删除对应组合，提取该组合特有的股票删除，不能删除 total 中与其他组合共有的股票 """
        # 根据特定 Portfolio pop
        if pofname in UNDL_POOL:
            holdonly = set(popcodes)
            for k in UNDL_POOL:
                if k not in ( 'total', pofname):
                    holdonly -= UNDL_POOL[k]
            UNDL_POOL['total'] -= holdonly   # 删除 total 中对应部分
            del UNDL_POOL[pofname]  # 删除对应产品, 可能仍有部分碎股会存在


    def __init__(self,pofname,pofval_dir,holdlst_dir,trdlst_dir,handlst_dir,cwstatus_dirs):
        self._pofname = pofname                               # 产品名称
        # 读取配置文件
        #-----------------------  基本路径 --------------------------------------
        self._pofval_dir = pofval_dir                           # 产品资产存储文件路径
        self._holdlst_dir = holdlst_dir                         # 组合持仓文件路径，数据结构为 基于标的的字典 ex. {'stocks':_dir1,futures:_dir2}
        self._trdlst_dir = trdlst_dir                           # 当日交易单子文件路径，类似于holdlst_dir 是基于标的的字典
        self._handlst_dir = handlst_dir                         # 手动交易单子路径
        self._cwstatus_dirs = cwstatus_dirs                       # 交易状态文件路径
        #-----------------------  变量初始化 --------------------------------------
        self._pofvalue = self.get_pofvalue()                  # 初始化总资产
        self._holdings = {'T1':pd.DataFrame(),'T0':pd.DataFrame()}                   # 初始化组合持仓，T0对应今日买入的资产,T1对应今日之前买入的资产 , T+0/T+1 部分分别存储一个基于品种的、类似lst的dict
        self._addvalue = {'fixed':0 ,'floated':0 }          # 组合收益数值，若在当天出场则为fixed收益，否则为floated
        self._trdlist = {}                                    # 初始化交易单子
        self._handlist = {}                                   # 初始化手动交易单子
        for undl in trdlst_dir:                              # 清空所有交易单存储所在的文件夹，避免前一日的交易单对今天造成影响
            clear_dir(trdlst_dir[undl])
        for undl in self._handlst_dir:                        # 清空手动交易单子路径
            clear_dir(handlst_dir[undl])
        #-----------------------  交易状态设置 --------------------------------------
        self._noposition = {}
        self._lastcwstate = self.cwstate_snapshot(predaystat=True)       # 初始化交易状态,使用前一日的cwstate交易状态，改文件路径可由cwstate路径推出
        for strategy in self._lastcwstate:  # 检查是否有持仓
            self._noposition[strategy] = not np.any(self._lastcwstate[strategy][:,0])
        #-----------------------  画图配置 --------------------------------------
        self._plotid = 0                                      # 初始化该实例的plotid，如果需要画图的话该值应该为大于0的值，不需要的话则为0
        if not np.all(list(self._noposition.values())):                  # 交易开始前就有持仓的情况下，加入holdings, 当天开始时无持仓则不必
            self._holdings['T1'] = self.read_holdlist()
            assert self._holdings['T1'] is not None,'有持仓 hold T1 不应为None!'
            Portfolio.add_pool(addcodes=self._holdings['T1']['code'],pofname=self._pofname)    # 将持仓增加到 POOL中
            Portfolio.PLOT_NUM += 1                                  # 有持仓则确定该对象需要画图，增加类需要的画图数目
            self._plotid = Portfolio.PLOT_NUM                         # 设定 plotid 为第几个需要画的图
            Portfolio.PLOT_OBJ[self._plotid] = [self,True]            # 第二个布尔值为画图开关，当停止画图时会被设置为False
        Portfolio.REGI_OBJ.append(self)                                # 将该实例对象添加到类的实例记录列表
        print('%s : portfolio created ! ' % self._pofname)

    def get_pofvalue(self):
        with open(self._pofval_dir, 'r') as pofinfo:
            val = float(pofinfo.readlines()[0].strip())
        return val

    def cwstate_snapshot(self,predaystat=False):
        """ 返回从cwstate.txt读取的内容，类型为np array
            如果需要前一日的交易状态，则需从当前cwstate路径推出前一日交易状态的路径
            可以这样推是因为目前cwstate_history 文件夹 和 cwstate.txt 在同一目录下 ！
            根据各个策略提取，结果为以策略名为Key的字典
        """
        contents = {}
        length = 6
        for strategy in self._cwstatus_dirs:
            cw = self._cwstatus_dirs[strategy]
            if predaystat:   # 提取前一日交易状态路径
                cwdir = os.path.join(cw,''.join(['cwstate_history\cwstate_',YESTERDAY,'.txt']))
            else:
                cwdir = os.path.join(cw,'cwstate.txt')
            with open(cwdir,'r') as cwinfo:
                temp = cwinfo.readlines()
                contents_temp = [c.strip().split(',') for c in temp]
                contents[strategy] = np.array([([float(c) for c in t]) for t in contents_temp if len(t)==length])   # 确保足够长，过滤掉意外空行的情况
        return contents

    def get_trdstat(self,precwstat,currcwstat,trdtype='T+1'):
        """ 计算两个不同cwstate快照之间的交易状况 """
        precwstat = np.array(precwstat)
        currcwstat = np.array(currcwstat)
        statchg = currcwstat[:,0] - precwstat[:,0]    # 假设两次状态间档位数相同，否则将出错
        inlvs = np.sum(statchg < 0)     # 入场档数
        outlvs = np.sum(statchg > 0)    # 出场档数
        holdlvs = np.sum(statchg == 0)  # 未动档数
        totlvs = inlvs+outlvs+holdlvs    # 总档数
        poslvs = np.sum(currcwstat[:,0]<0) #当前持有档数
        preposlvs = np.sum(precwstat[:,0]<0) #此前持有档数
        if trdtype=='T+1':
            if preposlvs==0 and poslvs>0: # 有交易 无持仓->有持仓 ： 只有进
                assert outlvs==0
                trdstat = '021'
            elif preposlvs>0 and poslvs==0: # 有交易 有持仓->无持仓 ： 只有出
                assert inlvs==0
                trdstat = '120'
            elif preposlvs>0 and poslvs>0 and (holdlvs<totlvs):  # 有交易 有持仓->有持仓 : 进出都有可能
                assert(inlvs>0 or outlvs>0)
                trdstat = '121'
            else:
                assert(inlvs==0 and outlvs==0)
                trdstat = 'NoTrade'
            return {'trdstat':trdstat,'inlvs' : inlvs, 'outlvs': outlvs, 'holdlvs':holdlvs,'poslvs':poslvs,'totlvs':totlvs}
        else:
            raise NotImplementedError('No other trdtype specified yet')

    def flush_trdstat(self):
        """ 根据持仓状态变更，更新 holdlist以及资产池，并更新 lasttrdstat
            如果交易状态改变，会不断扫描交易单文件夹，直到成功读入 对应交易记录
        """
        currcwstat = self.cwstate_snapshot()
        updtstat = False
        # 提取基于策略的交易情况
        for strategy in currcwstat:
            trdstat = self.get_trdstat(self._lastcwstate[strategy],currcwstat[strategy])
            if trdstat['trdstat'] != 'NoTrade':   # 有交易发生
                print('%s %s: trade happend with InLevels:%d -- OutLevels:%d -- HoldLevels:%d' %(self._pofname,strategy,trdstat['inlvs'],trdstat['outlvs'],trdstat['holdlvs']))
                trdlst = self.read_trdlist(strategy=strategy)
                if trdstat['trdstat']=='120': # 从有持仓变为空仓
                    if trdlst['out']:
                        self.update_holdings(trdlst['out'],'T1')  # 当天只能卖出T+1的股票
                        self._holdings = {'T0':{},'T1':{}}
                        Portfolio.pop_pool(self._pofname,trdlst['out'])
                        self._noposition[strategy] = True
                        updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                        print('%s %s: trading list found : Out' % (self._pofname,strategy))
                    else:
                        print('%s %s: waiting for trading list ...' % (self._pofname,strategy))
                elif trdstat['trdstat']=='021':  # 从无持仓变为有持仓
                    if trdlst['in']:
                        #self._holdings['T0'] = trdlst['in']     # 当天买入的股票算作 T+0
                        self.update_holdings(trdlst['in'],'T0')
                        Portfolio.add_pool(self._holdings['T0'],self._pofname)
                        self._noposition[strategy] = False
                        updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                        print('%s %s: trading list found : In' % (self._pofname,strategy))
                    else:
                        print('%s %s: waiting for trading list ...' % (self._pofname,strategy))
                elif trdstat['trdstat']=='121':  # 有持仓，且持仓变动
                    if trdstat['inlvs'] == 0:   # 只有卖出
                        if trdlst['out']:
                            self.update_holdings(trdlst['out'],'T1')
                            updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                            print('%s %s: trading list found : Out' % (self._pofname,strategy))
                        else:
                            print('%s %s: waiting for trading list ...' % (self._pofname,strategy))
                    elif trdstat['outlvs'] == 0: # 只有买入
                        if trdlst['in']:
                            self.update_holdings(trdlst['in'],'T0')
                            Portfolio.add_pool(trdlst['in'],self._pofname)
                            updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                            print('%s %s: trading list found : In' % (self._pofname,strategy))
                        else:
                            print('%s %s: waiting for trading list ...' % (self._pofname,strategy))
                    else:  # 买卖都有
                        if trdlst['in'] and trdlst['out']:
                            self.update_holdings(trdlst['in'],'T0')
                            self.update_holdings(trdlst['out'],'T1')
                            Portfolio.add_pool(trdlst['in'],self._pofname)
                            updtstat = True   # 只有在trdlist完成提取后才会更新 trdstat, 防止trdlist 更新较慢的情况
                            print('%s %s: trading list found : In & Out' % (self._pofname,strategy))
                        else:
                            print('%s %s: waiting for trading list ...' % (self._pofname,strategy))
            if updtstat:  # 持仓更新成功，单子已经到达录入成功
                data_subscribe(SUBSCRIBE_SOURCE)
                self._lastcwstate[strategy] = currcwstat[strategy]  # 更新前一cwstate
                time.sleep(Portfolio.CHARGE_TIME)

    ######################## ------------------------------------------ ########################################
    def read_trdlist(self,strategy, handtrd = False):
        """ 读取标准格式的交易单子 ， 返回 买入 卖出两个方向的单子 """
        if handtrd:
            lst_dir = self._handlst_dir
            scanedlst = self._handlist
        else:
            lst_dir = self._trdlst_dir
            scanedlst = self._trdlist
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
            Portfolio.add_pool(self._holdings['T0'], self._pofname)
            self._noposition = False
            hastrd = True
            print('%s : trading by hand found : In' % self._pofname)
        if handlst['out']:
            self.update_holdlist(handlst['out'], 'T1')
            hastrd = True
            print('%s ：trading by hand found : Out' % self._pofname)
        if hastrd:  # 持仓更新成功，单子已经到达录入成功
            data_subscribe(SUBSCRIBE_SOURCE)
            time.sleep(Portfolio.CHARGE_TIME)
    ######################## ------------------------------------------ ########################################

    def read_holdlist(self):
        """ 读取标准格式的持仓单，该产品的所有标的均应包含
            单子格式 code,name,num,multi,prc  其中，code 应为根据所用数据源确定的带有前后缀的标的代码, num应该体现持仓方向（做空为负）
            读取的holdlist 为 pd.dataframe
            注意 ： 此处不涉及 T0 T1
        """
        if os.path.exists(self._holdlst_dir):
            holdlist = pd.read_csv(self._holdlst_dir,encoding='gb2312') #,names=['code','name','num','multi','prc'],header=1)
            #holdlist.index = holdlist['code'].tolist()
            return holdlist
        else:
            # 在当天没有持仓的情况下应避免调用此函数
            raise Exception('%s : No holdlist dir for %s' %(self._pofname))

    def update_holdings(self,lst,type):
        """ 只有在有交易的时候才调用此函数 更新holdlist , 对象初始化时不必调用 """
        # 做多数量为正，做空数量为负
        if lst is None:   # lst 为空
            return
        ts = 0  # 交易成本
        if type=='T1':     ####### 只在出场的时候更新T+1
            codes = lst['code']
            num = lst['num']
            multi = lst['multi']
            self._holdings['T1'].loc[codes,['num']] += num
            addsgl = (self._holdings['T1'].loc[codes,'prc']-lst['prc'])*num*multi  # 单个标的的收益，考虑到了lst的num包含了交易方向信息
            self._addvalue['fixed'] += np.sum( addsgl.values )
        elif type=='TO':    ######## 只在当日入场时更新 T+0
            if self._holdings['T0'].empty:
                self._holdings['T0'] = lst.loc[:,['code','name','num','multi','prc']]
            else:
                temp = self._holdings['T0'].append(lst.loc[:,['code','name','num','multi','prc']],ignore_index=True)
                temp['val'] = temp['num']*temp['prc']
                part1 = temp.groupby(['code'])['name','num','val'].sum()
                part2 = temp.groupby(['code'])['multi'].mean()
                grouped = pd.concat([part1,part2],axis=1) # type:pd.DataFrame
                grouped['code'] = grouped.index
                grouped['prc'] = grouped['val'].values / grouped['num'].values
                grouped.drop('val')
                self._holdings['T0'] = grouped
        else:
            raise Exception('Need to specify the type of holdlist to be updated!')
        ts += lst['tscost'].sum()
        self._addvalue['fixed'] += ts
    
    def update_addvalue(self):
        # 只计算尚未卖出的收益，即floated 收益
        # 当日卖出的部分属于 fixed 收益，在 update_holdlist 中计算
        if self._holdings['T0'].empty and self._holdings['T1'].empty:  # 调用时没有持仓
            return
        newinfo = pd.DataFrame( UNDL_POOL_INFO, index = POOL_COLUMNS).T
        addval = 0
        for tp in self._holdings:
            holding = self._holdings[tp]
            if holding.empty:
                continue
            else:
                holding_code = holding['code'].values
                #holding_code = holding['code'].tolist()
                lastprc = newinfo['rt_last'][holding_code].values
                addval += np.sum( ( lastprc - holding['prc'].values) * holding['num'].values * holding['multi'].values )
        self._addvalue['floated'] = addval

    def startplot(self):   # 如需画图，则将该产品 对象 添加到 Portfolio 类画图列表中
        added_plots = Portfolio.PLOT_OBJ
        if self._plotid not in added_plots:
            Portfolio.PLOT_NUM += 1
            self._plotid = Portfolio.PLOT_NUM
            Portfolio.PLOT_OBJ[self._plotid] = [self,True]
        else:
            Portfolio.PLOT_OBJ[self._plotid][1] = True

    def stopplot(self):
        if self._plotid in Portfolio.PLOT_OBJ:
            Portfolio.PLOT_OBJ[self._plotid][1] = False

    def update_object(self):
        """ 定时扫描交易状态 trdlist_dir handtrd_dir 并根据交易更新 holdlist，同时更新收益 """
        self._pofvalue = self.get_pofvalue()    # 更新总资产，应对当日转账情况
        #self.check_handtrd()    # 检查手动交易
        self.flush_trdstat()
        empty_position = np.all(list(self._noposition.values()))
        if not empty_position:  # 有持仓
            self.startplot()
        elif empty_position:
            self.stopplot()
        if UNDL_POOL_INFO:
            self.update_addvalue()


if __name__=='__main__':
    a=pd.DataFrame([['a',1,100],['a',2,100],['b',1,200],['b',1,200]],columns=['c','d','e'])
    print(a)
    print(a.groupby(by='c')['d','e'].sum())
    print(a.groupby(by='c')['e'].mean())
    t= pd.concat([a.groupby(by='c')['d'].sum(),a.groupby(by='c')['e'].mean()],axis=1)
    t['c']=t.index
    print(t)
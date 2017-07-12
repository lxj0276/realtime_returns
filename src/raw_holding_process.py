import datetime as dt
import os

import numpy as np
import pandas as pd
import sqlite3
from remotewind import w
from gmsdk import md

from date_math.date_math import *
from gm_daily.gm_daily import *
from global_vars import *
from database_assistant.database_assistant import *


class rawholding_stocks:

    @staticmethod
    def addfix(undl,source=SUBSCRIBE_SOURCE,endmark=''):
        if undl[0].isnumeric() and len(undl[0])<6:
            undl = ''.join(['0'*(6-len(undl)),undl])
        if 'goldmin' in source:
            if undl[0:2] in ('IF','IC','IH'):
                return '.'.join(['CFFEX',undl+endmark])
            elif undl[0] in ('0','3'):
                return '.'.join(['SZSE',undl+endmark])
            elif undl[0] in ('6'):
                return '.'.join(['SHSE',undl+endmark])
            elif undl[0] == '7':
                return 'NEWSTOCK'
            else:
                return undl
        elif source == 'wind':
            if undl[0:2] in ('IF','IC','IH'):
                return '.'.join([undl,'CFE'])
            elif undl[0] in ('0','3'):
                return '.'.join([undl,'SZ'])
            elif undl[0] in ('6'):
                return '.'.join([undl,'SH'])
            elif undl[0] == '7':
                return 'NEWSTOCK'
            else:
                return undl
        elif source == 'simulation':
            return '.'.join([undl,'SIM'])
        else:
            raise Exception('Wrong data source! ')

    def __init__(self,hold_dbdir,pofname):
        self._hold_dbdir = hold_dbdir
        self._pofname = pofname

    def get_holdname(self,inputdate = None):
        """ 根据日期时间确定持仓表格名称，如果没有给定的话(input=None)则提取当前,input应该是datetime 格式 """
        if inputdate is None:
            inputdate = dt.date.today().strftime('%Y%m%d')
        else:
            inputdate = inputdate.strftime('%Y%m%d')
        return '_'.join([self._pofname,'holding_stocks',inputdate])

    def holdlist_to_db(self,textvars,tabledir,date=None,tablename=None,codemark='证券代码',replace=True,currencymark='币种'):
        """ 将一张软件端导出的 持仓表格 更新至数据库
            textvars 存储数据库格式为TEXT的字段
            currencymark 用于识别汇总情况表头 目前只有通达信终端才有 若找不到就不建立表格
            codemark 用于标识正表表头
            tablename 表格存储在数据库中的名称
        """
        if date is None:
            date = dt.datetime.today()
        if not tablename:
            tablename = self.get_holdname(inputdate=date)
        with db_assistant(dbdir=self._hold_dbdir) as holddb:
            conn = holddb.connection
            c = conn.cursor()
            with open(tabledir,'r') as fl:
                rawline = fl.readline()
                startwrite = False
                summary = False
                newtb = False
                while rawline:
                    line = rawline.strip().split(',')
                    if not startwrite:
                        if currencymark: # 在找到详细数据之前会先查找汇总,如果不需要汇总则直接寻找标题
                            if not summary:    # 检查持仓汇总部分
                                if currencymark in line:  #寻找汇总标题
                                    stitles = line
                                    currpos = stitles.index(currencymark)
                                    stitlecheck = db_assistant.gen_table_titles(titles=stitles,varstypes={'TEXT':(currencymark,)})
                                    stitletrans = stitlecheck['typed_titles']
                                    stitle_empty = stitlecheck['empty_pos']
                                    stitlelen = len(stitletrans)
                                    holddb.create_db_table(tablename=tablename+'_summary',titles=stitletrans,replace=True)
                                    rawline = fl.readline()
                                    summary = True
                                    continue
                            else:
                                if line[currpos] == '人民币':   # 读取人民币对应的一行
                                    exeline = ''.join(['INSERT INTO ', tablename+'_summary', ' VALUES (', ','.join(['?']*stitlelen), ')'])
                                    newline = []
                                    for dumi in range(len(line)):
                                        if not stitle_empty[dumi]:
                                            newline.append(line[dumi])
                                    c.execute(exeline, newline)
                                    conn.commit()
                        #寻找正表标题
                        if codemark in line:
                            titles = line
                            titlecheck = db_assistant.gen_table_titles(titles=titles,varstypes={'TEXT':textvars})
                            titletrans = titlecheck['typed_titles']
                            # title_empty = titlecheck['empty_pos']   # 此处尤其暗藏风险，假设正表数据没有空列
                            newtb = holddb.create_db_table(tablename=tablename,titles=titletrans,replace=replace)
                            rawline = fl.readline()
                            startwrite = True
                            if not newtb:  # 表格已经存在就不必再写入
                                break
                            continue
                    elif startwrite and newtb:   # 在已找到正文并且表格是新建的情况下才开始写
                        exeline = ''.join(['INSERT INTO ', tablename, ' VALUES (', ','.join(['?']*len(line)), ')'])
                        c.execute(exeline, line)
                        conn.commit()
                    rawline = fl.readline()
            if startwrite and newtb: #实现写入并退出循环
                print('Table '+tablename+' updated to database !')
            elif startwrite and not newtb:
                print('Table '+tablename+' already in the database !')
            else:  # 未能实现写入
                print('Table '+tablename+' cannot read the main body, nothing writen !')

    def holdlist_format(self,titles,date=None,tablename=None,outdir=None):
        """ 从数据库提取画图所需格式的持仓信息，tablename 未提取的表格的名称
            存储为 DataFrame, 输出到 csv
            titles 应为包含 证券代码 证券名称 证券数量 最新价格 的列表
            需要返回字段 : code, name, num, multi, prc
        """
        if date is None:
            date = dt.datetime.today()
        if not tablename:
            tablename = self.get_holdname(inputdate=date)
        with db_assistant(self._hold_dbdir) as holddb:
            conn = holddb.connection
            exeline = ''.join(['SELECT ',','.join(titles),' FROM ',tablename])
            holdings = pd.read_sql(exeline,conn)
            holdings.columns = ['code','name','num','prc']  # 因此给的title只能有4个
            # 剔除非股票持仓和零持仓代码 逆回购 理财产品等
            holdings['code'] = holdings['code'].map(rawholding_stocks.addfix)
            holdings = holdings[~ holdings['code'].isin(HOLD_FILTER)]
            holdings = holdings[holdings['num']>0]
            holdings['multi'] = np.ones([len(holdings),1])
            #holdings['val'] = holdings['num']*holdings['prc']
            holdings = holdings.sort_values(by=['code'],ascending=[1])
            holdings = holdings.loc[:,['code','name','num','multi','prc']]
            if outdir:
                holdings.to_csv(outdir,header = True,index=False)
            else:
                return holdings

    def get_totvalue(self,titles,date=None,tablename=None,othersource=None):
        """ 提取 客户端软件 对应的总资产 """
        if date is None:
            date = dt.datetime.today()
        if not tablename:
            tablename = self.get_holdname(inputdate=date)
        if not titles[0]:    # 客户端持仓表格没有资产信息，需要从其他源（手填）提取
            with open(othersource,'r') as pof:
                totval = float(pof.readlines()[0].strip())
        else:
            with db_assistant(dbdir=self._hold_dbdir) as holddb:
                conn = holddb.connection
                exeline = ''.join(['SELECT ',','.join(titles),' FROM ',tablename+'_summary'])
                values = conn.execute(exeline).fetchall()
                totval = np.sum(values[0])
        return totval


class rawholding_futures:
    """
    期货持仓信息：所提取的信息应集中于一个期货账户，如果同一产品有其他账户，应在创建一个该类的对象；
    但可以有多个期货策略
    """
    @classmethod
    def get_3rd_friday(cls,date=None):
        """ 获得date日期对应的主力合约的交割日期 """
        if date is None:
            date = dt.datetime.today()
        thisyear = date.year
        thismonth = date.month
        firstday = dt.date(year=thisyear,month=thismonth,day=1)
        weekdate = firstday.weekday()
        if weekdate<=4:
            diff = 4-weekdate
        else:
            diff = 7+(4-weekdate)
        thirdfriday = firstday + dt.timedelta(days=diff+14)
        if date<thirdfriday:
            return thirdfriday
        else:
            if thismonth==12:
                nextmonth = 1
                nextyear = thisyear+1
            else:
                nextmonth = thismonth+1
                nextyear = thisyear
            return rawholding_futures.get_3rd_friday(date=dt.date(year=nextyear,month=nextmonth,day=1))

    @classmethod
    def get_contracts_real(cls,date=None,cttype = 'IC'):
        """ 返回 date  多对应日期当天的各合约,实际存在的合约,不是我们自定义的合约 """
        if date is None:
            date = dt.datetime.today()
        w.start()
        deliv = w.wss(''.join([cttype,date.strftime('%y%m'),'.CFE']),'lastdelivery_date').Data[0][0]
        season_mons = np.array([3,6,9,12])
        dmobj = date_math(date)
        if date.strftime('%Y%m%d')>deliv.strftime('%Y%m%d'):  # 已经换月
            near1 = dmobj.month_add(months=1)
            near2 = dmobj.month_add(months=2)
        else:  # 还未换月
            near1 = date
            near2 = dmobj.month_add(months=1)
        tempday = near2.day
        near2mon = near2.month
        near2yr = near2.year
        back1mon = season_mons[season_mons>near2mon][0]
        back1yr = near2yr + (near2mon==12)
        back2mon = back1mon+3-12*(back1mon==12)
        back2yr = back1yr + (back1mon==12)
        back1 = dt.datetime(year=back1yr,month=back1mon,day=tempday)
        back2 = dt.datetime(year=back2yr,month=back2mon,day=tempday)
        result = {'near1':''.join([cttype,near1.strftime('%y%m')]),
                  'near2':''.join([cttype,near2.strftime('%y%m')]),
                  'back1':''.join([cttype,back1.strftime('%y%m')]),
                  'back2':''.join([cttype,back2.strftime('%y%m')])}
        return result

    @classmethod
    def get_contracts_ours(cls,date=None,cttype = 'IC'):
        """ 返回我们当前使用的合约代码，第三个周四就换到下一个月，期间次月合约为None """
        if date is None:
            date = dt.datetime.today()
        real_contracts = rawholding_futures.get_contracts_real(date=date,cttype = cttype)
        w.start()
        near1_deliv = w.wss(''.join([real_contracts['near1'],'.CFE']),'lastdelivery_date').Data[0][0]
        timediff = w.tdayscount(date,near1_deliv).Data[0][0]
        if timediff<=2:
            real_contracts['near1'] = real_contracts['near2']
            real_contracts['near2'] = None
        return real_contracts

    def __init__(self,hold_dbdir,pofname,logdir,cwdir):
        self._hold_dbdir = hold_dbdir  # 暂未使用
        self._pofname = pofname
        self._logdir = logdir
        self._cwdir = cwdir
        self._multiplier = {'IF':300,'IC':200,'IH':200}

    def get_holdname(self,inputdate = None):
        """ 根据日期时间确定持仓表格名称，如果没有给定的话(input=None)则提取当前,input应该是datetime 格式 """
        if inputdate is None:
            inputdate = dt.datetime.today().strftime('%Y%m%d')
        return '_'.join([self._pofname,'holding_futures',inputdate])

    def get_holdnum(self,date=None):
        """ 提取期货持有手数，如果未指定日期则默认为当前持有，通过cwstat.txt，如果给定日期则需要在cwstate_history寻找 """
        holdnum = {}
        for strat in self._logdir:  # 不要遍历self._cwdir,可能包含只做多的策略
            nvoldir = os.path.join(self._cwdir[strat],'nVolume.txt')
            if (date is None) or (date.strftime('%Y%m%d')==dt.datetime.today().strftime('%Y%m%d')):
                cwdir = os.path.join(self._cwdir[strat],'cwstate.txt')
            else:
                cwdir = os.path.join(self._cwdir[strat],'cwstate_history',''.join(['cwstate_',date.strftime('%Y%m%d'),'.txt']))
            contents = []
            with open(cwdir,'r') as cwinfo:
                temp = cwinfo.readlines()
                contents_temp = [c.strip().split(',') for c in temp]
                [contents.append([float(c) for c in t]) for t in contents_temp if len(t)==6]
            contents = np.array(contents)
            with open(nvoldir) as nvol:
                temp = nvol.readlines()
                num = [float(c.strip()) for c in temp if c]
            holdnum[strat] = np.sum(contents[:,0])*num[0]   # contents[:,0] 已经包含了持仓方向信息
        return holdnum

    def get_totval(self,date=None,prctype = 'close',source='wind'):
        """ 提取期货账户总金额 ， 可能包含多个策略"""
        if date is None:
            date = dt.datetime.today()
        totvals = 0   # 用于记录各个策略所提取到的期货账户总资产金额，应采用最晚写的结果，同时过滤掉空结果
        maxtime = 0
        diffval = 0
        holdnum = self.get_holdnum(date=date)
        for strat in self._logdir:
            acclogdir = os.path.join(self._logdir[strat],'accountlog',''.join(['accountlog_',date.strftime('%Y%m%d'),'.txt']))
            if prctype=='settle':
                stratinfo = strat.split('_')
                cttype = stratinfo[1].upper()
                montype = stratinfo[0]
                contracts = rawholding_futures.get_contracts_ours(date=date,cttype=cttype)
                num = holdnum[strat]
                if source=='wind':
                    ######  wind data ########
                    ct = '.'.join([contracts[montype],'CFE'])
                    w.start()
                    data = w.wsd(ct,'settle,close',date,date).Data
                    diffval += (data[0][0]-data[1][0])*num*self._multiplier[cttype]
                elif source=='gm':
                    ##### 掘金 data ##########
                    ct = '.'.join(['CFFEX',contracts[montype]])
                    gm_obj = gm_daily('18201141877','Wqxl7309')
                    data = gm_obj.gmwsd(code=ct,valstr='settle_price,close',startdate=date,enddate=date)
                    diffval += (data.loc[0,'settle_price']-data.loc[0,'close'])*num*self._multiplier[cttype]
                ###################
                # diffval += (-16.6)*num*self._multiplier[cttype]   # 紧急措施 手动
            with open(acclogdir) as acclog:
                temp = acclog.readlines()
                contents_temp = [c.strip().split(',') for c in temp]
                contents = [float(c) for c in contents_temp[0]]
                if contents[1]>maxtime:
                    maxtime = contents[1]
                    totvals = contents[2]    # 将时间也包括上
        return totvals+diffval

    def holdlist_format(self,date=None,prctype='close',outdir=None,source='wind'):
        """ 提取标准格式, 与get_totval 平行，不会互相调用 """
        if date is None:
            date = dt.datetime.today()
        if source=='wind':
            #######  万得数据源 #######
            w.start()
            if date is None:
                date = dt.datetime.today()
        elif source=='gm':
            ######  掘金数据源 #######
            md.init('18201141877','Wqxl7309')
            if prctype=='settle':
                prctype = 'settle_price'   # 转为掘金格式
        holdnum = self.get_holdnum(date=date)
        holding = pd.DataFrame()
        for strat in self._logdir:
            stratinfo = strat.split('_')
            cttype = stratinfo[1].upper()
            montype = stratinfo[0]
            name = rawholding_futures.get_contracts_ours(date=date,cttype=cttype)[montype]
            code = rawholding_stocks.addfix(name)
            num = holdnum[strat]
            multi = self._multiplier[cttype]
            if source=='wind':
                ############# wind 数据源
                prc = w.wsd('.'.join([name,'CFE']),prctype,date,date).Data[0][0]
            elif source=='gm':
                ######### 掘金数据
                lastbar = md.get_last_n_dailybars(symbol='.'.join(['CFFEX',name]),n=1,end_time=date.strftime('%Y-%m-%d'))[0]
                prc = eval('.'.join(['lastbar',prctype]))
            #### 紧急措施 手动
            # prc = 6069.2 if name=='IC1707' else 5880.6
            holdlist = pd.DataFrame([[code,name,num,multi,prc]],columns=['code','name','num','multi','prc'])
            #holdlist['val'] = holdlist['num']*holdlist['multi']*holdlist['prc']
            holding = holding.append(holdlist,ignore_index=True)
        holding = holding[holding['num']!=0]
        if outdir:
            holding.to_csv(outdir,header = True,index=False)
        else:
            return holding
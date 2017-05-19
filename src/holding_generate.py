import datetime as dt
import os
import re

from dateutil import parser
import numpy as np
import pandas as pd
import sqlite3

from global_vars import *




def undl_backfix(undl):
    """ 为标的undl 增加后缀，目前采用万得后缀标准 undl 应为字符串"""
    if undl[0:2] in ('IF','IC','IH'):
        return '.'.join([undl,'CFE'])
    undlsize = len(undl)
    if undlsize<6:
        undl = ''.join(['0'*(6-undlsize),undl])
    if undl[0] in ('0','3'):
        return '.'.join([undl,'.SZ'])
    elif undl[0] in ('6'):
        return '.'.join([undl,'.SH'])
    else:
        return undl


class DatabaseConnect:
    def __init__(self,dbdir):
        self.dbdir=dbdir

    def __enter__(self):
        self.connection=sqlite3.connect(self.dbdir)
        return self.connection

    def __exit__(self,exc_type,exc_instantce,traceback):
        self.connection.close()
        return False  # pop up errors


class ClientToDatabase:
    """ 用于将交易端导出的数据存储进相应的数据库(包含多张表) """

    @classmethod
    def get_datetime(cls,inputdate = None):
        """ 获取日期与时间，如果没有给定的话(input=None)则提取当前,input应该是datetime 格式 """
        if inputdate is None:
            now = dt.datetime.now()
        else:
            now = inputdate
        if now.time()>dt.time(12,0,0,0):
            mark = '_afternoon'
        else:
            mark = '_morning'
        return now.date().strftime('%Y%m%d')+mark

    @classmethod  # 可以升级为模块方法与其他任务共享
    def gen_table_titles(cls,titles,varstypes,defaluttype = 'REAL'):
        """ 为采取 通过直接读取生成数据库表格标题 的任务创建带有数据类型的标题 同时识别空标题
            titles 为包含已经识别出作为标题的数据的列表
            vartypes 应给为可能包含的数据类型的字典
        """
        typed_titles = []
        empty_pos = []
        for tvar in titles:
            typed = False
            for tp in varstypes:
                if tvar in varstypes[tp]:
                    typed_titles.append(''.join([tvar.strip('(%)'),' ',tp]))
                    empty_pos.append(False)
                    typed = True
                    break
                elif tvar == '':
                    empty_pos.append(True)
                    typed = True
                    break
            if not typed:
                typed_titles.append(''.join([tvar.strip('(%)'),' ',defaluttype]))
                empty_pos.append(False)
        return {'typed_titles':typed_titles,'empty_pos':empty_pos}

    @classmethod # 可以升级为模块方法与其他任务共享
    def create_db_table(cls,cursor,tablename,titles,replace=True):
        """ 通过数据库已建立的 cursor object 创建数据库表格
            title 为标题 列表
            如果replace为真，则会替换已存在的同名表格
        """
        exeline = ''.join(['CREATE TABLE ',tablename,' (',','.join(titles),') '])
        try:
            cursor.execute(exeline)
        except sqlite3.OperationalError as e:
            if 'already exists' in str(e):
                if replace:    # 如果替换的话，先删除已存在表格再创建
                    cursor.execute(''.join(['DROP TABLE ',tablename]))
                    cursor.execute(exeline)
                    print('Table '+tablename+' created!')
                    return True
                else:
                    print('Table '+tablename+' already exists!')
                    return False
            else:
                raise e
        else:
            print('Table '+tablename+' created!')


    def __init__(self,hold_dbdir,trd_dbdir,pofname):
        self._hold_dbdir = hold_dbdir
        self._trd_dbdir = trd_dbdir
        self._pofname = pofname
        self.holdtbname = None
        self.setholdtbname()
        self.table_output_count = {}

    def setholdtbname(self,inputdate = None):
        """ 根据日期时间确定持仓表格名称，如果没有给定的话(input=None)则提取当前,input应该是datetime 格式 """
        self.holdtbname = self._pofname + '_' + self.get_datetime(inputdate)

    def holdlist_to_db(self,tabledir,textvars,tablename=None,codemark='证券代码',replace=True,currencymark='币种'):
        """ 将一张软件端导出的 持仓表格 更新至数据库
            textvars 存储数据库格式为TEXT的字段
            currencymark 用于识别汇总情况表头 目前只有通达信终端才有 若找不到就不建立表格
            codemark 用于标识正表表头
            tablename 表格存储在数据库中的名称
        """
        if not tablename:
            tablename = self.holdtbname
        with DatabaseConnect(self._hold_dbdir) as conn:
            c = conn.cursor()
            with open(tabledir,'r') as fl:
                rawline = fl.readline()
                startwrite = False
                summary = False
                while rawline:
                    line = rawline.strip().split(',')
                    if not startwrite:
                        if currencymark: # 在找到详细数据之前会先查找汇总,如果不需要汇总则直接寻找标题
                            if not summary:    # 检查持仓汇总部分
                                if currencymark in line:  #寻找汇总标题
                                    stitles = line
                                    currpos = stitles.index(currencymark)
                                    stitlecheck = ClientToDatabase.gen_table_titles(stitles,{'TEXT':(currencymark,)})
                                    stitletrans = stitlecheck['typed_titles']
                                    stitle_empty = stitlecheck['empty_pos']
                                    ClientToDatabase.create_db_table(c,tablename+'_summary',stitletrans,replace)
                                    rawline = fl.readline()
                                    summary = True
                                    continue
                            else:
                                if line[currpos] == '人民币':   # 读取人民币对应的一行
                                    exeline = ''.join(['INSERT INTO ', tablename, '_summary VALUES (', ','.join(['?']*len(stitletrans)), ')'])
                                    newline = []
                                    for dumi in range(len(line)):
                                        if not stitle_empty[dumi]:
                                            newline.append(line[dumi])
                                    c.execute(exeline, newline)
                                    conn.commit()
                        #寻找正表标题
                        if codemark in line:
                            titles = line
                            codepos = titles.index(codemark)
                            titlecheck = ClientToDatabase.gen_table_titles(titles,{'TEXT':textvars})
                            titletrans = titlecheck['typed_titles']
                            # title_empty = titlecheck['empty_pos']   # 此处尤其暗藏风险，假设正表数据没有空列
                            ClientToDatabase.create_db_table(c,tablename,titletrans,replace)
                            rawline = fl.readline()
                            startwrite = True
                            continue
                    else:
                        exeline = ''.join(['INSERT INTO ', tablename, ' VALUES (', ','.join(['?']*len(line)), ')'])
                        line[codepos] = undl_backfix(line[codepos])
                        c.execute(exeline, line)
                        conn.commit()
                    rawline = fl.readline()
            if startwrite: #实现写入并退出循环
                print('Table '+tablename+' updated to database !')
            else:  # 未能实现写入
                print('Table '+tablename+' cannot read the main body, nothing writen !')

    def holdlist_format(self,titles,tablename=None,outdir=None):
        """ 从数据库提取画图所需格式的持仓信息，tablename 未提取的表格的名称
            存储为 DataFrame, 输出到 csv
            titles 应为包含 证券代码 证券名称 证券数量 最新价格 的列表
            需要返回字段 : code, name, num, prc,val
        """
        # 逆回购 理财产品等
        if not tablename:
            tablename = self.holdtbname
        with DatabaseConnect(self._hold_dbdir) as conn:
            exeline = ''.join(['SELECT ',','.join(titles),' FROM ',tablename])
            holdings = pd.read_sql(exeline,conn)
            holdings.columns = ['code','name','num','prc']
            # 剔除非股票持仓和零持仓代码
            holdings = holdings[~ holdings['code'].isin(HOLD_FILTER)]
            holdings = holdings[holdings['num']>0]
            holdings['val'] = holdings['num']*holdings['prc']
            holdings = holdings.sort_values(by=['code'],ascending=[1])
            if outdir:
                holdings.to_csv(outdir,header = True,index=False)
            else:
                return holdings

    def get_totvalue(self,titles,tablename,othersource):
        """ 提取 客户端软件 对应的总资产 """
        if not titles:    # 客户端持仓表格没有资产信息，需要从其他源（手填）提取
            with open(othersource,'r') as pof:
                totval = float(pof.readlines()[0].strip())
        else:
            with DatabaseConnect(self._hold_dbdir) as conn:
                exeline = ''.join(['SELECT ',','.join(titles),' FROM ',tablename+'_summary'])
                values = conn.execute(exeline).fetchall()
                totval = np.sum(values[0])
        return totval

    def trdlist_to_db(self,textvars,tabledir,tablename=None,codemark='证券代码',replace=False):
        """ 将一张软件端导出的 交易/委托表格 更新至数据库
            textvars 存储数据库格式为TEXT的字段
            codemark 用于标识正表表头
            tablename 表格存储在数据库中的名称
        """
        if not tablename:
            rec_time = dt.datetime.now().strftime('%Y%m%d')
            tablename = ''.join([self._pofname,'_trading_',rec_time])
        with DatabaseConnect(self._trd_dbdir) as conn:
            c = conn.cursor()
            with open(tabledir,'r') as fl:
                rawline = fl.readline()
                foundtitle = False
                linecount = 0
                processed_lines = 0
                while rawline:
                    line = rawline.strip().split(',')
                    if not foundtitle:     # 寻找到正文开始标题后才开始写入
                        if codemark in line:  #寻找正表标题
                            titles = line
                            codepos = titles.index(codemark) + 1
                            titlecheck = ClientToDatabase.gen_table_titles(titles,{'TEXT':textvars})
                            titletrans = ['row_id INTEGER']+titlecheck['typed_titles']
                            # title_empty = titlecheck['empty_pos']   # 此处尤其暗藏风险，假设正表数据没有空列
                            newcreated = ClientToDatabase.create_db_table(c,tablename,titletrans,replace)
                            if not newcreated:
                                processed_lines = c.execute(''.join(['SELECT COUNT(',codemark,') FROM ',tablename])).fetchall()[0][0]
                            else:
                                self.table_output_count[tablename] = 0    # 如果表格是新创建的话，需要初始化输出行数记录
                            rawline = fl.readline()
                            foundtitle = True
                            continue
                    else:  # 已经找到了标题
                        linecount += 1
                        if linecount>processed_lines:
                            line = [linecount]+line
                            exeline = ''.join(['INSERT INTO ', tablename, ' VALUES (', ','.join(['?']*len(line)), ')'])
                            line[codepos] = undl_backfix(line[codepos])   # 增加 .SZ ,.SH 等证券后缀
                            c.execute(exeline, line)
                            conn.commit()
                    rawline = fl.readline()
                print('%d lines updated to trading recorder database' %(linecount-processed_lines,))

    def trdlist_format(self,titles,tablename,tscostrate,multiplyer=None,outdir=None):
        """ 从数据库提取画图所需格式的 交易 信息，tablename 未提取的表格的名称
            存储为 DataFrame, 输出到 csv
            titles 应为包含 的列表
            需要返回字段 : code, name, num, prc,val,tscost,inout
            做多 数量为正、金额为正， 做空为负、金额为负， 价格恒正, 交易成本恒为负
        """
        tofilter = ['131810','204001']
        def marktrans(x):
            if '买' in x:
                return 'in'
            elif '卖' in x:
                return 'out'
        def reverseval(x):
            if x=='out':
                return -1
            else:
                return 1
        def tsratecalc(x):
            if x=='out':
                return tscostrate['out']  # 卖出会有印花税
            else:
                return -tscostrate['in']
        def mulpcalc(x):
            mulp = multiplyer.get(x)
            if not mulp:
                return 1
            else:
                return mulp
        with DatabaseConnect(self._trd_dbdir) as conn:
            exeline = ''.join(['SELECT ',','.join(titles),' FROM ',tablename,' WHERE row_id >',str(self.table_output_count[tablename])])
            trades = pd.read_sql(exeline,conn)
            trades.columns = ['code','name','num','prc','inout']
            # 剔除非股票持仓和零持仓代码
            trades = trades[~ trades['code'].isin(tofilter)]
            trades = trades[trades['num']>0]
            trades['inout'] = trades['inout'].map(marktrans)
            trades['num'] = trades['num']*trades['inout'].map(reverseval)*trades['code'].map(mulpcalc)
            trades['val'] = trades['num']*trades['prc']
            trades['tscost'] = trades['val']*trades['inout'].map(tsratecalc)
            trades = trades.sort_values(by=['code'],ascending=[1])
            trades = trades.ix[:,['code','name','num','prc','val','tscost','inout']]
            self.table_output_count[tablename] += trades.shape[0]
            if outdir:
                trades.to_csv(outdir,header = True,index=False)
            else:
                return trades

def futures_holding(date,account_info,outputfmt='for_plot'):
    """ 生成期货持仓，并计算总资产
        生成表格结构: code name num prc val
        account_info 结构 ：
            {'init_cash': 期货端总资产 ,
             'contract1': {'settle':结算价， 'trdside':'Long'/'Short' ,'multiplier' , 'holdnum':[]},
             'contract2': ...}
     """
    margin = 0.3
    output = pd.DataFrame()
    if outputfmt == 'for_plot':
        totval = account_info['tot_value']
        for ct in account_info:
            if ct != 'tot_value':
                code = ct
                name = ct.split('.')[0]
                numcom = account_info[ct]['holdnum']*account_info[ct]['multiplier']*account_info[ct]['trdside']
                num = np.sum(numcom)
                prc = account_info[ct]['settle']
                val = num*prc
                ctinfo = pd.DataFrame([[code,name,num,prc,val]],columns=['code','name','num','prc','val'])
                output = output.append(ctinfo ,ignore_index=True)
        return {'table':output,'totval':totval}
    elif outputfmt == 'for_calcdiv':
        cashamt = account_info['tot_value']
        for ct in account_info:
            if ct != 'tot_value':
                deposit = np.sum(account_info[ct]['holdnum']*account_info[ct]['settle'])*account_info[ct]['multiplier']*(margin+0.1)
                code = ct
                numcom =account_info[ct]['holdnum']*account_info[ct]['trdside']
                num = np.sum(numcom)
                prc = account_info[ct]['settle']
                ctinfo = pd.DataFrame([[date,code,num,0,0,prc]],columns=['date','stkcd','num','cash','share','prc'])
                output = output.append(ctinfo ,ignore_index=True)
                cashamt -= deposit
        cashinfo = pd.DataFrame([[date,'999157',cashamt,0,0,1]],columns=['date','stkcd','num','cash','share','prc'])
        output = output.append(cashinfo ,ignore_index=True)
        return {'table':output,'cashinfo':cashinfo}
    else:
        raise Exception('No output format provided ')


if __name__ == '__main__':
    objtemp = ClientToDatabase(r'C:\Users\Jiapeng\Desktop\test.db','test')
    textvars=['成交编号','成交类型','成交时间','成交状态','股东代码','买卖','申请编号','委托编号','委托类型','业务名称','证券代码','证券名称']
    tabledir=r'C:\Users\Jiapeng\Desktop\bq1_trading.csv'
    #objtemp.trdlist_to_db(textvars,tabledir,tablename=None,codemark='证券代码',replace=False)
    titles = ['证券代码','证券名称','成交数量','成交价格','买卖']
    #tb = objtemp.trdlist_format(titles,outdir='',tablename=r'test_trading_20170519',tscostrate={'in':2/10000,'out':12/10000},multiplyer={'603568.SH':100})
    #tb.to_csv(r'C:\Users\Jiapeng\Desktop\bb.csv')
    textvars2=['成交编号','合约','买卖','开平','成交时间','报单编号','成交类型','投保','交易所']
    tabledir2=r'C:\Users\Jiapeng\Desktop\成交记录_170518.csv'
    objtemp.trdlist_to_db(textvars2,tabledir2,tablename='futures_trading',codemark='合约',replace=True)
    titles2=['合约','合约','成交手数','成交价格','买卖']
    tb2 = objtemp.trdlist_format(titles2,outdir='',tablename=r'futures_trading',tscostrate={'in':2/10000,'out':12/10000},multiplyer={'IC1706':100})
    print(tb2)

    objtemp.holdlist_to_db(tabledir=r'C:\Users\Jiapeng\Desktop\持仓_170519.csv',
                           textvars=['合约','买卖','投保','交易所'],
                           tablename='futures_holding',
                           codemark='合约',replace=True,currencymark=None)
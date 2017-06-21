import datetime as dt
import os
import re

import numpy as np
import pandas as pd
import sqlite3

#from global_vars import *
from database_assistant.database_assistant import *


class rawtrading_stocks:
    @classmethod
    def undl_backfix(cls,undl):
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

    def __init__(self,pofname,trd_dbdir):
        self._pofname = pofname
        self._trd_dbdir = trd_dbdir
        self.table_output_count = {}

    def get_trdname(self,inputdate = None):
        """ 根据日期时间确定持仓表格名称，如果没有给定的话(input=None)则提取当前,input应该是datetime 格式 """
        if inputdate is None:
            inputdate = dt.date.today().strftime('%Y%m%d')
        return '_'.join([self._pofname,'trading_stocks',inputdate])

    def trdlist_to_db(self,textvars,tabledir,tablename=None,codemark='证券代码',replace=False):
        """ 将一张软件端导出的 交易/委托表格 更新至数据库
            textvars 存储数据库格式为TEXT的字段
            codemark 用于标识正表表头
            tablename 表格存储在数据库中的名称
            已经写入数据库的行将不再写入,需要确定数据表格按交易时间排序
        """
        if not tablename:
            tablename = self.get_trdname()
        with db_assistant(dbdir=self._trd_dbdir) as trddb:
            conn = trddb.connection
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
                            titlecheck = db_assistant.gen_table_titles(titles=titles,varstypes={'TEXT':textvars})
                            titletrans = ['row_id INTEGER']+titlecheck['typed_titles']
                            titlelen = len(titletrans)
                            # title_empty = titlecheck['empty_pos']   # 此处尤其暗藏风险，假设正表数据没有空列
                            newcreated = trddb.create_db_table(tablename=tablename,titles=titletrans,replace=replace)
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
                            exeline = ''.join(['INSERT INTO ', tablename, ' VALUES (', ','.join(['?']*titlelen), ')'])
                            line[codepos] = rawtrading_stocks.undl_backfix(line[codepos])   # 增加 .SZ ,.SH 等证券后缀
                            c.execute(exeline, line)
                            conn.commit()
                    rawline = fl.readline()
                print('%d lines updated to trddb with table %s' %(linecount-processed_lines,tablename))

    def trdlist_format(self,titles,tscostrate,tablename=None,outdir=None):
        """ 从数据库提取画图所需格式的 交易 信息，tablename 未提取的表格的名称
            存储为 DataFrame, 输出到 csv
            titles 应为包含 的列表
            需要返回字段 : code, name, num, prc,val,tscost,inout
            做多 数量为正、金额为正， 做空为负、金额为负， 价格恒正, 交易成本恒为负
        """
        tofilter = ['131810','204001']
        if not tablename:
            tablename = self.get_trdname()
        def marktrans(x):
            if '买' in x:
                return 'in'
            elif '卖' in x:
                return 'out'
            else:
                raise Exception('Error in trdlist_format')
        def reverseval(x):
            if x=='out':
                return -1
            elif x=='in':
                return 1
            else:
                raise Exception('Error in trdlist_format')
        def tsratecalc(x):
            if x=='out':
                return tscostrate['out']  # 卖出会有印花税
            else:
                return -tscostrate['in']
        with db_assistant(dbdir=self._trd_dbdir) as trddb:
            conn = trddb.connection
            exeline = ''.join(['SELECT ',','.join(titles),' FROM ',tablename,' WHERE row_id >',str(self.table_output_count[tablename])])
            trades = pd.read_sql(exeline,conn)
            trades.columns = ['code','name','num','prc','inout']
            # 剔除非股票持仓和零持仓代码
            trades = trades[~ trades['code'].isin(tofilter)]
            trades = trades[trades['num']>0]
            trades['inout'] = trades['inout'].map(marktrans)
            trades['num'] = trades['num']*trades['inout'].map(reverseval)*trades['code']
            trades['val'] = trades['num']*trades['prc']
            trades['tscost'] = trades['val']*trades['inout'].map(tsratecalc)
            trades = trades.sort_values(by=['code'],ascending=[1])
            trades = trades.ix[:,['code','name','num','prc','val','tscost','inout']]
            self.table_output_count[tablename] += trades.shape[0]
            if outdir:
                trades.to_csv(outdir,header = True,index=False)
            else:
                return trades


class rawtrading_futures:
    def __init__(self,pofname,trd_dbdir,logdir,cwdir):
        self._pofname = pofname
        self._trd_dbdir = trd_dbdir
        self._logdir = logdir
        self._nvoldir = os.path.join(cwdir,'nVolume.txt')
        self.table_output_count = {}

    def get_trdname(self,inputdate = None):
        """ 根据日期时间确定持仓表格名称，如果没有给定的话(input=None)则提取当前,input应该是datetime 格式 """
        if inputdate is None:
            inputdate = dt.date.today().strftime('%Y%m%d')
        return '_'.join([self._pofname,'trading_futures',inputdate])

    def trdlog_to_db(self,inputdate = None):
        """
        从交易记录读取信息并生成pd.dataframe,
        写入数据库，因为数据比较少（100当才100行），因为每次都全部写入
        """
        if inputdate is None:
            inputdate = dt.date.today().strftime('%Y%m%d')
        logdir = os.path.join(self._logdir,''.join(['tradelog_',inputdate,'.txt']))
        with open(logdir) as fl:
            line = fl.readline()
            trdlogs = []
            names = ['date','time','action']
            namefull = False
            while line:
                vars = line.strip().split()
                trdlog = vars[:3]
                trdlog += [v.split('=')[1] for v in vars[3:]]
                if not namefull:
                    names += [v.split('=')[0] for v in vars[3:]]
                    namefull = True
                trdlogs.append(trdlog)
                line = fl.readline()
        fulllog = pd.DataFrame(trdlogs,columns=names)
        tablename = self.get_trdname()
        with db_assistant(dbdir=self._trd_dbdir) as trddb:
            conn = trddb.connection
            fulllog.to_sql(name=tablename,con=conn)

    def trdlist_format(self,titles,tscostrate,tablename=None,outdir=None):
        if not tablename:
            tablename = self.get_trdname()
        def marktrans(x):
            if '开仓' in x:
                return 'in'
            elif '平仓' in x:
                return 'out'
            else:
                raise Exception('Error in trdlist_format')
        def reverseval(x):
            if x=='out':
                return -1
            elif x=='in':
                return 1
            else:
                raise Exception('Error in trdlist_format')

        with db_assistant(dbdir=self._trd_dbdir) as trddb:
            conn = trddb.connection
            exeline = ''.join(['SELECT ',','.join(titles),' FROM ',tablename,' WHERE row_id >',str(self.table_output_count[tablename])])
            trades = pd.read_sql(exeline,conn)
            trades.columns = ['code','name','num','prc','inout']
            # 剔除非股票持仓和零持仓代码
            trades = trades[trades['num']>0]
            trades['inout'] = trades['inout'].map(marktrans)
            trades['num'] = trades['num']*trades['inout'].map(reverseval)*trades['code']
            trades['val'] = trades['num']*trades['prc']
            trades['tscost'] = trades['val']*trades['inout'].map(tsratecalc)
            trades = trades.sort_values(by=['code'],ascending=[1])
            trades = trades.ix[:,['code','name','num','prc','val','tscost','inout']]
            self.table_output_count[tablename] += trades.shape[0]
            if outdir:
                trades.to_csv(outdir,header = True,index=False)
            else:
                return trades



if __name__=='__main__':
    import configparser as cp
    cf = cp.ConfigParser()
    cf.read(r'E:\realtime_monitors\realtime_returns\configures\BaiQuanHuiJin1.ini')

    print(cf.sections())
    print(cf.options('stocks'))
    print(cf.items('stocks'))
    print(cf.get('stocks','text_vars_hold'))
    print(cf.get('stocks','text_vars_hold').split(','))
    print(cf.options('cwstate'))

import datetime as dt
import os
import re

import numpy as np
import pandas as pd
import sqlite3

from global_vars import *




def undl_backfix(undl):
    """ 为标的undl 增加后缀，目前采用万得后缀标准 undl 应为字符串"""
    if undl[0] in ('0','3'):
        return undl + '.SZ'
    elif undl[0] in ('6'):
        return undl + '.SH'
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
            for tp in varstypes:
                if tvar in varstypes[tp]:
                    typed_titles.append(tvar.strip('(%)')+' '+tp)
                    empty_pos.append(False)
                elif tvar == '':
                    empty_pos.append(True)
                else:
                    typed_titles.append(tvar.strip('(%)')+' '+defaluttype)
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
                else:
                    print('Table '+tablename+' already exists!')
            else:
                raise e
        else:
            print('Table '+tablename+' created!')


    def __init__(self,dbdir,pofname):
        self._dbdir = dbdir
        self._pofname = pofname
        self.holdtbname = None
        self.setholdtbname()

    def setholdtbname(self,inputdate = None):
        """ 根据日期时间确定持仓表格名称，如果没有给定的话(input=None)则提取当前,input应该是datetime 格式 """
        self.holdtbname = self._pofname + '_' + self.get_datetime(inputdate)

    def holdlist_to_db(self,tabledir,textvars,currencymark='币种',codemark='证券代码',replace=True,tablename=None):
        """ 将 一张 持仓表格 更新至数据库
            textvars 存储数据库格式为TEXT的字段
            currencymark 用于识别汇总情况表头 目前只有通达信终端才有 若找不到就不建立表格
            codemark 用于标识正表表头
        """
        if not tablename:
            tablename = self.holdtbname
        with DatabaseConnect(self._dbdir) as conn:
            c = conn.cursor()
            with open(tabledir,'r') as fl:
                rawline = fl.readline()
                startwrite = False
                summary = False
                while rawline:
                    line = rawline.strip().split(',')
                    if not startwrite:     # 在找到详细数据之前会先查找汇总
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

    def holdlist_format(self,titles,outdir,tablename=None):
        """ 从数据库提取画图所需格式的持仓信息，存储为 DataFrame, 输出到 csv
            titles 应为包含 证券代码 证券名称 证券数量 最新价格 的列表
            需要返回字段 : code, name, num, prc,val
        """
        # 逆回购 理财产品等
        refound_sz = ['131810','131811','131800','131809','131801','131802','131803','131805','131806']
        refound_sh = ['204001','204007','204002','204003','204004','204014','204028','204091','204182']
        other_vars = ['131990','888880','SHRQ88','SHXGED','SZRQ88','SZXGED']
        tofilter = refound_sz+refound_sh+other_vars
        if not tablename:
            tablename = self.holdtbname
        with DatabaseConnect(self._dbdir) as conn:
            exeline = ''.join(['SELECT ',','.join(titles),' FROM ',tablename])
            holdings = pd.read_sql(exeline,conn)
            holdings.columns = ['code','name','num','prc']
            # 剔除非股票持仓和零持仓代码
            holdings = holdings[~ holdings['code'].isin(tofilter)]
            holdings = holdings[holdings['num']>0]
            holdings['val'] = holdings['num']*holdings['prc']
            holdings = holdings.sort_values(by=['code'],ascending=[1])
            holdings.to_csv(outdir,header = True,index=False)

    def trdlist_to_db(self):
        pass

    def trdlist_format(self):
        # 做多 数量为正、金额为正， 做空为负、金额为负， 价格恒正, 交易成本恒为负
        # 返回项 : windcode, name, num, prc,val,transaction_cost, inout
        pass


def futures_gen(date,account_info,outputfmt='for_plot'):
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
    for t in FUTURES_INFO:
        tt = futures_gen(TODAY,FUTURES_INFO[t],outputfmt='for_plot')
        print(tt['table'])
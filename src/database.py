
import os
import re
import sqlite3
import datetime as dt
import pandas as pd
import numpy as np



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
    def __init__(self,dbdir,pofname):
        self._dbdir = dbdir
        self._pofname = pofname
        self._holdtbname = None
        self.set_holdtbname()

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

    def set_holdtbname(self,inputdate = None):
        self._holdtbname = self._pofname + '_' + self.get_datetime(inputdate)

    def holdlist_to_db(self,tabledir,textvars,currencymark='币种',codemark='证券代码',replace=True):
        """ 将 一张 持仓表格 更新至数据库 """
        tablename = self._holdtbname
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
            print('Table '+tablename+' updated to database !')

    def trdlist_to_db(self):
        pass




if __name__ == '__main__':
    basepath = r'E:\calc_dividend\holding_gen'
    raw = 'rawholding'
    file = 'holdingfiles'
    newfile = 'newfiles'
    db = 'holdingdb'
    dividend = 'dividendfiles'

    date = str(dt.datetime.strftime(dt.date.today(),'%Y%m%d'))

    products =['bq1'] # ['bq1','bq2','jq1','hj1','gd2','ls1']
    textvars = {'bq1': ('备注','股东代码','证券代码','证券名称','资金帐号'),
                'bq2': ('股东代码','证券代码','证券名称'),
                'jq1': ('股东代码','证券代码','证券名称'),
                'hj1': ('股东代码','证券代码','证券名称'),
                'gd2': ('股东代码','证券代码','证券名称'),
                'ls1': ('产品名称','到期日','股东账号','账号名称','证券代码','证券名称','状态','资金账号')
                }
    divfile = os.path.join(basepath,dividend,'dividend_'+date+'.txt')

    for p in products:
        obj = ClientToDatabase(r'E:\test.db',p)
        obj.holdlist_to_db(r'E:\calc_dividend\holding_gen\rawholding'+'\\'+p+'\\'+p+'_20170502.csv',textvars[p])


    # 读取 raw 至数据库
    # for p in products:
    #     rawfile = os.path.join(basepath,raw,p,p+'_'+date+'.csv')
    #     rawdb = os.path.join(basepath,db,p+'.db')
    #     outdir = os.path.join(basepath,file,p)
    #     filedir = os.path.join(outdir ,p +'_'+date+'.csv')
    #     newdir = os.path.join(basepath,newfile,p, p+'_'+date+'.csv')
    #     if os.path.exists(rawfile):
    #         obj = ClientToDatabase(rawdb,p)
    #         obj.UpdateDatabase(rawfile,textvars[p])





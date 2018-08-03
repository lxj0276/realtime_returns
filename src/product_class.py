
import configparser as cp
import datetime as dt
import os

import pandas as pd

import src.global_vars as gv
import database_assistant.DatabaseAssistant as da
from src.portfolio_class import Portfolio


class Products(Portfolio):
    """ 继承portfolio得类，将Portfolio类连接上数据库，并不是所有产品都会需要数据库 """
    def __init__(self,pofname,configdir,date=None,prctype='settle'):
        self._pofname = pofname
        # 读取文件路径配置
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(configdir,'realtime_returns_directories.ini'))
        # 读取产品配置
        product_cf = cp.ConfigParser()
        product_cf.read(os.path.join(configdir,'.'.join([pofname,'ini'])))
        ######### 需要写入的文件路径 ############
        self._pofval_dir = os.path.join(cfp.get('dirs','pofval'),pofname,'_'.join([pofname,'pofvalue',gv.TODAY+'.txt']))
        self._holdlst_dir = os.path.join(cfp.get('dirs','list_holding'),pofname,'_'.join([pofname,'positions',gv.TODAY+'.csv']))

        self._posi_db = os.path.join( cfp.get('dirs','positions_db'), '_'.join([pofname,'standard','tables.db']) )
        self._blog = product_cf.options('blog')
        self.list_pofval_gen(date=date,prctype=prctype)
        # ########################### 提取当前类所需的路径文件 只需要持仓相关 #########################
        # self._rawhold_dir = os.path.join(cfp.get('dirs','raw_holding'),pofname)           # 存储从软件端导出的持仓记录的文件夹
        # self._holddb_dir = os.path.join(cfp.get('dirs','products_db'),pofname,'_'.join([pofname,'holding.db']))     # 存储每日持仓记录的数据库
        # ########################### 提取父类初始化所需的各种路径 #########################
        # cwstatus_dirs = dict(product_cf.items('cwstate'))
        # # 将原始数据(通达信导出，ctp记录等)导入数据库，并声称portfolio_class 使用的标准持仓格式 (期货、股票以及其他标的（如果有）在一张表格中)
        # pofvalue = []
        # holdings = pd.DataFrame()
        # ######################### 读取 股票 持仓信息   #########################
        # print('%s : updating stocks holding...' % pofname)
        # rawfile = os.path.join(self._rawhold_dir,''.join([pofname,'_positions_stocks_',gv.TODAY,'.csv']))
        # obj = RawHoldingStocks(hold_dbdir=self._holddb_dir,pofname=pofname)   # 创建客户端转数据库对象
        # obj.holdlist_to_db(tabledir=rawfile,textvars=product_cf.get('stocks','text_vars_hold').split(','),replace=False)  # 写入数据库
        # holding = obj.holdlist_format(titles=product_cf.get('stocks','vars_hold').split(','))   # 从数据库提取标准格式
        # holdings = holdings.append(holding,ignore_index=True)
        # holdval = obj.get_totvalue(titles=product_cf.get('stocks','vars_value').split(','),othersource=os.path.join(cfp.get('dirs','other'),pofname+'.txt'))
        # pofvalue.append(holdval)
        # ######################### 读取 期货 持仓信息   #########################
        # if product_cf.options('blog'):
        #     prctype = 'settle'
        #     print('%s : updating futures holding...' % pofname)
        #     obj = RawHoldingFutures(hold_dbdir=self._holddb_dir,pofname=pofname,logdir=dict(product_cf.items('blog')),cwdir=cwstatus_dirs)
        #     holding = obj.holdlist_format(prctype=prctype,date=gv.Yesterday,source='wind')
        #     holdings = holdings.append(holding,ignore_index=True)
        #     holdval = obj.get_totval(date=gv.Yesterday,prctype = prctype)
        #     pofvalue.append(holdval)
        # ******************** 写入 holdlist ***************************
        # holdings.to_csv(holdlst_dir,header=True,index=False)
        # # ******************** 写入产品总资产 ***************************
        # with open(pofval_dir,'w') as pof:
        #     pof.write(str(np.sum(pofvalue)))
        # print('%s : formatted holding list produced' % pofname)
        ##### 初始化父类 #####
        super(Products,self).__init__(pofname=pofname,configdir=configdir)

    @staticmethod
    def addfix(undl,source=gv.SUBSCRIBE_SOURCE,endmark=''):
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

    def list_pofval_gen(self,date = None,prctype = 'settle'):
        """ 从标准数据库中读取持仓信息并写入文件 """
        assert prctype in ('close','settle')
        assetfut = '999997' if prctype=='settle' else '999998'
        if date is None:
            date = dt.datetime.today()
        with da.DatabaseAssistant(dbdir=self._posi_db) as posidb:
            conn = posidb.connection
            stktb = '_'.join([self._pofname,'positions_stocks',date.strftime('%Y%m%d')])
            exeline = ''.join(['SELECT code,num,multi,',prctype,' AS prc FROM ',stktb])
            holdings = pd.read_sql(con=conn,sql=exeline)
            pofval = holdings.loc[holdings['code']=='999999','num'].values[0]
            if self._blog: # 有期货持仓
                futtb = '_'.join([self._pofname,'positions_futures',date.strftime('%Y%m%d')])
                exeline = ''.join(['SELECT code,num,multi,',prctype,' AS prc FROM ',futtb])
                posifut = pd.read_sql(con=conn,sql=exeline)
                holdings = holdings.append(posifut,ignore_index=True)
                pofval += holdings.loc[holdings['code']==assetfut,'num'].values[0]
            holdings = holdings[~holdings['code'].isin(gv.HOLD_FILTER)]
            holdings = holdings[holdings['num']!=0]
            holdings['code'] = holdings['code'].map(Products.addfix)
            #### 写入文件 #######
            holdings.to_csv(self._holdlst_dir,header=True,index=False)
            with open(self._pofval_dir,'w') as pof:
                pof.write(str(pofval))
            print('[+]{0} : formatted holding list produced'.format(self._pofname))


if __name__=='__main__':
    pofname = 'BaiQuanLiShi1'
    configdir = r'E:\realtime_monitors\realtime_returns\configures'
    obj = Products(pofname=pofname,configdir=configdir)
    obj.list_pofval_gen(date=dt.datetime(2017,9,22))
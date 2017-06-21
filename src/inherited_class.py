
import os

from src.holding_generate import *
from src.global_vars import *
from src.help_functions import *
from src.portfolio_class import *
from src.raw_holding_process import *



class Products(Portfolio):
    """ 继承portfolio得类，将Portfolio类连接上数据库，并不是所有产品都会需要数据库 """
    def __init__(self,pofname,pofval_dir,holdlst_dir,trdlst_dir,handlst_dir,cwstatus_dir,log_dir,rawhold_dir,rawtrd_dir,holddb_dir,trddb_dir,othersource=None):
        self.rawhold_dir = rawhold_dir           # 存储从软件端导出的持仓记录的文件夹
        self.rawtrd_dir = rawtrd_dir             # 存储从软件端导出的交易记录的文件夹
        self.holddb_dir = holddb_dir             # 存储每日持仓记录的数据库 格式类似于holdlst_dir 为基于标的的字典 ex. {'stocks':_dir1,futures:_dir2}
        self.trddb_dir = trddb_dir               # 存储每日交易记录的数据库 格式类似于holdlst_dir 为基于标的的字典 ex. {'stocks':_dir1,futures:_dir2}
        # 创建对象时初始化数据库，读取并更新持仓数据库，并生成“标准格式”

        pofvalue = 0
        for k in holdlst_dir:    # 读取持仓信息
            ###################################  method 1 #############################################################
            # if k == 'stocks':
            #     cdmark = '证券代码'
            #     cumark = '币种'
            # else:
            #     cdmark = '合约'
            #     cumark = None
            # obj = ClientToDatabase(hold_dbdir=holddb_dir[k],trd_dbdir=trddb_dir[k],pofname=pofname)   # 创建客户端转数据库对象
            # hold_name = ''.join([pofname,'_positions_',k,'_',TODAY])
            # hold_table = os.path.join(rawhold_dir[k],hold_name+'.csv')
            # # 写入数据库
            # #obj.holdlist_to_db(tabledir=hold_table,textvars=TEXT_VARS_H[pofname][k],tablename=hold_name,codemark=cdmark,replace=True,currencymark=cumark)
            # holdlst_dir[k] = os.path.join(os.path.join(holdlst_dir[k],hold_name+'.csv'))  # Portfolio class 需要能够直接读取的holdlist 文件
            # obj.holdlist_format(titles=HOLD_VARS[pofname][k],outdir=holdlst_dir[k],tablename=hold_name,undltype=k)   # 从数据库提取标准格式
            # holdval = obj.get_totvalue(titles=VALUE_VARS[pofname][k],tablename=hold_name,othersource=othersource.get(k))
            # pofvalue += holdval
            ###################################  method 1 #############################################################
            hold_name = ''.join([pofname,'_positions_',k,'_',TODAY])
            holdlst_dir[k] = os.path.join(os.path.join(holdlst_dir[k],hold_name+'.csv'))  # Portfolio class 需要能够直接读取的holdlist 文件，期货的即使没有也要修改
            if k == 'stocks':
                cdmark = '证券代码'
                cumark = '币种'
                obj = rawholding_stocks(hold_dbdir=holddb_dir[k],pofname=pofname)   # 创建客户端转数据库对象
                hold_table = os.path.join(rawhold_dir[k],hold_name+'.csv')
                # 写入数据库
                obj.holdlist_to_db(tabledir=hold_table,textvars=TEXT_VARS_H[pofname][k],tablename=hold_name,codemark=cdmark,currencymark=cumark,replace=True)
                obj.holdlist_format(titles=HOLD_VARS[pofname][k],tablename=hold_name,outdir=holdlst_dir[k])   # 从数据库提取标准格式
                holdval = obj.get_totvalue(titles=VALUE_VARS[pofname][k],tablename=hold_name,othersource=othersource.get(k))
                pofvalue += holdval
            elif k == 'futures' and 'Hedge' in cwstatus_dir:
                obj = rawholding_futures(hold_dbdir=holddb_dir[k],pofname=pofname,logdir=log_dir,cwdir=cwstatus_dir['Hedge'])
                obj.holdlist_format(cttype='IC',outdir=holdlst_dir[k])
                holdval = obj.get_totval(date=YESTERDAY)
                pofvalue += holdval['total_close']
        # 写入产品总资产
        #clear_dir(pofval_dir)  # 先清空确保没有以前的文件的影响
        pofvaldir = os.path.join(pofval_dir,''.join(['pofvalue',TODAY,'.txt']))
        with open(pofvaldir,'w') as pof:
            pof.write(str(pofvalue))
        super(Products,self).__init__(pofname,pofvaldir,holdlst_dir,trdlst_dir,handlst_dir,cwstatus_dir)

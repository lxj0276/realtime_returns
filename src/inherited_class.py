
import os

from src.database import *
from src.global_vars import *
from src.help_functions import *
from src.portfolio_class import *


class Products(Portfolio):
    """ 继承portfolio得类，将Portfolio类连接上数据库，并不是所有产品都会需要数据库 """
    def __init__(self,pofname,pofval_dir,holdlst_dir,trdlst_dir,handlst_dir,cwstatus_dir,rawhold_dir,rawtrd_dir,holddb_dir,trddb_dir):
        self.rawhold_dir = rawhold_dir           # 存储从软件端导出的持仓记录的文件夹
        self.rawtrd_dir = rawtrd_dir             # 存储从软件端导出的交易记录的文件夹
        self.holddb_dir = holddb_dir             # 存储每日持仓记录的数据库 格式类似于holdlst_dir 为基于标的的字典 ex. {'stocks':_dir1,futures:_dir2}
        self.trddb_dir = trddb_dir               # 存储每日交易记录的数据库 格式类似于holdlst_dir 为基于标的的字典 ex. {'stocks':_dir1,futures:_dir2}
        # 创建对象时初始化数据库，读取并更新持仓数据库，并生成“标准格式”
        k = 'stocks'   # 目前只配置了股票
        obj = ClientToDatabase(holddb_dir[k],pofname)   # 创建客户端转数据库对象
        raw_hold_name = ''.join([pofname,'_positions_',k,'_',TODAY,'.csv'])
        obj.holdlist_to_db(os.path.join(rawhold_dir[k],raw_hold_name),TEXT_VARS[pofname][k],currencymark='币种',codemark='证券代码',replace=True)
        holdlst_dir[k] = os.path.join(os.path.join(holdlst_dir[k],obj.holdtbname+'.csv'))  # Portfolio class 需要能够直接读取的holdlist 文件
        obj.holdlist_format(HOLD_VARS[pofname][k],holdlst_dir[k])
        super(Products,self).__init__(pofname,pofval_dir,holdlst_dir,trdlst_dir,handlst_dir,cwstatus_dir)

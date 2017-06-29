
import os
import configparser as cp

from src.portfolio_class import *
from src.raw_holding_process import *



class Products(Portfolio):
    """ 继承portfolio得类，将Portfolio类连接上数据库，并不是所有产品都会需要数据库 """
    def __init__(self,pofname,configdir):
        # 读取文件路径配置
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(configdir,'_directories.ini'))
        # 读取产品配置
        product_cf = cp.ConfigParser()
        product_cf.read(os.path.join(configdir,'.'.join([pofname,'ini'])))
        ########################### 提取当前类所需的路径文件 #########################
        self._rawhold_dir = os.path.join(cfp.get('dirs','raw_holding'),pofname)           # 存储从软件端导出的持仓记录的文件夹
        self._rawtrd_dir = os.path.join(cfp.get('dirs','raw_trading'),pofname)             # 存储从软件端导出的交易记录的文件夹
        self._holddb_dir = os.path.join(cfp.get('dirs','products_db'),pofname,'_'.join([pofname,'holding.db']))     # 存储每日持仓记录的数据库
        self._trddb_dir = os.path.join(cfp.get('dirs','products_db'),pofname,'_'.join([pofname,'trading.db']))      # 存储每日交易记录的数据库
        ########################### 提取父类初始化所需的各种路径 #########################
        pofval_dir = os.path.join(cfp.get('dirs','pofval'),pofname,'_'.join([pofname,'pofvalue',TODAY+'.txt']))
        holdlst_dir = os.path.join(cfp.get('dirs','list_holding'),pofname,'_'.join([pofname,'positions',TODAY+'.csv']))
        trdlst_dir = ''
        handlst_dir = ''
        cwstatus_dirs = dict(product_cf.items('cwstate'))
        # 将原始数据(通达信导出，ctp记录等)导入数据库，并声称portfolio_class 使用的标准持仓格式 (期货、股票以及其他标的（如果有）在一张表格中)
        pofvalue = []
        holdings = pd.DataFrame()
        ######################### 读取 股票 持仓信息   #########################
        print('%s : updating stocks holding...' % pofname)
        rawfile = os.path.join(self._rawhold_dir,''.join([pofname,'_positions_stocks_',TODAY,'.csv']))
        obj = rawholding_stocks(hold_dbdir=self._holddb_dir,pofname=pofname)   # 创建客户端转数据库对象
        obj.holdlist_to_db(tabledir=rawfile,textvars=product_cf.get('stocks','text_vars_hold').split(','),replace=True)  # 写入数据库
        holding = obj.holdlist_format(titles=product_cf.get('stocks','vars_hold').split(','))   # 从数据库提取标准格式
        holdings = holdings.append(holding,ignore_index=True)
        holdval = obj.get_totvalue(titles=product_cf.get('stocks','vars_value').split(','),othersource=os.path.join(cfp.get('dirs','other'),pofname+'.txt'))
        pofvalue.append(holdval)
        ######################### 读取 期货 持仓信息   #########################
        if product_cf.options('blog'):
            print('%s : updating futures holding...' % pofname)
            obj = rawholding_futures(hold_dbdir=self._holddb_dir,pofname=pofname,logdir=dict(product_cf.items('blog')),cwdir=cwstatus_dirs)
            holding = obj.holdlist_format(prctype='close',preday=True,source='gm')
            holdings = holdings.append(holding,ignore_index=True)
            holdval = obj.get_totval(date=Yesterday,prctype = 'close')
            pofvalue.append(holdval)
        # ******************** 写入 holdlist ***************************
        holdings.to_csv(holdlst_dir,header=True,index=False)
        # ******************** 写入产品总资产 ***************************
        with open(pofval_dir,'w') as pof:
            pof.write(str(np.sum(pofvalue)))
        print('%s : formatted holding list produced' % pofname)
        ##### 初始化父类 #####
        super(Products,self).__init__(pofname=pofname,pofval_dir=pofval_dir,holdlst_dir=holdlst_dir,trdlst_dir=trdlst_dir,handlst_dir=handlst_dir,cwstatus_dirs=cwstatus_dirs)


if __name__=='__main__':
    pofname = 'BaiQuan1'
    configdir = r'E:\realtime_monitors\realtime_returns\configures'
    test = Products(pofname,configdir)
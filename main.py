
import configparser as cp

from src.global_vars import *
from src.product_class import *
from src.portfolio_class import *

import threading

######### 提取原始文件
nametrans = {
    'bq1':'BaiQuan1',
    'bq2':'BaiQuan2',
    'bq3':'BaiQuan3',
    'jq1':'BaiQuanJinQu1',
    'hj1':'BaiQuanHuiJin1',
    # 'ls1':'BaiQuanLiShi1',
    # 'gd2':'GuoDaoLiShi2',
    'xy7':'XingYing7',
    'ms1':'BaiQuanMS1'
}

fromdir = r'C:\Users\Jiapeng\Desktop\tempholdings'
todir = r'E:\realtime_monitors\realtime_returns\raw_holdings'
for p in nametrans:
    fromfile = os.path.join(fromdir,''.join([p,'_',YESTERDAY,'.csv']))
    tofile = os.path.join(todir,nametrans[p],''.join([nametrans[p],'_positions_stocks_',TODAY,'.csv']))
    if not os.path.exists(tofile):
        os.system('copy %s %s ' %(fromfile,tofile))

print('\n copying ls1 value...')
ls1val_from = os.path.join(r'E:\calc_dividend\holding_gen\ls1_value',''.join(['ls1_value_',YESTERDAY,'.txt']))
ls1val_to = r'E:\realtime_monitors\realtime_returns\pofvalues\_pofvalues_others\BaiQuanLiShi1.txt'
os.system('copy /Y %s %s' %(ls1val_from,ls1val_to))
print('\n')

# 配置文件路径
configdir = r'E:\realtime_monitors\realtime_returns\configures'
cfp = cp.ConfigParser()
cfp.read(os.path.join(configdir,'local_directories.ini'))

for p in nametrans:
    pofname = nametrans[p]
    ############ 检测是否有新产品，如果有需要建立新文件夹 ##########
    for dirname in cfp.options('dirs'):
        if dir in ('products_db','other'):
            continue
        path = os.path.join(cfp.get('dirs',dirname),pofname)
        if not os.path.exists(path):
            os.system('mkdir '+path)
            print('%s created !' % path)
    ############ 生成对象  ##################
    obj_bq = Products(pofname=pofname,configdir=configdir)
    print('\n')

########## 开始画图 ##########
try:
    Portfolio.update_undlpool()
except:
    raise
finally: ######### 关闭子进程 ############
    for source in PRE_THREADS:
        PRE_THREADS[source].stop()

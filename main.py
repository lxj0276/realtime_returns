
from src.product_class import *
from src.portfolio_class import *



######### 提取原始文件
nametrans = {'bq1':'BaiQuan1',
             'bq2':'BaiQuan2',
             'jq1':'BaiQuanJinQu1',
             'hj1':'BaiQuanHuiJin1',
             'ls1':'BaiQuanLiShi1',
             'gd2':'GuoDaoLiShi2',
             'xy7':'XingYing7'
             }

fromdir = r'C:\Users\Jiapeng\Desktop\新建文件夹'
todir = r'E:\realtime_monitors\realtime_returns\raw_holdings'
for p in nametrans:
    fromfile = os.path.join(fromdir,''.join([p,'_',YESTERDAY,'.csv']))
    tofile = os.path.join(todir,nametrans[p],''.join([nametrans[p],'_positions_',TODAY,'.csv']))
    os.system('copy %s %s ' %(fromfile,tofile))

# 配置文件路径
configdir = r'E:\realtime_monitors\realtime_returns\configures'

for p in nametrans:
    if p != 'bq1':
        continue
    pofname = nametrans[p]
    obj_bq = Products(pofname=pofname,configdir=configdir)

Portfolio.update_undlpool()
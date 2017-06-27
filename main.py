
import configparser as cp

from src.product_class import *
from src.portfolio_class import *



######### 提取原始文件
# temptrans = {'bq1':['BaiQuan1','Baiquan1'],
#              'bq2':['BaiQuan2','Baiquan2'],
#              'jq1':['BaiQuanJinQu1','Jinqu1'],
#              'hj1':['BaiQuanHuiJin1','Huijin1'],
#              'ls1':['BaiQuanLiShi1','Lishi1'],
#              'gd2':['GuoDaoLiShi2','Guodao2'],
#              'xy7':['XingYing7','Xingying7']
#              }
#
# #fromdir = r'E:\calc_dividend\holding_gen\rawholding'
# fromdir = r'C:\Users\Jiapeng\Desktop\新建文件夹'
# todir = r'E:\realtime_monitors\realtime_returns\raw_holdings'
# for p in temptrans:
#     #fromfile = os.path.join(fromdir,p,''.join([p,'_',TODAY,'.csv']))
#     fromfile = os.path.join(fromdir,''.join([p,'_',YESTERDAY,'.csv']))
#     tofile = os.path.join(todir,temptrans[p][0],'stocks',''.join([temptrans[p][1],'_positions_stocks_',TODAY,'.csv']))
#     os.system('copy %s %s ' %(fromfile,tofile))
#
#     fut_from = os.path.join(fromdir,''.join([p,'_170523.csv']))
#     fut_to = os.path.join(todir,temptrans[p][0],'futures',''.join([temptrans[p][1],'_positions_futures_',TODAY,'.csv']))
#     os.system('copy %s %s ' %(fut_from,fut_to))
#
#
#
products = ['Baiquan1','Baiquan2','Jinqu1','Huijin1','Guodao2','Lishi1','Xingying7']
no_futures = ['Huijin1','Jinqu1']

nametrans = {'Baiquan1':'BaiQuan1',
             'Baiquan2':'BaiQuan2',
             'Jinqu1' :'BaiQuanJinQu1',
             'Huijin1':'BaiQuanHuiJin1',
             'Guodao2':'GuoDaoLiShi2',
             'Lishi1' : 'BaiQuanLiShi1',
             'Xingying7':'XingYing7'}

# 读取配置文件
configdir = r'E:\realtime_monitors\realtime_returns\configures'
cfp = cp.ConfigParser()
cfp.read(os.path.join(configdir,'_directories.ini'))

list_holding = cfp.get('dirs','list_holding')
list_trading = cfp.get('dirs','list_trading')
list_byhand  = cfp.get('dirs','list_byhand')
raw_holding = cfp.get('dirs','raw_holding')
raw_trading = cfp.get('dirs','raw_trading')
db = cfp.get('dirs','products_db')
pofval = cfp.get('dirs','pofvalues')
other = cfp.get('dirs','other')

# cwstatedir = {
#     'Baiquan1':{ 'Long': r'\\BQ1_ICLONG\cwstate',
#                  'Hedge':r'\\BQ1_ICHEDGE\cwstate' },
#     'Baiquan2':{'Hedge': r'\\BQ2_ICHEDGE\cwstate'},
#     'Jinqu1' :{'Long':  r'\\JQ1_ICLONG\cwstate'},
#     'Huijin1':{'Long':  r'\\HJ1_ICLONG\cwstate'},
#     'Guodao2':{'Long':  r'\\GD2_ICLONG\cwstate',
#                'Hedge': r'\\GD2_ICHEDGE\cwstate'},
#     'Lishi1' :{'Long':  r'\\LS1_ICLONG\cwstate',
#                'Hedge': r'\\LS1_ICHEDGE\cwstate'},
#     'Xingying7':{'Hedge': r'\\XY7_ICHEDGE\cwstate'}
# }
#
# logdir = {
#     'Baiquan1':r'\\BQ1_ICHEDGE\blog',
#     'Baiquan2':r'\\BQ2_ICHEDGE\blog',
#     'Guodao2':r'\\GD2_ICHEDGE\blog',
#     'Lishi1':r'\\LS1_ICHEDGE\blog',
#     'Xingying7':r'\\XY7_ICHEDGE\blog',
# }

pofobjs = []
for pofname in products:
    pfilename = nametrans[pofname]
    holdlst_dir = {'stocks' :os.path.join(list_holding,pfilename,'stocks'),
                   'futures':os.path.join(list_holding,pfilename,'futures')}
    trdlst_dir = {'stocks' :os.path.join(list_trading,pfilename,'stocks'),
                  'futures':os.path.join(list_trading,pfilename,'futures')}
    handlst_dir = {'stocks' :os.path.join(list_byhand,pfilename,'stocks'),
                   'futures':os.path.join(list_byhand,pfilename,'futures')}

    rawhold_dir = {'stocks' :os.path.join(raw_holding,pfilename,'stocks'),
                   'futures':os.path.join(raw_holding,pfilename,'futures')}
    rawtrd_dir = {'stocks' :os.path.join(raw_trading,pfilename,'stocks'),
                  'futures':os.path.join(raw_trading,pfilename,'futures')}

    db_holding = {'stocks' : os.path.join(db,pfilename,'stocks',''.join([pofname,'_stocks_holding.db'])),
                  'futures' : os.path.join(db,pfilename,'futures',''.join([pofname,'_futures_holding.db']))}
    db_trading = {'stocks' : os.path.join(db,pfilename,'stocks',''.join([pofname,'_stocks_trading.db'])),
                  'futures' : os.path.join(db,pfilename,'futures',''.join([pofname,'_futures_trading.db']))}

    othersources = {'stocks':os.path.join(other,pofname+'_stocks.txt'),
                    'futures':os.path.join(other,pofname+'_futures.txt')}

    pofval_dir = os.path.join(pofval,pfilename)
    log_dir = logdir.get(pofname)


    obj_bq = Products(pofname=pofname,pofval_dir=pofval_dir,holdlst_dir=holdlst_dir,trdlst_dir=trdlst_dir,handlst_dir=trdlst_dir,
                      rawhold_dir=rawhold_dir,rawtrd_dir=rawtrd_dir,holddb_dir=db_holding,trddb_dir=db_trading,
                      cwstatus_dir=cwstatedir[pofname],othersource=othersources,log_dir=log_dir)
    #pofobjs.append(obj_bq)

#update_undlpool(pofobjs=pofobjs)
Portfolio.update_undlpool()
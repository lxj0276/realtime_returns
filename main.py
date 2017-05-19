
from src.inherited_class import *
from src.portfolio_class import *




temptrans = {'bq1':['BQ1','Baiquan1'],
             'bq2':['BQ2','Baiquan2'],
             'jq1':['BQJQ1','Jinqu1'],
             'hj1':['BQHJ1','Huijin1'],
             'ls1':['BQLS1','Lishi1'],
             'gd2':['GDLS2','Guodao2'],
             'xy7':['XY7','Xingying7']
             }

#fromdir = r'E:\calc_dividend\holding_gen\rawholding'
fromdir = r'C:\Users\Jiapeng\Desktop\新建文件夹'
todir = r'E:\realtime_monitors\realtime_returns\raw_holdings'
for p in temptrans:
    #fromfile = os.path.join(fromdir,p,''.join([p,'_',TODAY,'.csv']))
    fromfile = os.path.join(fromdir,''.join([p,'_',TODAY,'.csv']))
    tofile = os.path.join(todir,temptrans[p][0],'stocks',''.join([temptrans[p][1],'_positions_stocks_',TODAY,'.csv']))
    os.system('copy %s %s ' %(fromfile,tofile))






products = ['Baiquan1','Baiquan2','Jinqu1','Huijin1','Guodao2','Lishi1','Xingying7']
no_futures = ['Huijin1','Jinqu1']

nametrans = {'Baiquan1':'BaiQuan1',
             'Baiquan2':'BaiQuan2',
             'Jinqu1' :'BaiQuanJinQu1',
             'Huijin1':'BaiQuanHuiJin1',
             'Guodao2':'GuoDaoLiShi2',
             'Lishi1' : 'BaiQuanLiShi1',
             'Xingying7':'XingYing7'}

list_holding = r'.\lists_holding'
list_trading = r'.\lists_trading'
list_byhand  = r'.\lists_byhand'
raw_holding = r'\raw_holdings'
raw_trading = r'\raw_trading'
db = r'\products_database'
pofval = r'\pofvalue'

cwstatedir = {
    'Baiquan1':{ 'Long': r'\\BQ1_ICLONG\cwstate\cwstate.txt',
                 'Hedge':r'\\BQ1_ICHEDGE\cwstate\cwstate.txt' },
    'Baiquan2':{'Long':  r'\\BAIQUAN2TRD1\cwstate\cwstate.txt',
                'Hedge': r'\\BAIQUAN2TRD2\cwstate\cwstate.txt'},
    'Jinqu1' :{'Long':  r'\\JQ1_ICLONG\cwstate\cwstate.txt'},
    'Huijin1':{'Long':  r'\\HUIJIN2\cwstate\cwstate.txt'},
    'Guodao2':{'Long':  r'\\GD2_ICLONG\cwstate\cwstate.txt',
               'Hedge': r'\\TRADING5\cwstate\cwstate.txt'},
    'Lishi1' :{'Long':  r'\\BQLS1_TRADING1\cwstate\cwstate.txt',
               'Hedge': r'\\BQLS1_TRADING2\cwstate\cwstate.txt'},
    'Xingying7':{'Hedge': r'\\XY7_ICHEDGE\cwstate\cwstate.txt'}
}





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
    pofval_dir = os.path.join(pofval,pfilename)

    obj_bq = Products(pofname=pofname,pofval_dir=pofval_dir,holdlst_dir=holdlst_dir,trdlst_dir=trdlst_dir,handlst_dir=trdlst_dir,
                      rawhold_dir=rawhold_dir,rawtrd_dir=rawtrd_dir,holddb_dir=db_holding,trddb_dir=db_trading,
                      cwstatus_dir=cwstatedir[pofname],)

Portfolio.update_undlpool()
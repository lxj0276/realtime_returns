import datetime as dt

UNDL_POOL = {'total':set([])}               # 数据结构 dict： {'total' : 所有标的 set , 'pofname1': {'stocks': set , 'futures': set , ...} , 'pofname2': {...} , ... }
UNDL_POOL_INFO = {}                          # 存储以各个品种code为key的字典
POOL_COLUMNS = ['rt_last']                 #rt_time,rt_pct_chg'
SUBSCRIBE_SOURCE =  'goldmine_pull'                         #  'goldmime_snapshot'  'simulation'   'goldmine'  'wind'
PRE_THREADS = {}

TODAY = str(dt.datetime.strftime(dt.date.today(),'%Y%m%d'))
today = dt.date.today()
START_TIME = dt.datetime(year=today.year, month=today.month,day=today.day,hour= 8,minute=30,second=0)
END_TIME = dt.datetime(year=today.year, month=today.month,day=today.day,hour= 19,minute=45,second=0)

TEXT_VARS = {'Baiquan1': {'stocks': ['备注','股东代码','证券代码','证券名称','资金帐号']},
             'Baiquan2': {'stocks': ['股东代码','证券代码','证券名称']},
             'Jinqu1':   {'stocks': ['股东代码','证券代码','证券名称']},
             'Huijin1':  {'stocks': ['股东代码','证券代码','证券名称']},
             'Guodao2':  {'stocks': ['股东代码','证券代码','证券名称']},
             'Lishi1':   {'stocks': ['产品名称','到期日','股东账号','账号名称','证券代码','证券名称','状态','资金账号']},
             }

HOLD_VARS = {'Baiquan1': {'stocks': ['证券代码','证券名称','参考持股','当前价']},
             'Baiquan2': {'stocks': ['证券代码','证券名称','证券数量','当前价']},
             'Jinqu1':   {'stocks': ['证券代码','证券名称','证券数量','当前价']},
             'Huijin1':  {'stocks': ['证券代码','证券名称','库存数量','当前价']},
             'Guodao2':  {'stocks': ['证券代码','证券名称','证券数量','当前价']},
             'Lishi1':   {'stocks': ['证券代码','证券名称','当前拥股','最新价']},
             }
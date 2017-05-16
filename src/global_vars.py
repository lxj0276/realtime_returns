import datetime as dt
import numpy as np
from WindPy import w


UNDL_POOL = {'total':set([])}               # 数据结构 dict： {'total' : 所有标的 set , 'pofname1': {'stocks': set , 'futures': set , ...} , 'pofname2': {...} , ... }
UNDL_POOL_INFO = {}                          # 存储以各个品种code为key的字典
POOL_COLUMNS = ['rt_last']                 #rt_time,rt_pct_chg'
SUBSCRIBE_SOURCE =  'goldmine_snapshot'                         #  'goldmine_snapshot'  'simulation'   'goldmine'  'wind'
PRE_THREADS = {}
FLUSH_CWSTAT = 1   # 画图更新时间间隔 秒

today = dt.datetime.today()
w.start()
TODAY = today.strftime('%Y%m%d')
YESTERDAY = w.tdaysoffset(-1,TODAY).Times[0].strftime('%Y%m%d')

START_TIME = dt.datetime(year=today.year, month=today.month,day=today.day,hour= 8,minute=30,second=0)
END_TIME = dt.datetime(year=today.year, month=today.month,day=today.day,hour= 19,minute=45,second=0)



TEXT_VARS = {'Baiquan1': {'stocks': ['备注','股东代码','证券代码','证券名称','资金帐号']},
             'Baiquan2': {'stocks': ['股东代码','证券代码','证券名称']},
             'Jinqu1':   {'stocks': ['股东代码','证券代码','证券名称']},
             'Huijin1':  {'stocks': ['股东代码','证券代码','证券名称']},
             'Guodao2':  {'stocks': ['股东代码','证券代码','证券名称']},
             'Lishi1':   {'stocks': ['产品名称','到期日','股东账号','账号名称','证券代码','证券名称','状态','资金账号']},
             'Xingying7':{'stocks': ['股东代码','证券代码','证券名称','交易所名称']}
             }

HOLD_VARS = {'Baiquan1': {'stocks': ['证券代码','证券名称','参考持股','当前价']},
             'Baiquan2': {'stocks': ['证券代码','证券名称','证券数量','当前价']},
             'Jinqu1':   {'stocks': ['证券代码','证券名称','证券数量','当前价']},
             'Huijin1':  {'stocks': ['证券代码','证券名称','库存数量','当前价']},
             'Guodao2':  {'stocks': ['证券代码','证券名称','证券数量','当前价']},
             'Lishi1':   {'stocks': ['证券代码','证券名称','当前拥股','最新价']},
             'Xingying7':{'stocks': ['证券代码','证券名称','实际数量','当前价']}
             }

VALUE_VARS = {'Baiquan1': {'stocks': ['可用','参考市值']},
              'Baiquan2': {'stocks': ['资产']},
              'Jinqu1':   {'stocks': ['资产']},
              'Huijin1':  {'stocks': ['资产']},
              'Lishi1':  {'stocks': []},
              'Guodao2':  {'stocks': ['资产']},
              'Xingying7':{'stocks': ['资产']},
              }



tempprc = 5876.6
FUTURES_INFO = {'Baiquan1':{'tot_value': 2440623.73 ,
                             'IC1705.CFE': { 'settle': tempprc, 'trdside': -1,'multiplier': 200,'holdnum':np.array([4]) } },
                'Baiquan2':{'tot_value': 3872192.93 ,
                             'IC1705.CFE': { 'settle': tempprc, 'trdside': -1,'multiplier': 200,'holdnum':np.array([6]) } },
                'Guodao2' :{'tot_value': 3836095.6,
                            'IC1705.CFE': { 'settle': tempprc, 'trdside': -1,'multiplier': 200, 'holdnum':np.array([6]) } },
                'Lishi1'  :{'tot_value': 8692398.98,
                           'IC1705.CFE': { 'settle': tempprc, 'trdside': -1,'multiplier': 200, 'holdnum':np.array([16]) } }
                }

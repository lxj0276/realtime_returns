import datetime as dt
import numpy as np
from WindPy import w


VERSION = 'Testing'
UNDL_POOL = {'total':set([])}               # 数据结构 dict： {'total' : 所有标的 set , 'pofname1': {'stocks': set , 'futures': set , ...} , 'pofname2': {...} , ... }
UNDL_POOL_INFO = {}                          # 存储以各个品种code为key的字典
POOL_COLUMNS = ['rt_last']                 #rt_time,rt_pct_chg'
SUBSCRIBE_SOURCE = 'goldmine_snapshot'                         #  'goldmine_snapshot'  'simulation'   'goldmine'  'wind'
PRE_THREADS = {}
FLUSH_CWSTAT = 1   # 画图更新时间间隔 秒

STAMP_TAX = 1/1000

today = dt.datetime.today()
if not w.isconnected():
    w.start()
TODAY = today.strftime('%Y%m%d')
YESTERDAY = w.tdaysoffset(-1,TODAY).Times[0].strftime('%Y%m%d')

START_TIME = dt.datetime(year=today.year, month=today.month,day=today.day,hour= 8,minute=30,second=0)
END_TIME = dt.datetime(year=today.year, month=today.month,day=today.day,hour= 19,minute=45,second=0)


# 持仓表格中 在数据库中存为 TEXT 格式的字段
TEXT_VARS_H = {'Baiquan1': {'stocks': ['备注','股东代码','证券代码','证券名称','资金帐号']},
               'Baiquan2': {'stocks': ['股东代码','证券代码','证券名称']},
               'Jinqu1':   {'stocks': ['股东代码','证券代码','证券名称']},
               'Huijin1':  {'stocks': ['股东代码','证券代码','证券名称']},
               'Guodao2':  {'stocks': ['股东代码','证券代码','证券名称']},
               'Lishi1':   {'stocks': ['产品名称','到期日','股东账号','账号名称','证券代码','证券名称','状态','资金账号']},
               'Xingying7':{'stocks': ['股东代码','证券代码','证券名称','交易所名称']}
               }
refound_sz = ['131810','131811','131800','131809','131801','131802','131803','131805','131806']
refound_sh = ['204001','204007','204002','204003','204004','204014','204028','204091','204182']
other_vars = ['131990','888880','SHRQ88','SHXGED','SZRQ88','SZXGED','511990','732855']
HOLD_FILTER = refound_sz+refound_sh+other_vars
# 生成 标准持仓表格 所需提取的数据库中的字段
HOLD_VARS = {'Baiquan1': {'stocks': ['证券代码','证券名称','参考持股','当前价']},
             'Baiquan2': {'stocks': ['证券代码','证券名称','证券数量','当前价']},
             'Jinqu1':   {'stocks': ['证券代码','证券名称','证券数量','当前价']},
             'Huijin1':  {'stocks': ['证券代码','证券名称','库存数量','当前价']},
             'Guodao2':  {'stocks': ['证券代码','证券名称','证券数量','当前价']},
             'Lishi1':   {'stocks': ['证券代码','证券名称','当前拥股','最新价']},
             'Xingying7':{'stocks': ['证券代码','证券名称','实际数量','当前价']}
             }
# 计算股票端总资产所需的字段，为空列表表示没有summary表格，无法计算需要额外提供
VALUE_VARS = {'Baiquan1': {'stocks': ['可用','参考市值']},
              'Baiquan2': {'stocks': ['资产']},
              'Jinqu1':   {'stocks': ['资产']},
              'Huijin1':  {'stocks': ['资产']},
              'Lishi1':  {'stocks': []},
              'Guodao2':  {'stocks': ['资产']},
              'Xingying7':{'stocks': ['资产']},
              }
# 交易表格中 在数据库中存为 TEXT 格式的字段
TEXT_VARS_T = {'Baiquan1': {'stocks': ['成交编号','成交类型','成交时间','成交状态','股东代码','买卖','申请编号','委托编号','委托类型','业务名称','证券代码','证券名称']},
               'Baiquan2': {'stocks': ['成交编号','成交类型','成交时间','股东代码','买卖标志','委托编号','证券代码','证券名称']},
               'Jinqu1':   {'stocks': ['成交编号','成交时间','股东代码','买卖标志','委托编号','委托属性','证券代码','证券名称']},
               'Huijin1':  {'stocks': ['成交编号','成交日期','成交时间','股东代码','买卖标志','委托编号','证券代码','证券名称','状态说明']},
               'Guodao2':  {'stocks': ['报价方式','股东代码','买卖标志','委托编号','委托时间','证券代码','证券名称','状态说明']},
               'Lishi1':   {'stocks': ['成交编号','成交时间','合同编号','买卖标记','账号名称','证券代码','证券公司','证券名称','资金账号']},
               'Xingying7':{'stocks': ['备注','成交编号','成交时间','股东代码','交易所名称','买卖标志','委托编号','摘要','证券代码','证券名称']}
               }
# 生成 标准交易表格 所需提取的数据库中的字段
TRADE_VARS = {'Baiquan1': {'stocks': ['证券代码','证券名称','成交数量','成交价格','买卖']},
              'Baiquan2': {'stocks': ['证券代码','证券名称','成交数量','成交价格','买卖标志']},
              'Jinqu1':   {'stocks': ['证券代码','证券名称','成交数量','成交价格','买卖标志']},
              'Huijin1':  {'stocks': ['证券代码','证券名称','成交数量','成交价格','买卖标志']},
              'Guodao2':  {'stocks': ['证券代码','证券名称','成交数量','成交价格','买卖标志']},
              'Lishi1':   {'stocks': ['证券代码','证券名称','成交拥股','成交价格','买卖标记']},
              'Xingying7':{'stocks': ['证券代码','证券名称','成交数量','成交价格','买卖标志']}
              }



tempprc = 6021.4
FUTURES_INFO = {'Baiquan1':{'tot_value': 2324783.72,
                             'IC1705.CFE': { 'settle': tempprc, 'trdside': -1,'multiplier': 200,'holdnum':np.array([4]) } },
                'Baiquan2':{'tot_value': 3698432.92,
                             'IC1705.CFE': { 'settle': tempprc, 'trdside': -1,'multiplier': 200,'holdnum':np.array([6]) } },
                'Guodao2' :{'tot_value': 3662335.59,
                            'IC1705.CFE': { 'settle': tempprc, 'trdside': -1,'multiplier': 200, 'holdnum':np.array([6]) } },
                'Lishi1'  :{'tot_value': 8229038.97,
                           'IC1705.CFE': { 'settle': tempprc, 'trdside': -1,'multiplier': 200, 'holdnum':np.array([16]) } },
                'Xingying7' :{'tot_value': 4253431.55,
                              'IC1705.CFE': { 'settle': tempprc, 'trdside': -1,'multiplier': 200, 'holdnum':np.array([1]) } }
                }

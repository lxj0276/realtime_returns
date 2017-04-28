

UNDL_POOL = {'total':set([])}               # 数据结构 dict： {'total' : 所有标的 set , 'pofname1': {'stocks': set , 'futures': set , ...} , 'pofname2': {...} , ... }
UNDL_POOL_INFO = {}                          # 存储以各个品种code为key的字典
POOL_COLUMNS = 'rt_last'                    #rt_time,rt_pct_chg'
CALLBACK_TYPE =  'simulation'   # 'wind'
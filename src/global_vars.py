import datetime as dt
from remotewind import w


VERSION = 'Testing'
UNDL_POOL = {'total':set([])}               # 数据结构 dict： {'total' : 所有标的 set , 'pofname1': {'stocks': set , 'futures': set , ...} , 'pofname2': {...} , ... }
UNDL_POOL_INFO = {}                          # 存储以各个品种code为key的字典
POOL_COLUMNS = ['rt_last']                 #rt_time,rt_pct_chg'
SUBSCRIBE_SOURCE = 'goldmine_snapshot'                         #  'goldmine_snapshot'  'simulation'   'goldmine'  'wind'
PRE_THREADS = {}
FLUSH_CWSTAT = 0.5   # 画图更新时间间隔 秒

w.start()
Today = dt.datetime.today()
TODAY = Today.strftime('%Y%m%d')
Yesterday =  w.tdaysoffset(-1,TODAY).Times[0]
YESTERDAY = Yesterday.strftime('%Y%m%d')

START_TIME = dt.datetime(year=Today.year, month=Today.month,day=Today.day,hour= 8,minute=30,second=0)
MID1_TIME = dt.datetime(year=Today.year, month=Today.month,day=Today.day,hour= 11,minute=30,second=0)
MID2_TIME = dt.datetime(year=Today.year, month=Today.month,day=Today.day,hour= 13,minute=0,second=0)
END_TIME = dt.datetime(year=Today.year, month=Today.month,day=Today.day,hour= 18,minute=15,second=0)
PLOT_POINTS = int((END_TIME-START_TIME).seconds/FLUSH_CWSTAT)

# 生成股票持仓需要过滤的代码
refound_sz = ['131810','131811','131800','131809','131801','131802','131803','131805','131806']
refound_sh = ['204001','204007','204002','204003','204004','204014','204028','204091','204182']
other_vars = ['131990','888880','SHRQ88','SHXGED','SZRQ88','SZXGED','511990','732855','NEWSTOCK']
HOLD_FILTER = refound_sz+refound_sh+other_vars
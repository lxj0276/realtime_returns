
import datetime as dt
import pandas as pd
from gmsdk import md


class gm_daily:

    def __init__(self,username,password):
        self._username = username
        self._password = password
        md.init(username=username,password=password)

    def gmwsd(self,code,valstr,startdate,enddate):
        # 假设startdate,enddate均为datetime/time 格式
        cols = ['strtime','open','high','low','close','volume','amount','settle_price']
        today = dt.datetime.today()
        if 'strtime' not in valstr:
            valstr = ','.join(['strtime',valstr])
        if enddate.strftime('%Y-%m-%d')>=today.strftime('%Y-%m-%d'):       # 需要取今日数据
            nowtime = dt.datetime.now()
            if nowtime.hour*100+nowtime.minute> 1500:   # 收盘后请求当日数据，根据tick数据构建
                head = dt.datetime(year=today.year,month=today.month,day=today.day,hour=15)
                tail = dt.datetime(year=today.year,month=today.month,day=today.day,hour=16)
                tickdata = md.get_ticks(code,head.strftime('%Y-%m-%d %H:%M:%S'),tail.strftime('%Y-%m-%d %H:%M:%S'))
                t = tickdata[-1]
                todaydata = [t.strtime,t.open,t.high,t.low,t.last_price,t.cum_volume,t.cum_amount,t.settle_price]   # 将所有字段添加if needed
            else:   #收盘前请求当日数据，将返回前一日结果
                lastdata = md.get_last_dailybars(code)
                t = lastdata[0]
                todaydata = [t.strtime,t.open,t.high,t.low,t.close,t.volume,t.amount,t.settle_price]
            todaydata = pd.DataFrame([todaydata],columns=cols)
        else:
            todaydata = pd.DataFrame()
        if startdate==enddate:   # 不再需要其他数据
            return todaydata.loc[:,valstr.split(',')]
        else:
            enddate += dt.timedelta(days=-1)
            tempdata = md.get_dailybars(code,startdate.strftime('%Y-%m-%d'),enddate.strftime('%Y-%m-%d'))
            predata = [[t.strtime,t.open,t.high,t.low,t.close,t.volume,t.amount,t.settle_price] for t in tempdata]
            predata = pd.DataFrame(predata,columns=cols)
            return predata.append(todaydata,ignore_index=True).loc[:,valstr.split(',')]

if __name__=='__main__':
    obj = gm_daily('18201141877','Wqxl7309')
    code = 'CFFEX.IC1707'
    #code = 'SHSE.000905'
    startdate = dt.datetime(year=2017,month=6,day=20)
    enddate = dt.datetime(year=2017,month=6,day=26)
    valstr=','.join(['open','low','high','close,settle_price'])

    data = obj.gmwsd(code,valstr,startdate,enddate)

    print(data)
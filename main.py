
from src.base_class import *


if __name__=='__main__':
    pofname1=u'组合1'
    pofvalue1=820386
    hldlstdir1={'stocks' : '.\BQ1ICLong20170421.csv'}
    trdlstdir1={'stocks' : r'E:\realtime_monitors\realtime_returns\testfiles'}
    cwstatusdir1=r'.\cwstate1.txt'

    t1=portfolio(pofname1,pofvalue1,hldlstdir1,trdlstdir1,cwstatusdir1)

    pofname2=u'组合2'
    pofvalue2=820386
    hldlstdir2={'stocks' : '.\BQ1ICLong20170421.csv'}
    trdlstdir2={'stocks' : r'E:\realtime_monitors\realtime_returns\testfiles'}
    cwstatusdir2=r'.\cwstate2.txt'

    t2=portfolio(pofname2,pofvalue2,hldlstdir2,trdlstdir2,cwstatusdir2)

    pofname3=u'组合3'
    pofvalue3=820386
    hldlstdir3={'stocks' : '.\BQ1ICLong20170421.csv'}
    trdlstdir3={'stocks' : r'E:\realtime_monitors\realtime_returns\testfiles'}
    cwstatusdir3=r'.\cwstate3.txt'

    t3=portfolio(pofname3,pofvalue3,hldlstdir3,trdlstdir3,cwstatusdir3)

    portfolio.update_undlpool()
    w.cancelRequest(0)

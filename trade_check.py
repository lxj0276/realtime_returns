#encoding=utf-8
import sqlite3
import os
import datetime

class DBconnection:
    def __init__(self,dbdir):
        self.dbdir=dbdir
    def __enter__(self):
        self.connection=sqlite3.connect(self.dbdir)
        return self.connection
    def __exit__(self,exc_type,exc_instantce,traceback):
        self.connection.close()


def check_trdtoday(dbdir,cwdir,levels=2):
    if not os.path.exists(dbdir):
        with DBconnection(dbdir) as db:
            c=db.cursor()
            c.execute('CREATE TABLE lastcwstate (position,trdnum,spotprc,futprc,spread,trddate)')
            for dumi in range(levels):
                c.execute('INSERT INTO lastcwstate VALUES (?,?,?,?,?,?)',(0,0,0,0,0,0))
                db.commit()
    if not os.path.exists(cwdir):
        raise Exception('No cwdir exist!')

    with DBconnection(dbdir) as db:
        c=db.cursor()
        last=c.execute('SELECT * FROM lastcwstate').fetchall()

    trades={}
    with open(cwdir,'r') as cwinfo:
        temp=cwinfo.readlines()
        contents_temp=[c.strip().split(',') for c in temp]
        contents=[[int(c) for c in t] for t in contents_temp]
        currlevels=len(contents)
        if currlevels != levels:
            raise Exception('cwstate has level num: %d, while current setting of level num: %d' %(currlevels,levels))
        buy=0
        sell=0
        hold=0
        for dumi in range(currlevels):
            buy+=(contents[dumi][0]==-1 and last[dumi][0]==0)
            sell+=(contents[dumi][0]==0 and last[dumi][0]==-1)
            hold+=(contents[dumi][0]==-1)
        trades['buylevels']=buy
        trades['selllevels']=sell
        trades['holdlelves']=hold

    with DBconnection(dbdir) as db:
        c=db.cursor()
        c.execute('DROP TABLE lastcwstate')
        c.execute('CREATE TABLE lastcwstate (position,trdnum,spotprc,futprc,spread,trddate)')
        for dumi in range(levels):
            c.execute('INSERT INTO lastcwstate VALUES (?,?,?,?,?,?)',contents[dumi])
            db.commit()

    today=datetime.datetime.today().strftime("%Y%m%d ")
    with DBconnection(dbdir) as db:
        c=db.cursor()
        alltbs=c.execute('SELECT name FROM sqlite_master WHERE type=\'table\'').fetchall()
        hastb=('lastcwstate'+today) in  alltbs
        if not hastb:
            exeline='CREATE TABLE lastcwstate'+today+ ' (position,trdnum,spotprc,futprc,spread,trddate)'
            c.execute(exeline)
            for dumi in range(levels):
                exeline='INSERT INTO lastcwstate'+today+' VALUES (?,?,?,?,?,?)'
                c.execute(exeline,contents[dumi])
                db.commit()
    return trades


if __name__=='__main__':
    dbdir=r'\testfiles\db.db'
    cwdir=r'\testfiles\cwstate.txt'
    check_trdtoday(dbdir,cwdir)


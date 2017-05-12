import time
import threading

from gmsdk import md


ret = md.init(
                username="18201141877",
                password="Wqxl7309",
                mode= 2,
                subscribe_symbols="SHSE.600000.tick,SZSE.000001.tick")

# ret = md.init(username="18201141877", password="Wqxl7309",mode=2)
# ret = md.subscribe("SHSE.600000.bar.60,SZSE.000001.bar.30")
# ret = md.subscribe("SHSE.600000.tick,SZSE.000001.tick")



def on_tick(tick):
    print('in on_tick')
    print('%s %s %s' % ( tick.strtime, tick.sec_id, tick.last_price))
    print(tick)


md.ev_tick += on_tick

def fastshot():
    while True:
        time.sleep(1)
        ticks = md.get_last_ticks("SHSE.600000,SZSE.000001")
        print(ticks[0].last_price,ticks[1].last_price)


threading.Thread(target=fastshot).start()
threading.Thread(target=md.run).start()
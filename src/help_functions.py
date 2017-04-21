

def holdlist_format():
    # 做多 数量为正， 做空为负， 价格衡正
    pass

def trdlist_format():
    pass



import matplotlib.pyplot as plt

import numpy as np

x=[]
y=[]
fig=plt.figure()
ax=fig.add_subplot(111)
ax.set_xlim(0,100)
for i in range(100):
    x.append(i)
    y.append(np.random.rand())
    ax.plot(x,y,color='r')
    #ax.plot(i,np.random.rand(),color='r')
    plt.pause(0.1)
from binance.client import Client
from binance.websockets import BinanceSocketManager
import pymongo
import inspect 
from pymongo import MongoClient

import time
import csv 



binance = ['BTCUSDC',
'FETBTC',
'ZILBTC',
'ADABTC',
'BTCPAX',
'CELRBTC',
'BTCTUSD',
'XMRBTC',
'NULSBTC',
'FTMBTC'

]
mydb = ['BTCUSDC_db',
'FETBTC_db',
'ZILBTC_db',
'ADABTC_db',
'BTCPAX_db',
'CELRBTC_db',
'BTCTUSD_db',
'XMRBTC_db',
'NULSBTC_db',
'FTMBTC_db'

]
mycol = ['BTCUSDC_col',
'FETBTC_col',
'ZILBTC_col',
'ADABTC_col',
'BTCPAX_col',
'CELRBTC_col',
'BTCTUSD_col',
'XMRBTC_col',
'NULSBTC_col',
'FTMBTC_col'

]


client = Client("a","b")

bm = BinanceSocketManager(client)


#add the coins here 
#________________________________________________________________________
interval = '15m'
#binance = ['adabtc','aionbtc', 'arnbtc', 'batbtc']
#mydb = ['adabtc','aionbtc', 'arnbtc', 'batbtc']
#mycol = ['adabtc','aionbtc', 'arnbtc', 'batbtc']
#add the mongo details per coin here
#________________________________________________________________________

myclient = MongoClient('mongodb+srv://hello:Muhammad00@cluster0-sry3w.mongodb.net/test?retryWrites=true&w=majority')


###########################################################

for x in range(0,len(binance)):
    mydb[x] = myclient[mydb[x]]           #create database
    mycol[x] = mydb[x] ['data']     #create collection   
    
    
    
#________________________________________________________________________________






def process_message0(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message1(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message2(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message3(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])   

def process_message4(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message5(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message6(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message7(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c']) 
def process_message8(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c'])
    
def process_message9(g):
    frame = inspect.currentframe()
    s = int(inspect.getframeinfo(frame).function[-1])
       
    mydict = [g]
    mycol[s].insert_many(mydict)
    print(binance[s], "----", g['k']['c']) 

conn_key = bm.start_kline_socket('BTCUSDC',process_message0 , interval=interval)
conn_key = bm.start_kline_socket('FETBTC',process_message1 , interval=interval)
conn_key = bm.start_kline_socket('ZILBTC',process_message2 , interval=interval)
conn_key = bm.start_kline_socket('ADABTC',process_message3 , interval=interval)
conn_key = bm.start_kline_socket('BTCPAX',process_message4 , interval=interval)
conn_key = bm.start_kline_socket('CELRBTC',process_message5 , interval=interval)
conn_key = bm.start_kline_socket('BTCTUSD',process_message6 , interval=interval)
conn_key = bm.start_kline_socket('XMRBTC',process_message7 , interval=interval)
conn_key = bm.start_kline_socket('NULSBTC',process_message8 , interval=interval)
conn_key = bm.start_kline_socket('FTMBTC',process_message9 , interval=interval)



bm.start()

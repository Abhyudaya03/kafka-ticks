import threading
from flask import Flask
from go_kicker_mqtt import KickerClient
from queue import Queue
import requests
q=Queue()

kc = KickerClient('34.100.136.129',1883,'gokicker','zztubWDYKRALvfZN',q,['localhost:9092'])
kc.connect_mqtt()
t= threading.Thread(target=kc.start)
t.setDaemon(True)
t.start()

app= Flask(__name__)
tokens=[]
response=requests.get('https://api.kite.trade/instruments')
response_array=response.text.split('\n')
for res in response_array:
    res_list=res.split(',')
    if(len(res_list)<12):
        continue
    if(res_list[11]=='NSE' or res_list[11]=='NFO'):
        tokens.append(res_list[0])
# print((tokens))

@app.route('/')
def health_check():
    return 'server up and running'

@app.route('/subscribe')
def subscribe():
    kc.subscribe(tokens)
    return "subscribed"

@app.route('/get_tokens')
def get_tokens():
    kc.get_tokens()
    return "get tokens"

if __name__ == '__main__':
  
    # run() method of Flask class runs the application 
    # on the local development server.
    app.run()
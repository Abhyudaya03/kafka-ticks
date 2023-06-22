import threading
from flask import Flask
from go_kicker_mqtt import KickerClient
from queue import Queue
q=Queue()

kc = KickerClient('34.100.136.129',1883,'gokicker','zztubWDYKRALvfZN',q,['localhost:9092'])
kc.connect_mqtt()
t= threading.Thread(target=kc.start)
t.setDaemon(True)
t.start()

app= Flask(__name__)


@app.route('/')
def health_check():
    return 'server up and running'

@app.route('/subscribe')
def subscribe():
    kc.subscribe([10795266, 11862018, 10826498, 10829314, 10827266, 13172738, 10729730, 9583362, 12729858, 10729474, 9150466, 9151234, 11864066, 10774530, 10827522, 13137154, 10779138, 10847746, 9396738, 10813698, 10829570, 8966914, 10775042, 11864578, 10785282, 10838018, 13166850, 10819330, 10843906, 10800642, 10842114, 9156354, 10786562, 10836226, 13172226, 10774786, 13176578, 10065410, 10777858, 10806530, 10861058, 11847938, 9181186, 10828290, 8967938, 10849538, 11313154, 13177346, 10720258, 10793986, 10838274, 10833410, 11862530, 13175810, 13166082, 10813442, 10792450, 9156098, 10771714, 10827778, 10833666, 13167106, 13172994, 10434306, 13173506, 9152002, 13178370, 13178626, 10819842, 10839298, 9128962, 10801922, 13176834, 10785538, 8983810, 10065154, 10851842, 10894594, 10841346, 10843650, 10838786, 10775554, 10842370, 11847682, 10064642, 10812418, 11803650, 13176066, 13178114, 9362434, 8984322, 11235074, 9150210, 10837762, 9180674, 10777346, 10777602, 10800130, 10802178, 10771970, 10784514, 13174274, 10849026, 9151746, 9501186, 11864322, 13137410, 10720002, 10837506, 10434562, 10803202, 9069826, 10793730, 11802370, 9149954, 10064898, 10894082, 9128194, 10852098, 13175554, 9365250, 11843074, 11312898, 9364482, 9500930, 10778114, 10779394, 10836482, 13165826, 13166338, 13167362, 13176322, 10812162, 10784258, 9362690, 11681026, 11862274, 9179394, 13177602, 10827010, 10800898, 10818562, 10849282, 13175298, 13175042, 10841090, 10839042, 11863810, 10794242, 10837250, 9181442, 9583618, 11680514, 10839554, 10803458, 10818306, 11235330, 13168130, 9128706, 9152258, 10802434, 13173250, 13177090, 10771458, 9151490, 13174018, 9070082, 9396482, 13166594, 13169410, 13174530, 13177858, 10848258, 16611586, 16619010, 16619266, 16670978, 10424578, 10425090, 17338882, 17377538, 17379586, 17383682, 17608194, 17610498, 19029250, 19035138, 19035394, 19080706, 19080962, 19412738, 19412994, 20642562, 11767298, 11767554, 15373826, 20783362, 20783618, 20848642, 20855298, 20855554, 20859906, 23021570, 23026946, 23027202, 23208194, 23208450, 23341314, 23341570, 23600386, 23600642, 10625538, 10626050, 25487106, 25490946, 25491202, 25494530, 26319362, 26355970, 26356226, 42534146, 42534402, 43129090, 43129346, 43624706, 43624962, 43768066, 43768322, 44065282, 44069122, 44069378, 43125250, 10093058])
    return "subscribed"

@app.route('/get_tokens')
def get_tokens():
    kc.get_tokens()
    return "get tokens"

if __name__ == '__main__':
  
    # run() method of Flask class runs the application 
    # on the local development server.
    app.run()
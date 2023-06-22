from mimetypes import init
import json
from paho.mqtt import client as mqtt_client
from kafka_producer import KafkaProducerClass
import threading

token_set=set()

class KickerClient():

    def __init__(self, host, port, username, password, out_queue, kservers):
        self.host=host
        self.port=port
        self.username =username
        self.password = password 
        self.client=mqtt_client.Client()
        self.ticks={}   
        self.out_queue=out_queue
        self.kproducer=KafkaProducerClass(kservers)
        

    def connect_mqtt(self) -> mqtt_client:
        def on_connect(client, userdata, flags, rc):
            if rc==0:
                print("Connected to MQTT Broker!")
                self.subscribe(list(token_set))
            else:
                print("Failed to connect, return code %d\n", rc)


        self.client.username_pw_set(self.username, self.password)
        self.client.on_connect = on_connect
        self.client.connect(self.host, self.port)
        self.client.on_message = self.on_message

    def start(self):
        self.client.loop_forever()

    def subscribe(self, instrument_tokens:list):
        ##publishing instrument token on subscribe channel to trigger kite subscription at the server side
        sub={
            "instrument_tokens":instrument_tokens
        }
        self.publish("subscribe",sub)
        for token in instrument_tokens:
            self.client.subscribe(str(token))

    def publish(self,topic,msg):

        result = self.client.publish(topic, json.dumps(msg))
        # result: [0, 1]
        print(result)
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")

    def get_tokens(self):
        self.client.subscribe('subscribe')

    def thread_kafka(self, msg):
        if(msg.topic=='subscribe'):
                dict_data=json.loads(msg.payload.decode())
                tokens_to_sub=[]
                for token in dict_data['instrument_tokens']:
                    if token not in token_set:
                        tokens_to_sub.append(token)
                token_set.update(dict_data['instrument_tokens'])
                if(len(tokens_to_sub)>0):
                    self.subscribe(tokens_to_sub)
                return
        future=self.kproducer.producer.send(msg.topic, value=msg.payload.decode())
        result=future.get()
        print("kafka result",result)
        # self.ticks['topic']=msg.payload.decode()
        # self.out_queue.put(msg.payload.decode())

    def on_message(self,client, userdata,msg):
            kafka_thread=threading.Thread(target=self.thread_kafka, args=(msg,))
            kafka_thread.start()
            # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
            # future=self.kproducer.producer.send(msg.topic, value=msg.payload.decode())
            # result=future.get()
            # # print("kafka result",result)
            # self.ticks['topic']=msg.payload.decode()
            # self.out_queue.put(msg.payload.decode())
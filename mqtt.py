import datetime
import json

from db import mongoDb
import paho.mqtt.client as mqtt
from env import env

mongo = mongoDb()
db = mongo.db
env = env()

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        payload['timestamp'] = datetime.datetime.fromisoformat(payload['timestamp'][:-6])
        userdata.insert_one(payload)
    except Exception as e:
        print(e)
        
client = mqtt.Client(userdata=db["gps"])
client.on_message = on_message

try:
    client.connect(env.MQTT_HOST, int(env.PORT))
    print(f"Connected to MQTT broker ({env.MQTT_HOST}:{int(env.PORT)})")
    client.subscribe(env.TOPIC)
    client.loop_forever()
except Exception as e:
    print(f"Error while connecting to MQTT : {e}")
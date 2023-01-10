#!/usr/bin/env python3
import re
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import datetime

INFLUXDB_ADDRESS = 'localhost'
INFLUXDB_USER = 'mqtt'
INFLUXDB_PASSWORD = 'mqtt'
INFLUXDB_DATABASE = 'ics_server_metrics'

MQTT_ADDRESS = '' # URL to MQTT broker
MQTT_PORT = 1883
MQTT_USER = ''
MQTT_PASSWORD = ''
MQTT_CLIENT_ID = 'MasterMonitor'
ICS_HOSTS = ["host_1", "host_2"] # list of server expected to be publishing
MQTT_TOPICS = []
for i in ICS_HOSTS:
        topic = (F"/SERVERS/{i}/#",1)
        MQTT_TOPICS.append(topic)

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)

def _parse_mqtt_message(tags, payload):
    memoryTopics = ['tmpfs', 'devtmpfs', 's3fs', 'dev', 'udev']
    topic = [i for i in (tags).split("/") if i.strip()]
    host = topic[1]
    sTopic = topic[2]
    server_data = {}
    server_data['tags'] = {}
    server_data['fields'] = {}
    try:
        #Memory Topics
        if sTopic in memoryTopics:
            try:
                measurement = 'devxvda1' if sTopic == 'dev' or sTopic =='udev' else sTopic
                server_data['measurement'] = measurement
                server_data['tags']['host'] = host
                server_data['fields'][topic[-1]] = float(payload) if re.search('[a-zA-Z]',payload) is None else 0.0
            except:
                server_data = [[]]
        #Memory
        elif sTopic in ['MemFree', 'MemTotal', 'MemAvailable', 'MemPercent']:
            measurement = 'memory'
            field = sTopic[3:]
            server_data['measurement'] = measurement
            server_data['tags']['host'] = host
            server_data['fields'][field] = float(payload)
        # Load
        elif sTopic in ['Load1', 'Load5', 'Load15']:
            measurement = 'cpu_load'
            field = sTopic
            server_data['measurement'] = measurement
            server_data['tags']['host'] = host
            server_data['fields'][field] = float(payload)
        # Comms and RunTime
        elif sTopic in ['GoodComms', 'RunTime']:
            measurement = 'communication' if sTopic == 'GoodComms' else 'runtime'
            field = sTopic
            server_data['measurement'] = measurement
            server_data['tags']['host'] = host
            comms_percent = 100 if measurement == 'communication' else 1
            server_data['fields'][field] = float(0) if payload == 'None' else float(payload)*comms_percent
        else:
            server_data = [{}]
    except Exception as ex:
        server_data = [{}]
    return server_data

def on_connect(client, userdata, flags, rc):
        if rc == 0:
                print("Connected to MQTT Broker")
                client.subscribe(MQTT_TOPICS)
        else:
                print(F"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(msg.topic, msg.payload.decode(encoding="UTF-8", errors="ignore"))
        server_data = _parse_mqtt_message(msg.topic, msg.payload.decode(encoding="UTF-8", errors="ignore"))

        # server_data = msg.topic
        if server_data is not None:
            influxdb_client.write_points([server_data])
            print('saved: ', server_data)

def _init_influxdb_database():
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)


def main():
    _init_influxdb_database()
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_ADDRESS, MQTT_PORT)
    mqtt_client.loop_forever()

if __name__ == '__main__':
    print('MQTT to InfluxDB bridge')
    main()

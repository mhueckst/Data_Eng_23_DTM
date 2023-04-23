#!/usr/bin/env python
import requests
import pytz
from datetime import datetime
from send_slack_msg import send_slack_notification
import sys
import json
from random import choice
from tqdm import tqdm
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer


TRIMET_DATA_URL = "http://www.psudataeng.com:8000/getBreadCrumbData"
DATASTORE_PATH = "/home/dtm-project/data-archive/"

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        #else:
            # print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                #topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


    def retrieve_and_save():
        def get_date_str():
            return datetime.now(tz=pytz.utc).astimezone(pytz.timezone("US/Pacific")).strftime("%Y-%m-%d")
        print("Fetching data...")
        try:
            data = requests.get(TRIMET_DATA_URL).text
            filename = f"{DATASTORE_PATH}{get_date_str()}.json"

            with open(filename, 'w') as writer:
                writer.write(data)
            return f"Today's Trimet data was retrieved and stored into {filename}"
        except:
            return f"There was an issue retrieving or storing today's Trimet data."


    def parse_json(filename):
        with open("data-archive/2023-04-21.json", "r") as read_file:
        # with open(filename, "r") as read_file:
            data = json.load(read_file)
        return data

    def produce_data(data_list):
        #producer.produce(topic, json.dumps(data_list), callback=delivery_callback)
        #producer.flush()
        #return
        length = len(data_list)
        print(length)
        bar = tqdm(total=length)
        for i in range(length):
            if(i % 10000 == 0):
                producer.flush()
            bar.update(1)
            data = json.dumps(data_list[i])
            # print("Data:", data)
            producer.produce(topic, value=data, callback=delivery_callback)
        producer.flush()

        # Block until the messages are sent.   
        # producer.poll(10000)



    # result = retrieve_and_save()
    # send_slack_notification(result)
    # Produce data by selecting random values from these lists.
    topic = "breadcrumbs_readings"
    data_parsed = parse_json(None)
    produce_data(data_parsed)






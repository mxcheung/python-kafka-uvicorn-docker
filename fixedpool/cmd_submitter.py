#!/aac/python/stream-virtualenv/bin/python3

from confluent_kafka import Consumer
from confluent_kafka import Producer
import json
import sys, os
import uuid
import time
import logging



VERBOSE_FMT = ('%(asctime)s %(levelname)s %(name)s %(module)s %(process)d %(thread)d '
                   '%(filename)s_%(lineno)s_%(funcName)s  %(message)s')

def parse_argument():
    import argparse, pathlib
    parser = argparse.ArgumentParser()
    parser.add_argument("-job_id", type=str, help="job id" , default="AU-5010")
    parser.add_argument("-command", type=str, help="Command to run")
    args = parser.parse_args()
    job_id = str(args.job_id)
    command = str(args.command)
    return job_id, command


my_job_id, command = parse_argument()


conf = {
            'bootstrap.servers': 'host01.local:9093,host02.local:9093,host03.local:9093',
            'client.id': 'clientid02',
            'group.id': my_job_id,
            "enable.auto.commit": True,
            "auto.offset.reset": 'earliest',
            "security.protocol": 'ssl',
            "ssl.keystore.location": '/aac/kafka/ca-store/kafka.client.keystore.jks',
            "ssl.keystore.password": 'please_replace',
            "ssl.key.password": 'please_replace',
            "enable.ssl.certificate.verification": False
        }

my_unique_id = str(uuid.uuid4())


logging.basicConfig(filename='/aac/python/logs/danny_submitter.log', 
                    format=VERBOSE_FMT,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
#logging.Formatter(fmt=cls.VERBOSE_FMT)
kafka_topics = 'LIONEL_CMD3'
responseTopic = ['LIONEL_RESPONSE']
running = True

# my_job_id = "1234"
my_unique_id = str(uuid.uuid4())
payload = {}
payload['job_id']=my_job_id
payload['unique_id']=my_unique_id
#payload['command']= f'/aac/python/scripts/hello8.py >> /aac/python/logs/DEPENDS_SYD_6011.log'
payload['command']= command
consumer_initialised = False



def kafka_setup():
    global consumer, producer
    # Instantiate
    consumer = Consumer(conf)
    producer = Producer(conf)

def on_assign(consumer, partitions):
    global consumer_initialised
    logging.info('Partitions assigned: {}'.format(partitions))
    consumer_initialised = True

def initialise_consumer(consumer, topic):
    global consumer_initialised
    consumer.subscribe(topic, on_assign=on_assign)
    while not consumer_initialised:
        msg = consumer.poll(timeout=1.0)

def basic_consume_loop(consumer, topic):
    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    except KeyboardInterrupt:
        shutdown()            
    finally:
        logging.info("Close consumer.")
        consumer.close()

def msg_process(msg):
    global running
  #  response_json_string  = json.loads( msg.value())
    job_id = payload['job_id']
    unique_id = payload['unique_id']
    logging.info('Received Job ID: {}, Unique ID: {}, Response {}'.format(job_id, unique_id, msg.value()))
    if (job_id == my_job_id) and (unique_id == my_unique_id):
       logging.info('Found my Job ID: {}, Unique ID: {} Response {}'.format(job_id, unique_id, msg.value()))
       logging.info('Return exit code')
       running = False

def submit(payload, producer):
    json_payload = json.dumps(payload).encode('utf-8')
    logging.info(json_payload)
    logging.info('Submitting Job ID: {}, Payload {}'.format(payload['job_id'], json_payload))
    producer.produce(kafka_topics, key=payload['job_id'], value=json_payload)
    producer.flush()

def shutdown():
    logging.info("Shutdown, goodbye!")
    running = False
    
if __name__ == "__main__":    
    kafka_setup()
    initialise_consumer(consumer, responseTopic)
    submit(payload,producer)
    basic_consume_loop(consumer, responseTopic)

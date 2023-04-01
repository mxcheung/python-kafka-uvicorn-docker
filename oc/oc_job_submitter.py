#!/aac/python/stream-virtualenv/bin/python3

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
    parser.add_argument("-job_id", type=str, help="job id" , default="SYD-9999")
    parser.add_argument("-command", type=str, help="Command to run")
    args = parser.parse_args()
    job_id = str(args.job_id)
    command = str(args.command)
    return job_id, command


my_job_id, command = parse_argument()



conf = {
            'bootstrap.servers': 'localhost0`:9093,localhost02:9093,localhost03:9093',
            'client.id': 'sgvlapaacudep02',
            "security.protocol": 'ssl',
            "ssl.keystore.location": '/aac/kafka/ca-store/kafka.client.keystore.jks',
            "ssl.keystore.password": 'please_replace',
            "ssl.key.password": 'please_replace',
            "enable.ssl.certificate.verification": False
        }


logging.basicConfig(filename='/aac/python/logs/oc_job_submitter.log', 
                    format=VERBOSE_FMT,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
#logging.Formatter(fmt=cls.VERBOSE_FMT)
kafka_topics = 'JOB_CMD'

# my_job_id = "6011"
my_unique_id = str(uuid.uuid4())
payload = {}
payload['job_id']=my_job_id
payload['country']=my_country
payload['unique_id']=my_unique_id
#payload['command']= f'/aac/python/scripts/hello8.py >> /aac/python/logs/6011.log'
payload['command']= command

def kafka_setup():
    global producer
    # Instantiate
    producer = Producer(producerconf)


def submit(payload, producer):
    json_payload = json.dumps(payload).encode('utf-8')
    logging.info(json_payload)
    logging.info('Submitting Job ID: {}, Payload {}'.format(payload['job_id'], json_payload))
    producer.produce(kafka_topics, key=payload['job_id'], value=json_payload)
    producer.flush()
    
if __name__ == "__main__":    
    kafka_setup()
    submit(payload,producer)
    sys.exit(3)

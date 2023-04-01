#!/aac/python/stream-virtualenv/bin/python3

from confluent_kafka import Consumer
import subprocess
import json
import sys, os
import uuid
import time
import logging

# declare constants 
EXIT_CODE_0 = 0
EXIT_CODE_2 = 2

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

conf = {
            'bootstrap.servers': 'broker01:9093,broker02:9093,broker03:9093',
            'client.id': 'myclientid',
            'group.id': 'oc_job_monitor',
            "enable.auto.commit": True,
            "auto.offset.reset": 'earliest',
            "security.protocol": 'ssl',
            "ssl.keystore.location": '/aac/kafka/ca-store/kafka.client.keystore.jks',
            "ssl.keystore.password": 'please_replace',
            "ssl.key.password": 'please_replace',
            "enable.ssl.certificate.verification": False
        }

my_job_id, command = parse_argument()
my_unique_id = str(uuid.uuid4())


logging.basicConfig( 
                    format=VERBOSE_FMT,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
#logging.Formatter(fmt=cls.VERBOSE_FMT)
kafka_topics = 'JOB_CMD'
responseTopic = ['JOB_RESPONSE']
running = True

consumer_initialised = False

def kafka_setup():
    global consumer
    # Instantiate
    consumer = Consumer(conf)

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


def generate_control_cmd(payload):
    returncode = payload['returncode']
    country, jobnum = payload['job_id'].split("-")
    if (returncode == EXIT_CODE_0):
        status = "SUCCESS"
    elif (returncode == EXIT_CODE_2):
        status = "NONE"
    else:       
        status = "FAILED"  
    command = f'/fcs/scripts/batch_process/control.pl -f A01 -e {country} -x COMPLETE -s {status} -p {jobnum}'
    if returncode not in [EXIT_CODE_0,EXIT_CODE_2]:
        error_msg = payload['stderr'] .replace('"'," ")
        command = command + f' -E "{error_msg}"'
    # logging.info('Job Id: {}, returncode: {}, status: {}'.format(payload['job_id'], returncode, status))
    #logging.info('Job ID: {}, command : {}'.format(payload['job_id'], command))
    return command        
        
def msg_process(msg):
    global running, exitcode
  #  response_json_string  = json.loads( msg.value())
   # logging.info('msg.value(): {}'.format(msg.value()))
    try:
        payload  = json.loads( msg.value())
        command = generate_control_cmd(payload)
        result = subprocess.run(command, shell=True)
        logging.info('Job ID: {}, Exit code of command {}: {}'.format(job_id, command, result.returncode))
    
    except Exception as e:
        logging.info("Exception processing message.")
        logging.info(e)
    finally:
        dummy = 1
      #  logging.info("End message process.")



def shutdown():
    logging.info("Shutdown, goodbye!")
    running = False
    
if __name__ == "__main__":    
    kafka_setup()
    initialise_consumer(consumer, responseTopic)
    basic_consume_loop(consumer, responseTopic)

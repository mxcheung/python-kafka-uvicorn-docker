#!/aac/python/stream-virtualenv/bin/python3

from confluent_kafka import Consumer
import subprocess
import json
import sys, os
import uuid
import time
import logging



VERBOSE_FMT = ('%(asctime)s %(levelname)s %(name)s %(module)s %(process)d %(thread)d '
                   '%(filename)s_%(lineno)s_%(funcName)s  %(message)s')



conf = {
            'bootstrap.servers': 'broker01:9093,broker02:9093,broker03:9093',
            'client.id': 'sgvlapaacudep02',
            'group.id': my_job_id,
            "enable.auto.commit": True,
            "auto.offset.reset": 'earliest',
            "security.protocol": 'ssl',
            "ssl.keystore.location": '/aac/java/rje2kafka/ca-store/kafka.client.keystore.jks',
            "ssl.keystore.password": 'clientpass',
            "ssl.key.password": 'clientpass',
            "enable.ssl.certificate.verification": False
        }




logging.basicConfig( 
                    format=VERBOSE_FMT,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
#logging.Formatter(fmt=cls.VERBOSE_FMT)
responseTopic = ['JOB_RESPONSE']
running = True

consumer_initialised = False
exitcode=2


def kafka_setup():
    global consumer
    # Instantiate
    consumer = Consumer(conf)

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
    global running, exitcode
  #  response_json_string  = json.loads( msg.value())
   # logging.info('msg.value(): {}'.format(msg.value()))
    try:
        payload  = json.loads( msg.value())
        job_id = payload['job_id']
        country = payload['country']
        unique_id = payload['unique_id']
        returncode = payload['returncode']
        stderr = payload['stderr']  
    #    logging.info('Received Job ID: {}, Unique ID: {}, Response {}'.format(job_id, unique_id, msg.value()))
    #    if (job_id == my_job_id) and (unique_id == my_unique_id):
    #        logging.info('Found my Job ID: {}, Unique ID: {} Response {}'.format(job_id, unique_id, msg.value()))
     #       logging.info('Return exit code: {}'.format(returncode))
        command = f'/scripts/batch_process/complete.pl -f XXX -e {country} -x COMPLETE -s NONE -p {job_id}'
    #    logging.info('Job ID: {}, Command: {}'.format(job_id, command))
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
    basic_consume_loop(consumer, responseTopic)
    sys.exit(exitcode)

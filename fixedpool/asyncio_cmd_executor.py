#!/aac/python/stream-virtualenv/bin/python3

import logging
import functools
import contextvars

import json
import subprocess
import asyncio
import uvicorn

from asyncio import events
from fastapi import APIRouter, FastAPI
from typing import Optional
import sys, os

from confluent_kafka import Consumer
from confluent_kafka import Producer

VERBOSE_FMT = ('%(asctime)s %(levelname)s %(name)s %(module)s %(process)d %(thread)d '
                   '%(filename)s_%(lineno)s_%(funcName)s  %(message)s')


conf = {
            'bootstrap.servers': 'host01.local:9093,host02.local:9093,host03.local:9093',
            'client.id': 'client01',
            'group.id': 'foo1202303095',
            "enable.auto.commit": True,
            "auto.offset.reset": 'earliest',
            "security.protocol": 'ssl',
            "ssl.keystore.location": '/aac/kafka/ca-store/kafka.client.keystore.jks',
            "ssl.keystore.password": 'please_replace',
            "ssl.key.password": 'please_replace',
            "enable.ssl.certificate.verification": False
        }

sys.path.insert(1, os.path.realpath(os.path.pardir))

app = FastAPI(
    title="demo",
    description="Dummy backend",
    version="0.0.1",
    contact={"name": "Blah", "email": "Blah@some.com"},
)

# Define the filter
class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.args and len(record.args) >= 3 and record.args[2] != "/health"

logging.basicConfig(format=VERBOSE_FMT,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)


# Add filter to the logger
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

kafka_topics = ['JOB_CMD']
responseTopic = 'JOB_RESPONSE'
running = True

consumer = None
producer = None

router = APIRouter()

@app.on_event("startup")
async def startup_event():
    # Start the Kafka consumer process
    print("FastAPI startup.")
    loop = asyncio.get_event_loop()
    loop.create_task(to_thread(kafka_consumer))

@app.on_event("shutdown")
def shutdown_event():
    global running, consumer, producer

    print("Shutdown, goodbye!")
    running = False
    consumer.close()   
    producer.close()
    
@app.get("/")
async def root():
    return {"message": "Hello! welcome to the Small Query!"}

@app.get("/health")
async def health():
    return {"message": "I'm ok!"}

async def to_thread(func, /, *args, **kwargs):
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)

def kafka_consumer():
    global consumer, producer
    # Instantiate
    consumer = Consumer(conf)
    producer = Producer(conf)
    basic_consume_loop(consumer, kafka_topics, producer)

def basic_consume_loop(consumer, topic, producer):
    try:
        consumer.subscribe(topic)

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
                msg_process(msg, producer)
    except KeyboardInterrupt:
        shutdown()            
    finally:
        # Close down consumer to commit final offsets.
        print("Close down consumer to commit final offsets.")
        consumer.close()

def shutdown():
    print("Shutdown, goodbye!")
    running = False

def msg_process(msg, producer):
    payload  = json.loads( msg.value())
    job_id = payload['job_id']
    unique_id = payload['unique_id']
    command = payload['command']    
    print(payload)
    result = subprocess.run(command, shell=True)
    logging.info('Job ID: {}, Exit code of command {}: {}'.format(job_id, command, result.returncode))
#    response_payload = {}
#    response_payload['job_id'] = job_id
#    response_payload['command'] = command
#    response_payload['returncode'] = result.returncode

    response_json_string = 'Job ID: {}, Unique ID: {}, Exit code of command {}: {}'.format(job_id, unique_id,  command, result.returncode)
    print('Job ID: {}, Response {}'.format(job_id, response_json_string))
#    producer.produce(responseTopic, key="key", value=response_json_string.encode('utf-8'))
 #   producer.produce(responseTopic, key="key", value=response_json_string.encode('utf-8'))

    payload['returncode']  = result.returncode

    json_payload = json.dumps(payload).encode('utf-8')
    producer.produce(responseTopic, key=payload['job_id'], value=json_payload)    
    producer.flush()
	
if __name__ == "__main__":
     # Start the FastAPI server
     uvicorn.run(app, host='0.0.0.0', port=8080)

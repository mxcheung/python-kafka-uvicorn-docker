#!/aac/python/stream-virtualenv/bin/python3

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

conf = {
            'bootstrap.servers': 'kafka01.windmill.local:9093,kafka02.windmill.local:9093,kafka03.windmill.local:9093',
            'client.id': 'myclientid02',
            'group.id': 'foo1202303095',
            "enable.auto.commit": True,
            "auto.offset.reset": 'earliest',
            "security.protocol": 'ssl',
            "ssl.keystore.location": '/kafka/ca-store/kafka.client.keystore.jks',
            "ssl.keystore.password": 'please_replace',
            "ssl.key.password": 'please_replace',
            "enable.ssl.certificate.verification": False
        }

sys.path.insert(1, os.path.realpath(os.path.pardir))

app = FastAPI(
    title="demo",
    description="Dummy backend",
    version="0.0.1",
    contact={"name": "Blah", "email": "Blah@somewhere.com"},
)

kafka_topics = ['KAFKA_CMD']
responseTopic = 'KAFKA_RESPONSE'
running = True

consumer = None
producer = None
router = APIRouter()

@app.on_event("startup")
async def startup_event():
    # Start the Kafka consumer process
    loop = asyncio.get_event_loop()
    loop.create_task(asyncio.to_thread(kafka_consumer))

@app.on_event("shutdown")
def shutdown_event():
    global running 
    print("Shutdown, goodbye!")
    running = False
    consumer.close()   
    producer.close()

@app.get("/")
async def root():
    return {"message": "Hello! welcome to the Kafka!"}

@app.get("/health")
async def health():
    return {"message": "I'm ok!"}

async def to_thread(func, /, *args, **kwargs):
    """Asynchronously run function *func* in a separate thread.
    Any *args and **kwargs supplied for this function are directly passed
    to *func*. Also, the current :class:`contextvars.Context` is propagated,
    allowing context variables from the main thread to be accessed in the
    separate thread.
    Return a coroutine that can be awaited to get the eventual result of *func*.
    """
    loop = events.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)

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

def msg_process(msg, producer):
    payload  = json.loads( msg.value())
    job_id = payload['job_id']
    command = payload['command']    
    print(payload)
    result = subprocess.run(command, shell=True)
    print('Job ID: {}, Exit code of command {}: {}'.format(job_id, command, result.returncode))
    response_json_string = 'Job ID: {}, Exit code of command {}: {}'.format(job_id, command, result.returncode)
    producer.produce(responseTopic, key="key", value=response_json_string.encode('utf-8'))
    producer.flush()

		
if __name__ == "__main__":
     # Start the FastAPI server
     uvicorn.run(app, host='127.0.0.1', port=8000)

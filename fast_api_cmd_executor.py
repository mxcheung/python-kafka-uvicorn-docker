#!/aac/python/stream-virtualenv/bin/python3

import json
import subprocess
import asyncio
import uvicorn

from fastapi import APIRouter, FastAPI
from typing import Optional
from multiprocessing import Process
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

router = APIRouter()

@app.get("/")
async def root():
    return {"message": "Hello! welcome to the Kafka!"}

@app.get("/health")
async def health():
    return {"message": "I'm ok!"}

    
kafka_topics = ['KAFKA_CMD']
responseTopic = 'KAFKA_RESPONSE'
running = True

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
    command = payload['command']    
    print(payload)
    result = subprocess.run(command, shell=True)
    print('Job ID: {}, Exit code of command {}: {}'.format(job_id, command, result.returncode))
    response_json_string = 'Job ID: {}, Exit code of command {}: {}'.format(job_id, command, result.returncode)
    producer.produce(responseTopic, key="key", value=response_json_string.encode('utf-8'))
    producer.flush()

def kafka_consumer():


    # Instantiate
    consumer = Consumer(conf)
    producer = Producer(conf)
    basic_consume_loop(consumer, kafka_topics, producer)
    consumer.close()   

def main():
    print("main....")
		
if __name__ == "__main__":
     consumer_process = Process(target=kafka_consumer)  
     consumer_process.start()
     
     # Start the FastAPI server
     uvicorn.run(app, host="0.0.0.0", port=8080)

     # Wait for the kafka consumer process to exit
     consumer_process.join()

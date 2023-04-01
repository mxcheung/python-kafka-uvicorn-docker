# python-kafka-uvicorn-docker


# Confluent Kafka
https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/asyncio_example.py

# Job submitter and completed job 
oc_job_submitter.py
oc_completed_job.py

#Issues

https://github.com/confluentinc/librdkafka/issues/1674
rd_kafka_destroy hangs for consumer when called after a fork #2374

<code>
INFO:     Shutting down
INFO:     Waiting for application shutdown.
Shutdown, goodbye!    Close down consumer to commit final offsets.
    %3|1678495022.741|DESTROY|sgvlapaacudep02#consumer-1| [thrd:app]: Failed to join internal main thread: Success (was process forked?)
    *** rdkafka_queue.h:211:rd_kafka_q_destroy0: assert: rkq->rkq_refcnt > 0 ***
</code>

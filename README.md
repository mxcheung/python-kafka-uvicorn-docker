# python-kafka-uvicorn-docker


# Confluent Kafka
https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/asyncio_example.py

# Job submitter and completed job 

 - Event driven job submitter will submit job and wait
 - Completed jop wil listen to completed job events and mark job as SUCCESS or FAILURE

- i. oc_job_submitter.py submit job to [job queue]

- ii. oc_job_executor.py listen to [job queue] and write return code to [response queue]
     
- iii. oc_completed_job.py listen  to [response queue] and mark job as complete based on result.

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

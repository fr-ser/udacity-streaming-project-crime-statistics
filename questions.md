## 1) How did changing values on the SparkSession property parameters affect

The throughput and latency of the data?

The following items are not all technically SparkSession properties, but also stream settings.

- In theory managing parallelism has a significant effect - not on a single PC, single partition
  docker Kafka setup, though. This can be achieved by managing Spark and Kafka partitions with
  `minPartitions` (Kafka topics should also be appropriately partitioned)
- Depending on the producer setting the batch size `maxOffsetsPerTrigger` and polling interval
  `kafkaConsumer.pollTimeoutMs` make a significant difference
- the above options are also dependent on the trigger. For this use case no trigger interval
  (immediate execution) has been used
- Resources such as cpu `spark.task.cpus` and memory `spark.executor.memory` are also important.

## 2) What were the 2-3 most efficient SparkSession property key/value pairs?

Through testing multiple variations on values, how can you tell these were the most optimal?

- optimal in the end was a batch of 6,000 with a checking intervall of 3 seconds.
  But only as this matched the speed of my producer.

- To see differences in performance I looked at the terminal progress report. The results are of
  course heavily dependent on the execution environment (my computer) and the payload (e.g. size)
  and source (e.g. number of messages produced per second) itself.

  Looking at the `processedRowsPerSecond` paints a clear picture. Of course the increased batch
  also incrased the `memoryUsedBytes`, but the tradeoff is worth it.

For a batch of 200 with otherwise default settings:

```json
{
  "batchId": 4,
  "durationMs": {
    "addBatch": 6818,
    "getBatch": 1,
    "getEndOffset": 0,
    "queryPlanning": 99,
    "setOffsetRange": 4,
    "triggerExecution": 6995,
    "walCommit": 42
  },
  ...
  "inputRowsPerSecond": 27.886224205242613,
  ...
  "numInputRows": 200,
  "processedRowsPerSecond": 28.591851322373124,
  ...
  "sources": [
    {
      ...
      "inputRowsPerSecond": 27.886224205242613,
      "numInputRows": 200,
      "processedRowsPerSecond": 28.591851322373124,
      ...
    }
  ],
  "stateOperators": [
    {
      ...
      "memoryUsedBytes": 165463,
      "numRowsTotal": 325,
      "numRowsUpdated": 117
    }
  ],
  "timestamp": "2020-02-22T09:51:52.544Z"
}

```

For batch the optimized batch:

```json
{
  "batchId": 4,
  "durationMs": {
    "addBatch": 6974,
    "getBatch": 1,
    "getEndOffset": 1,
    "queryPlanning": 58,
    "setOffsetRange": 4,
    "triggerExecution": 7121,
    "walCommit": 47
  },
  ...
  "inputRowsPerSecond": 683.2795506155132,
  ...
  "numInputRows": 5717,
  "processedRowsPerSecond": 802.836680241539,
  ...
  "sources": [
    {
      ...
      "inputRowsPerSecond": 683.2795506155132,
      "numInputRows": 5717,
      "processedRowsPerSecond": 802.836680241539,
      ...
    }
  ],
  "stateOperators": [
    {
      ...
      "memoryUsedBytes": 801943,
      "numRowsTotal": 2560,
      "numRowsUpdated": 1002
    }
  ],
  "timestamp": "2020-02-22T09:55:06.008Z"
}
```

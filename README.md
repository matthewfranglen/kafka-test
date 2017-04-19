Kafka Test
==========

Sets up kafka with some readers and writers.

```
./mvnw clean package && ( docker-compose stop ; docker-compose rm -f ; docker-compose run java )
```

Tests
-----

This tests the writing of messages to kafka, and how lag builds up based on that.

This performs the following steps:

 * Write to the partitions

 * Start consuming from some partitions

 * Drain all partitions

 * Confirm all written messages received

This will print the partition lag at each stage.

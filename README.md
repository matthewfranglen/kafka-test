Kafka Test
==========

Sets up kafka with some readers and writers.

```
./mvnw clean package && ( docker-compose stop ; docker-compose rm -f ; docker-compose up )
```

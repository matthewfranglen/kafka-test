package com.matthew.services;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class Writer {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public Writer(
            String server,
            String topic
    ) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", server);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        producer = new KafkaProducer<>(properties);
        this.topic = topic;
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<List<RecordMetadata>> write(Collection<String> messages) throws InterruptedException {
        CompletableFuture<RecordMetadata>[] result = (CompletableFuture<RecordMetadata>[])messages.stream()
            .map(this::toRecord)
            .map(this::send)
            .toArray(size -> new CompletableFuture[size]);

        return CompletableFuture.allOf(result)
            .thenApply(v -> Arrays.stream(result)
                    .map(this::getQuietly)
                    .collect(toList())
                );
    }

    private ProducerRecord<String, String> toRecord(String message) {
        return new ProducerRecord<>(topic, null, message);
    }

    private CompletableFuture<RecordMetadata> send(ProducerRecord<String, String> message) {
        CompletableFuture<RecordMetadata> result = new CompletableFuture<>();

        producer.send(message, (record, exception) -> {
            if (exception == null) {
                result.complete(record);
            } else {
                result.completeExceptionally(exception);
            }
        });

        return result;
    }

    private <T> T getQuietly(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

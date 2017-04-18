package com.matthew.services;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Writer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(Writer.class);

    private final KafkaProducer<K, V> writer;
    private final ProducerRecord<K, V> message;

    public Writer(KafkaProducer<K, V> writer, String topic, V message) {
        this.writer = writer;
        this.message = new ProducerRecord<>(topic, null, message);
    }

    public void run(int count, long delay) {
        try {
            while (true) {
                write(count);

                Thread.sleep(delay);
            }
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

    private void write(int count) throws InterruptedException {
        Map<Integer, Long> offsets = new HashMap<>();
        IntStream.range(0, count)
            .mapToObj(v -> message)
            .map(writer::send)
            .collect(Collectors.toList())
            .stream()
            .filter(this::hasValue)
            .map(this::getValue)
            .forEach(metadata -> {
                if (offsets.getOrDefault(metadata.partition(), 0L) < metadata.offset()) {
                    offsets.put(metadata.partition(), metadata.offset());
                }
            });

        logger.info("{} - wrote {}: {}", Thread.currentThread().getName(), count, offsets);
    }

    private <T> boolean hasValue(Future<T> future) {
        try {
            future.get();

            return true;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e) {
            logger.warn("{} - failed to write message", Thread.currentThread().getName(), e);
        }

        return false;
    }

    private <T> T getValue(Future<T> future) {
        try {
            return future.get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

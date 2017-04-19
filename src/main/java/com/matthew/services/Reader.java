package com.matthew.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Reader {

    private final KafkaConsumer<String, String> consumer;
    private final Queue<String> messages;

    public Reader(
            String server,
            String topic,
            String consumerGroup
    ) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", server);
        properties.put("group.id", consumerGroup);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("max.poll.interval.ms", "60000");
        properties.put("max.poll.records", "1");

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        messages = new LinkedList<>();
    }

    public void commit() {
        consumer.commitSync();
    }

    public CompletableFuture<List<String>> drain(ThreadPoolExecutor executor) {
        return CompletableFuture.supplyAsync(this::drain, executor);
    }

    private List<String> drain() {
        int emptyReadCount = 0;

        while (emptyReadCount < 10) {
            if (! read()) {
                emptyReadCount++;
            }
        }
        commit();

        List<String> result = new ArrayList<>();
        String value;

        while ((value = messages.poll()) != null) {
            result.add(value);
        }

        return result;
    }

    private boolean read() {
        ConsumerRecords<String, String> records = consumer.poll(100);

        if (records.isEmpty()) {
            return false;
        }

        StreamSupport.stream(records.spliterator(), false)
            .map(ConsumerRecord::value)
            .forEach(messages::add);

        return true;
    }

}

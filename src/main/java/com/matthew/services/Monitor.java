package com.matthew.services;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Monitor {

    private final KafkaConsumer<?, ?> consumer;
    private final String topic;

    public Monitor(
            String server,
            String topic,
            String consumerGroup
    ) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", server);
        properties.put("group.id", consumerGroup);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        this.topic = topic;
    }


    public Map<Integer, Long> getPartitionLag() {
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
            .map(partition -> new TopicPartition(topic, partition.partition()))
            .collect(toList());
        Map<Integer, Long> endOffsets = consumer.endOffsets(partitions).entrySet().stream()
            .collect(toMap(e -> e.getKey().partition(), Map.Entry::getValue));
        Map<Integer, Long> committedOffsets = partitions.stream()
            .collect(toMap(TopicPartition::partition, this::getConsumerOffset));

        return endOffsets.entrySet().stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue() - committedOffsets.getOrDefault(e.getKey(), 0L)));
    }

    private long getConsumerOffset(TopicPartition partition) {
        return Optional.ofNullable(consumer.committed(partition))
            .map(OffsetAndMetadata::offset)
            .orElse(0L);
    }

}

package com.matthew;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.matthew.services.Reader;
import com.matthew.services.Writer;

@Configuration
public class KafkaTestConfiguration {

    public static final String MESSAGE = "The Magic Words are Squeamish Ossifrage";

    @Bean
    @Scope("prototype")
    public Writer<?, String> producer(
            @Value("${kafka.server}") String server,
            @Value("${kafka.topic}") String topic
    ) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", server);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        return new Writer<>(new KafkaProducer<>(properties), topic, MESSAGE);
    }

    @Bean
    @Scope("prototype")
    public Reader<?, String> consumer(
            @Value("${kafka.server}") String server,
            @Value("${kafka.topic}") String topic,
            @Value("${kafka.consumerGroup}") String consumerGroup
    ) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", server);
        properties.put("group.id", consumerGroup);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<?, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        return new Reader<>(consumer);
    }

    @Bean
    @Scope("prototype")
    public ThreadPoolTaskExecutor pool() {
        ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
        pool.setCorePoolSize(4);
        pool.setMaxPoolSize(4);

        return pool;
    }

}

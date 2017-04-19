package com.matthew;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.matthew.services.Monitor;
import com.matthew.services.Reader;
import com.matthew.services.Writer;

@Configuration
public class KafkaTestConfiguration {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Bean
    public Writer writer(
            @Value("${kafka.server}") String server,
            @Value("${kafka.topic}") String topic
    ) {
        logger.info("Creating Writer: {}, {}", server, topic);

        return new Writer(server, topic);
    }

    @Bean
    public Reader consumer(
            @Value("${kafka.server}") String server,
            @Value("${kafka.topic}") String topic,
            @Value("${kafka.consumerGroup}") String consumerGroup
    ) {
        logger.info("Creating Reader: {}, {}, {}", server, topic, consumerGroup);

        return new Reader(server, topic, consumerGroup);
    }

    @Bean
    public Monitor monitor(
            @Value("${kafka.server}") String server,
            @Value("${kafka.topic}") String topic,
            @Value("${kafka.consumerGroup}") String consumerGroup
    ) {
        logger.info("Creating Monitor: {}, {}, {}", server, topic, consumerGroup);

        return new Monitor(server, topic, consumerGroup);
    }

    @Bean
    public ThreadPoolExecutor executor() {
        return new ThreadPoolExecutor(4, 4, 1_000, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    }

}

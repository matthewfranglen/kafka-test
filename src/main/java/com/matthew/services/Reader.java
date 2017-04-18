package com.matthew.services;

import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reader<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(Reader.class);

    private final KafkaConsumer<K, V> reader;

    public Reader(KafkaConsumer<K, V> reader) {
        this.reader = reader;
    }

    public void run(int count, long delay) {
        while (true) {
            read(count);

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
    }

    private void read(int count) {
        IntStream.range(0, count).forEach(reader::poll);

        logger.info("{} - read {} messages", Thread.currentThread().getName(), count);
    }

}

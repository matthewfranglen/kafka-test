package com.matthew.services;

import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Writer<K, V> {

    private final KafkaProducer<K, V> writer;
    private final ProducerRecord<K, V> message;

    public Writer(KafkaProducer<K, V> writer, String topic, V message) {
        this.writer = writer;
        this.message = new ProducerRecord<>(topic, null, message);
    }

    public void run(int count, long delay) {
        while (true) {
            write(count);

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
    }

    private void write(int count) {
        IntStream.range(0, count)
            .mapToObj(v -> message)
            .forEach(writer::send);
    }

}

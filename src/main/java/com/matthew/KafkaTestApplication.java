package com.matthew;

import java.util.stream.IntStream;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.matthew.services.Reader;
import com.matthew.services.Writer;

@SpringBootApplication
public class KafkaTestApplication implements ApplicationContextAware, CommandLineRunner {

    private ApplicationContext context;

    public static void main(String[] args) {
        SpringApplication.run(KafkaTestApplication.class, args);
    }

    public void setApplicationContext(ApplicationContext context) {
        this.context = context;
    }

    public void run(String... args) {
        makeConsumers();
        makeProducers();
    }

    private void makeConsumers() {
        ThreadPoolTaskExecutor pool = getPool();

        IntStream.range(1, 5)
            .mapToObj(this::toConsumer)
            .forEach(pool::execute);
    }

    private void makeProducers() {
        ThreadPoolTaskExecutor pool = getPool();

        IntStream.range(1, 5)
            .mapToObj(this::toProducer)
            .forEach(pool::execute);
    }

    private ThreadPoolTaskExecutor getPool() {
        return (ThreadPoolTaskExecutor) context.getBean("pool");
    }

    private Runnable toConsumer(int v) {
        Reader<?, ?> reader = (Reader<?, ?>) context.getBean("consumer");

        return () -> reader.run(v * 10, 10_000 - (v * 1_000));
    }

    private Runnable toProducer(int v) {
        Writer<?, ?> writer = (Writer<?, ?>) context.getBean("producer");

        return () -> writer.run(v * 10, 10_000 - (v * 1_000));
    }

}

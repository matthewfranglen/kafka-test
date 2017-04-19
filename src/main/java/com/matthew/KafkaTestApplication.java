package com.matthew;

import static java.util.stream.Collectors.toList;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.matthew.services.Monitor;
import com.matthew.services.Reader;
import com.matthew.services.Writer;

@SpringBootApplication
public class KafkaTestApplication implements CommandLineRunner {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Writer writer;

    @Autowired
    private List<Reader> readers;

    @Autowired
    private Monitor monitor;

    @Autowired
    private ThreadPoolExecutor executor;

    public static void main(String[] args) {
        SpringApplication.run(KafkaTestApplication.class, args);
    }

    public void run(String... args) throws Exception {
        executor.execute(this::run);
    }

    private void run() {
        try {
            Thread.sleep(1_000);

            List<String> messages = makeMessages();

            printLag();

            logger.info("Reading all old messages...");
            drainAll().get();
            printLag();

            logger.info("Writing messages...");
            writeMessages(messages).get();
            printLag();

            logger.info("Read from first partition...");
            List<String> firstReadMessages = drainFirstPartition().get();
            printLag();

            logger.info("Read from all partitions...");
            List<String> remainingMessages = drainRemainingPartitions().get();
            printLag();

            logger.info("Test messages...");
            compareMessages(messages, firstReadMessages, remainingMessages);

            logger.info("Test passed!");
            System.exit(0);
        } catch (Exception e) {}
    }

    private List<String> makeMessages() {
        return IntStream.range(0, 10)
                .mapToObj(v -> String.format("Message - %05d", v))
                .sorted()
                .collect(toList());
    }

    private void printLag() {
        logger.info("Lag: {}", monitor.getPartitionLag());
    }

    private CompletableFuture<List<RecordMetadata>> writeMessages(List<String> messages) throws InterruptedException {
        return writer.write(messages);
    }

    private CompletableFuture<List<String>> drainAll() {
        return drain(readers.stream());
    }

    private CompletableFuture<List<String>> drainFirstPartition() {
        return readers.get(0).drain(executor);
    }

    private CompletableFuture<List<String>> drainRemainingPartitions() {
        return drain(readers.stream().skip(1));
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<List<String>> drain(Stream<Reader> readers) {
        CompletableFuture<List<String>>[] futures = readers
            .map(reader -> reader.drain(executor))
            .toArray(size -> new CompletableFuture[size]);

        if (futures.length == 0) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return CompletableFuture.allOf(futures)
            .thenApply(v ->
                Arrays.stream(futures)
                    .map(this::getQuietly)
                    .map(Collection.class::cast)
                    .flatMap(Collection::stream)
                    .map(String.class::cast)
                    .collect(toList())
            );
    }

    private void compareMessages(Collection<String> expected, List<String> first, List<String> remaining) {
        Collection<String> combined = Stream.of(first, remaining)
            .flatMap(Collection::stream)
            .sorted()
            .collect(toList());

        assertThat(combined, equalTo(expected));
    }

    private <T> T getQuietly(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

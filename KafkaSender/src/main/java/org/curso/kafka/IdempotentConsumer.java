package org.curso.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.curso.kafka.service.ThreadConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IdempotentConsumer {
    public static final int numberOfConsumers = 5;



    public static void main(String[] args) {

        // Consumer properties

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "temp-event-group");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("isolation.level", "read_committed");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        ExecutorService executor = Executors.newFixedThreadPool(numberOfConsumers);

        // Generate consumers in a multithread environment
        for (int i=0; i<numberOfConsumers; i++) {
            ThreadConsumer consumer = new ThreadConsumer(new KafkaConsumer<String, String>(props));
            executor.execute(consumer);
        }
        while(!executor.isTerminated());
    }

}
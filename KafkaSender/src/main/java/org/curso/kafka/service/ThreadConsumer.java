package org.curso.kafka.service;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import org.slf4j.Logger;

import org.slf4j.LoggerFactory;


/**
 * ThreadConsumer
 *
 * @author: Rafael Hernamperez
 */

public class ThreadConsumer extends Thread {

    private final KafkaConsumer<String, String> consumer;

    private final String eventTopic = "test";

    private boolean closed = false;

    private Logger log = LoggerFactory.getLogger(ThreadConsumer.class);


    // Constructor

    public ThreadConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }


    // Main thread execution

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(eventTopic));
        try {
            while (!closed) {
                ConsumerRecords<String, String> events = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, String> event : events) {
                    log.info("--- Partition={}, Offset={}, key={}, value={}",
                            event.partition(),
                            event.offset(),
                            event.key(),
                            event.value());
                    consumer.commitAsync();
                }
            }
        } catch (Exception we) {
            if (!closed) {
                throw we;
            }
        } finally {
            consumer.close();
        }
    }
} // class
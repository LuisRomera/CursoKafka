package org.curso.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * IdempotentProducer example
 */
public class IdempotentProducer {

    private static final Logger log = LoggerFactory.getLogger(IdempotentProducer.class);
    private static final int numberOfSensors = 10;
    private static final int numberOfEvents = 1000;
    private static final String eventTopic = "test";


    public static void main(String[] args) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "transactional-producer");
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // enable idempotence
        producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-id"); // set transaction id
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> producer = new KafkaProducer<>(producerConfig);

        producer.initTransactions();
        try {
            producer.beginTransaction();
            String firstMsg = "Hello 12";
            producer.send(new ProducerRecord<>("test", firstMsg, firstMsg));
            producer.commitTransaction(); //commit
        } catch (KafkaException e) {
            producer.abortTransaction();
        }
    }
}
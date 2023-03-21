package org.curso.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.curso.kafka.utils.KafkaStreamProperties.TOPIC_IN;

public class Quijote {

    public static void main(String[] args) throws IOException {

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<Long, String> producer = new KafkaProducer<>(producerConfig);
        AtomicInteger i = new AtomicInteger();

        Files.lines(Paths.get("KafkaStreamsApp/src/main/resources/quijote.txt"))
                .forEach(l -> {
                    Long key = (long) i.get() % 3;
                    producer.send(new ProducerRecord<Long, String>("aggregate", key, l));
                    System.out.println(l);
                    i.getAndIncrement();
                    try {
                        Thread.sleep((long) (5000.0 * Math.random()));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

}

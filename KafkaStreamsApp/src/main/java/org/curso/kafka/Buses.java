package org.curso.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.curso.kafka.utils.KafkaStreamProperties.TOPIC_IN;

public class Buses {

    public static void main(String[] args) throws IOException {

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<Long, String> producer = new KafkaProducer<>(producerConfig);

        String data = Files.lines(Paths.get("KafkaStreamsApp/src/main/resources/buses.json"))
                .collect(Collectors.joining("\n"));
        JSONArray list = new JSONObject(data).getJSONArray("features");

        while (true) {
            int line = Math.toIntExact(Math.round(Math.random() * (list.length() - 1) + 1));
            JSONObject jsonObject = new JSONObject();
            producer.send(new ProducerRecord<Long, String>("topic.bus",  Integer.valueOf(String.valueOf(line))%3, Long.valueOf(String.valueOf(line)), list.get(line).toString()));
            System.out.println(list.get(line));
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

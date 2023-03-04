package org.curso.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.curso.kafka.utils.KafkaStreamProperties.TOPIC_IN;

public class Sender {

    public static void main(String[] args) throws IOException {

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<Long, String> producer = new KafkaProducer<>(producerConfig);

        List<String> data = Files.lines(Paths.get("KafkaStreamsApp/src/main/resources/data.csv"))
                .collect(Collectors.toList());
        List<String> header = Arrays.asList(data.get(0).split(","));

        while (true) {
            int line = Math.toIntExact(Math.round(Math.random() * (data.size() - 1) + 1));
            JSONObject jsonObject = new JSONObject();
            IntStream.range(0, header.size()).forEach(i -> {
                jsonObject.put(header.get(i), data.get(line).split(",")[i].replace("\"", ""));
            });
            producer.send(new ProducerRecord<Long, String>(TOPIC_IN, Long.valueOf(String.valueOf(line)), jsonObject.toString()));
            System.out.println(jsonObject);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

package org.curso.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaStreamProperties {

    public static final String TOPIC_IN = "topic.in";
    public static final String TOPIC_COUNT_IN = "topic.count.in";
    public static final String TOPIC_SESSION_IN = "topic.session.in";
    public static final String TOPIC_COUNT_OUT = "topic.count.out";
    public static final String TOPIC_OUT_A = "topic.out.a";

    public static Properties getKafkaStreamProperties(String appName){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kStream");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return config;
    }
}

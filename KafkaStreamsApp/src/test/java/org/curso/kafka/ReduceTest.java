package org.curso.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;

import static org.curso.kafka.utils.KafkaStreamProperties.getKafkaStreamProperties;

public class ReduceTest {

    @Test
    public void countTest() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "reduce");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kStream");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());



        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(CountApp.createTopology(),
                config, Instant.now());

        TestInputTopic<String, String> topicIn = topologyTestDriver
                .createInputTopic("in", Serdes.String().serializer(), Serdes.String().serializer());

        topicIn.pipeInput("aaa", "hola mundo", new Date().getTime());
        topicIn.pipeInput("bbb", "asdas asd", new Date().getTime());
        topicIn.pipeInput("aaa", "hola evento", new Date().getTime());
        topicIn.pipeInput("bbb", "cuarto", new Date().getTime());
        topicIn.pipeInput("aaa", "quinto", new Date().getTime());

        TestOutputTopic<String, String> outTopic = topologyTestDriver.createOutputTopic("out", Serdes.String().deserializer(), Serdes.String().deserializer());

        KeyValue<String, String> record = outTopic.readKeyValue();



//        topologyTestDriver.advanceWallClockTime(Duration.ofHours(2));
//
//        KeyValueStore store = topologyTestDriver.getKeyValueStore("store-name");
//
//
//
//        topologyTestDriver.createOutputTopic("out", Serdes.String().deserializer(),
//                Serdes.String().deserializer());


        topologyTestDriver.close();

    }



}

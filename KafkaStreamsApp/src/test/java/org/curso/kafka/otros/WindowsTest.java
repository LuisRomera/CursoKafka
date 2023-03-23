package org.curso.kafka.otros;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.curso.kafka.CountApp;
import org.curso.kafka.aggregate.CountWindowApp;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;

public class WindowsTest {

    @Test
    public void countTest() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "window");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kStream");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());



        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(CountWindowApp.createTopology(),
                config, Instant.now());

        TestInputTopic<String, String> topicIn = topologyTestDriver
                .createInputTopic("in", Serdes.String().serializer(), Serdes.String().serializer());


        long startTest = new Date().getTime();
        topicIn.pipeInput("aaa", "hola mundo", startTest);

        topicIn.pipeInput("bbb", "asdas asd", startTest + (60*61+100000));
        topicIn.pipeInput("aaa", "tercer evento", startTest + (60*61+100000));
        topicIn.pipeInput("bbb", "cuarto", startTest + (60*61+100000));
        topicIn.pipeInput("aaa", "quinto", startTest + (60*61+100000));


        topologyTestDriver.close();



//        topologyTestDriver.advanceWallClockTime(Duration.ofHours(2));
//
//        KeyValueStore store = topologyTestDriver.getKeyValueStore("store-name");
//
//
//
//        topologyTestDriver.createOutputTopic("out", Serdes.String().deserializer(),
//                Serdes.String().deserializer());



    }





}

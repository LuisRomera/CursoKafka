package org.curso.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;
import java.util.Properties;

public class WindowStreamTest {

    private TopologyTestDriver topologyTestDriver;

    @Test
    public void countTest() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "reduce");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kStream");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());



        topologyTestDriver = new TopologyTestDriver(WindowsStreamApp.createTopology(),
                config, Instant.now());

        TestInputTopic<String, String> topicIn = topologyTestDriver
                .createInputTopic("in", Serdes.String().serializer(), Serdes.String().serializer());

        long now = new Date().getTime();

        topicIn.pipeInput("aaa", "hola mundo", now);
        topicIn.pipeInput("bbb", "asdas asd", now + 5*1000);
        topicIn.pipeInput("aaa", "hola evento", now);
        topicIn.pipeInput("bbb", "cuarto", now + 71*1000);
        topicIn.pipeInput("aaa", "quinto", now);

        TestOutputTopic<String, String> outTopic = topologyTestDriver.createOutputTopic("out", Serdes.String().deserializer(), Serdes.String().deserializer());

        KeyValue<String, String> record = outTopic.readKeyValue();



    }
    @After
    public void close(){
        topologyTestDriver.close();
    }


}

package org.curso.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.curso.kafka.aggregate.TansformApp;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class ProcessorStreamTest {

    private TopologyTestDriver topologyTestDriver;

    @Test
    public void processTest() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "reduce");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kStream");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());



        topologyTestDriver = new TopologyTestDriver(ProcessorStreamApp.createTopology(), config);

        TestInputTopic<String, String> topicIn = topologyTestDriver
                .createInputTopic("in", Serdes.String().serializer(), Serdes.String().serializer());

        long now = new Date().getTime();

        topicIn.pipeInput("aaa", "hola mundo");
        topicIn.pipeInput("bbb", "asdas asd");
        topicIn.pipeInput("aaa", "hola evento");
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(61));
        topicIn.pipeInput("bbb", "cuarto");
        topicIn.pipeInput("aaa", "quinto");

        TestOutputTopic<String, Long> outTopic = topologyTestDriver.createOutputTopic("out", Serdes.String().deserializer(), Serdes.Long().deserializer());


        List<KeyValue<String, Long>> result = new ArrayList<>();
        while (!outTopic.isEmpty()){
            result.add(outTopic.readKeyValue());
        }




        outTopic.isEmpty();



    }
    @After
    public void close(){
        topologyTestDriver.close();
    }


}

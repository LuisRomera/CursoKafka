package org.curso.kafka;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static org.curso.kafka.utils.KafkaStreamProperties.getKafkaStreamProperties;

public class KafkaStreamAppTest {

    @Test
    public void countTest() {
        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(KafkaStreamsApp.createTopology(),
                getKafkaStreamProperties("KafkaStreamsApp"), Instant.now());

        TestInputTopic<Long, String> topicIn = topologyTestDriver
                .createInputTopic("in", Serdes.Long().serializer(), Serdes.String().serializer());

        topicIn.pipeInput(1L, "hola mundo", new Date().getTime());

        topologyTestDriver.advanceWallClockTime(Duration.ofHours(2));

        KeyValueStore store = topologyTestDriver.getKeyValueStore("store-name");

        topologyTestDriver.createOutputTopic("out", Serdes.String().deserializer(),
                Serdes.String().deserializer());

        topologyTestDriver.close();
    }
}

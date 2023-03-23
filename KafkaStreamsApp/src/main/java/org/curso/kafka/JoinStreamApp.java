package org.curso.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Arrays;

import static org.curso.kafka.utils.KafkaStreamProperties.getKafkaStreamProperties;

public class JoinStreamApp {
    public static void main(String[] args) {

        Topology topology = createTopology();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("KafkaStreams"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

    public static Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream("in");


        KStream<String, String> left = stream.filter((k, v) -> v.startsWith("hola"));
        KStream<String, String> right = stream.filter((k, v) -> !v.startsWith("hola"));

        KStream<String, String> joinStream = left.outerJoin(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue,
                JoinWindows.of(Duration.ofMinutes(5)));

        joinStream.foreach((k, v)-> System.out.println(k + " - " + v));


//        kTable.toStream().to("out");


        return builder.build();
    }


}

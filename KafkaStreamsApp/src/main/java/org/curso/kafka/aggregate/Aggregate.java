package org.curso.kafka.aggregate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.List;

import static org.curso.kafka.utils.KafkaStreamProperties.TOPIC_COUNT_IN;
import static org.curso.kafka.utils.KafkaStreamProperties.getKafkaStreamProperties;

public class Aggregate {

    public static void main(String[] args) {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> stream = builder.stream("aggregate");

        KGroupedStream<Long, String> groupedStream = stream.groupByKey();


        KTable<Long, Long> aggregateTable = groupedStream.aggregate(
                () -> 0L,
                (aggKey, newValue, aggValue) -> aggValue + newValue.length(),
                Materialized.<Long, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store")
                        .withValueSerde(Serdes.Long()));


        aggregateTable.toStream().foreach((k,v) -> System.out.println(k + " - " + v));

        aggregateTable.toStream().to("out");


        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("curso.word.count"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}

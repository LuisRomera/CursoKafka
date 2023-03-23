package org.curso.kafka;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.JSONObject;

import java.util.Arrays;

import static org.curso.kafka.utils.KafkaStreamProperties.getKafkaStreamProperties;

public class AggregateApp {
    public static void main(String[] args) {

        Topology topology = createTopology();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("KafkaStreams"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

    public static Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, String> stream = builder.stream("in");

        KStream<String, String> wordWord = stream.mapValues(v -> Arrays.asList(v.split(" ")))
                .flatMapValues(v -> v)
                .selectKey((k, v) -> v);


        KGroupedStream<String, String> groupedStream = wordWord.groupByKey();

        KTable<String, Long> kTable = groupedStream.aggregate(
                () -> 0L,
                (aggKey, newValue, aggValue) -> aggValue + newValue.length(),
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store")
                        .withValueSerde(Serdes.Long()));


        kTable.toStream().to("out");






        return builder.build();
    }


}

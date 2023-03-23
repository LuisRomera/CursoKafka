package org.curso.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

import static org.curso.kafka.utils.KafkaStreamProperties.getKafkaStreamProperties;

public class CountApp {
    public static void main(String[] args) {


        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "reduce");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kStream");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Topology topology = createTopology();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("KafkaStreams"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

    public static Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream("in");

        stream.foreach((k, v) -> System.out.println(v));


        KTable<String, String> kTable = stream.groupByKey().reduce(
                (newValue, aggValue) -> aggValue + " " + newValue
        );

        kTable.toStream().to("out");












//        KStream<String, String> wordWord = stream.mapValues(v -> Arrays.asList(v.split(" ")))
//                .flatMapValues(v -> v)
//                .selectKey((k, v) -> v);




//
//
//        KTable<String, Long> groupedStream = wordWord
//                .groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count();
//
//        groupedStream.toStream().foreach((k,v) -> System.out.println(k + " - " + v));
//
//
//
////        KTable<String, Long> kTable = groupedStream.aggregate(
////                () -> 0L,
////                (aggKey, newValue, aggValue) -> aggValue + newValue.length(),
////                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store")
////                        .withValueSerde(Serdes.Long()));
//
//
////        kTable.toStream().to("out");






        return builder.build();
    }


}

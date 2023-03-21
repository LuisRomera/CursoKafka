package org.curso.kafka.aggregate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import static org.curso.kafka.utils.KafkaStreamProperties.getKafkaStreamProperties;

public class Reduce {

    public static void main(String[] args) {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> stream = builder.stream("aggregate");

        KGroupedStream<Long, String> groupedStream = stream.groupByKey();


        KTable<Long, String> aggregateTable = groupedStream.reduce(
                (newValue, aggValue) -> aggValue + " | " + newValue);

        aggregateTable.toStream().to("out.l");


        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("curso.reduce"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}

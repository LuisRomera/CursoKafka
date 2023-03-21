package org.curso.kafka.aggregate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.json.JSONObject;

import java.awt.*;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.curso.kafka.utils.KafkaStreamProperties.*;
import static org.curso.kafka.utils.StreamUtils.printStream;

public class CountApp {

    public static void main(String[] args) {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> stream = builder.stream(TOPIC_COUNT_IN);

        KStream<Long, List<String>> splitStream = stream.mapValues(line -> Arrays.asList(line.split("\\s+")));

        KStream<Long, String> flatMapValuesStream = splitStream.flatMapValues(line -> line);


        // word_A -> 2
        // word_B -> 1

        // count words
        KStream<String, String> selectKeyStream = flatMapValuesStream.selectKey((k, v) -> v);
        //selectKeyStream.foreach((k, v) -> System.out.println("->" + k + " - " + v ));


        KTable<String, Long> groupStream = selectKeyStream.groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count();

        groupStream.toStream().foreach((k, v) -> System.out.println(k + " - " + v ));


        KGroupedTable<String, Long> groupedTable = groupStream.groupBy(
                (key, value) -> KeyValue.pair(key, value),
                Grouped.with(Serdes.String(), Serdes.Long()));

        KTable<String, Long> table = groupedTable.reduce(
                (aggValue, newValue) -> aggValue + newValue,
                (aggValue, oldValue) -> aggValue - oldValue);

        table.toStream().foreach((k, v) -> System.out.println(k + " - " + v ));


        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("curso.word.count"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}

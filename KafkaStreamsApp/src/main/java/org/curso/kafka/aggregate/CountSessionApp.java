package org.curso.kafka.aggregate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static java.time.Duration.ofMinutes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.curso.kafka.utils.KafkaStreamProperties.*;

public class CountSessionApp {
    public static void main(String[] args) {


        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> stream = builder.stream(TOPIC_SESSION_IN);


        KTable<Windowed<Long>, Long> countWindow = stream.groupByKey()
                .windowedBy(SessionWindows.with(Duration.ofMinutes(1)))
                .count();
        countWindow.toStream().foreach((k,v) -> System.out.println(k + " - " + v));



//        KStream<Long, List<String>> splitStream = stream.mapValues(line -> Arrays.asList(line.split("\\s+")));
//
//        // Window session
//        KStream<Windowed<Long>, Long> windowStream = splitStream.groupByKey()
//                .windowedBy(SessionWindows.with(Duration.ofMillis(3000)))
//                .count()
//                .toStream();
//
//        windowStream.foreach((k,v) -> System.out.println(k + " - " + v));
//
//        windowStream.to("out.session");

        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("curse.session.count"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}

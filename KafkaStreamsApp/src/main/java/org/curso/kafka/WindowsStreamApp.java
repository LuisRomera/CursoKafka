package org.curso.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

import static org.curso.kafka.utils.KafkaStreamProperties.getKafkaStreamProperties;

public class WindowsStreamApp {
    public static void main(String[] args) {

        Topology topology = createTopology();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("KafkaStreams"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

    public static Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream("in");

        KTable<Windowed<String>, Long> kTable = stream.groupByKey()
                .windowedBy(SessionWindows.with(Duration.ofMinutes(1)))
                .count();



        kTable.toStream().foreach((k, v)-> System.out.println(k + " - " + v));


//        kTable.toStream().to("out");


        return builder.build();
    }


}

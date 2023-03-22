package org.curso.kafka;

import netscape.javascript.JSObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.json.JSONObject;

import java.util.Properties;

import static org.curso.kafka.utils.KafkaStreamProperties.getKafkaStreamProperties;

public class KafkaStreamsApp {
    public static void main(String[] args) {


        Topology topology = createTopology();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("KafkaStreamsApp"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

    public static Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, String> stream = builder.stream("practica");
        KStream<Long, JSONObject> jsonStream = stream.map((k, v) -> new KeyValue<>(k, new JSONObject(v)));

        KStream<Long, JSONObject> filterStream = jsonStream
                .filter((k, v) -> v.getInt("Year") < 2023);

        KTable<String, Long> kTable = filterStream
                .selectKey((k, v) -> v.getString("Year"))
                .map((k, v) -> new KeyValue<>(k, v.toString()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count();

        kTable.toStream().peek((k, v) -> System.out.println(k + " - " + v));
        filterStream
                .map((k, v) -> new KeyValue<>(v.getString("Variable_code"), v.toString()))
                .to("practica-out");

        return builder.build();
    }


}

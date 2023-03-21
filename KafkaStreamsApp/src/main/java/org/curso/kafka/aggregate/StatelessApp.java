package org.curso.kafka.aggregate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;

import java.util.Date;

import static org.curso.kafka.utils.KafkaStreamProperties.*;
import static org.curso.kafka.utils.StreamUtils.printStream;

public class StatelessApp {

    public static void main(String[] args) {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> stream = builder.stream(TOPIC_IN);

        printStream("Source", stream);

        // Map
        KStream<Long, JSONObject> jsonStream = stream
                .map((k, v) -> {
                    JSONObject json = new JSONObject(v);
                    json.put("timestamp", new Date());
                    return new KeyValue<>(k, json);
                });
        printStream("Map", jsonStream);

        // Filter
        KStream<Long, JSONObject> filterStream = jsonStream
                .filter((k, v) -> ((JSONObject) v).getInt("Year") < 2021);

        printStream("Filter", filterStream);


        // Sink
        filterStream
                .map((k, v) -> new KeyValue<>(k, v.toString()))
                .to(TOPIC_OUT_A);


        // SelectKey
//        KStream<String, JSONObject> selectKeyStream = filterStream
//                .selectKey((k, v) -> ((JSONObject) v).getInt("Year") + "-" + ((JSONObject) v).getString("Variable_name"));
//
//        printStream("SelectKey", selectKeyStream);
//
//
//        // Repartition
//        KStream<String, String> preRepartitionStream =  selectKeyStream.map((k, v)-> new KeyValue<>(k.toString(), v.toString()));
//        Repartitioned<String, String> repartitioned = Repartitioned.with(Serdes.String(), Serdes.String());
//        KStream<String, String> repartitionStream = preRepartitionStream
//                .repartition(repartitioned);
//        printStream("Repartition", selectKeyStream);


        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("curso.stateless"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}

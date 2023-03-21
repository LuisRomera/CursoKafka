package org.curso.kafka.aggregate;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.json.JSONObject;

import java.util.Date;

import static org.curso.kafka.utils.KafkaStreamProperties.*;
import static org.curso.kafka.utils.StreamUtils.printStream;

public class TansformApp {

    public static void main(String[] args) {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream = builder.stream("transform.in");

        printStream("Source", stream);


        KStream<Object, Object> result = stream.transform(new CustomTransform());


        Topology topology = builder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("curso.transform"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}

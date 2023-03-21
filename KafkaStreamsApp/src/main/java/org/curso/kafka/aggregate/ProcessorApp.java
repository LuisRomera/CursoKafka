package org.curso.kafka.aggregate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import static org.curso.kafka.utils.KafkaStreamProperties.getKafkaStreamProperties;

public class ProcessorApp {

    public static void main(String[] args) {

        Topology topology = new Topology();

        topology.addSource("readerTopic", "processor.in");


        topology.addProcessor("countApp", new ProcessCount(), "readerTopic");

        topology.addSink("countsink", "topic.out.count", Serdes.String().serializer(),
                Serdes.Long().serializer(),"countApp");


        KafkaStreams kafkaStreams = new KafkaStreams(topology, getKafkaStreamProperties("curse.count"));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }


}

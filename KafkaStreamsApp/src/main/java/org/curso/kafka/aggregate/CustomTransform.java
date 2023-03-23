package org.curso.kafka.aggregate;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

public class CustomTransform implements TransformerSupplier {

    @Override
    public Transformer get() {
        return new Transformer() {

            private ProcessorContext processorContext;

            @Override
            public void init(ProcessorContext processorContext) {
                this.processorContext = processorContext;


            }

            @Override
            public KeyValue<String, String> transform(Object k, Object value) {
                String id = processorContext.topic() + "_" +
                        processorContext.offset() + "_" + processorContext.partition();
                return new KeyValue<>(id, (String) value);
            }

            @Override
            public void close() {

            }
        };
    }
}

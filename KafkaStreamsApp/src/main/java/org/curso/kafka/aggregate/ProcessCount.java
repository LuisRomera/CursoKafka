package org.curso.kafka.aggregate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.*;

public class ProcessCount implements ProcessorSupplier {

    @Override
    public Processor get() {

        return new Processor() {

            private KeyValueStore<String, Long> kvStore;

            private ProcessorContext processorContext;

            @Override
            public void init(ProcessorContext processorContext) {
                this.processorContext = processorContext;
                this.kvStore = processorContext.getStateStore("countStore");
                this.processorContext.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::forwardAll);
            }

            private void forwardAll(long timestamp) {
            }

            @Override
            public void process(Object key, Object value) {
                Arrays.asList(((String) value).split(" ")).forEach(w -> {
                    Long last = kvStore.get(w);
                    if (last == null) {
                        kvStore.put(w, 1L);
                        last = 1L;
                    } else {
                        kvStore.put(w, last + 1L);
                        last++;
                    }
                    processorContext.forward(w, last);
                });
            }

            @Override
            public void close() {

            }
        };
    }

//    @Override
//    public Set<StoreBuilder<?>> stores() {
//        final StoreBuilder<KeyValueStore<String, Long>> countsStoreBuilder =
//                Stores
//                        .keyValueStoreBuilder(
//                                Stores.persistentKeyValueStore("countStore"),
//                                Serdes.String(),
//                                Serdes.Long()
//                        );
//        return Collections.singleton(countsStoreBuilder);
//    }



}

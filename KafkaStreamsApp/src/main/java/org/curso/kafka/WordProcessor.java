package org.curso.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class WordProcessor implements ProcessorSupplier {
    @Override
    public org.apache.kafka.streams.processor.Processor get() {
        return new org.apache.kafka.streams.processor.Processor() {


            private ProcessorContext processorContext;


            private KeyValueStore<String, Long> store;


            @Override
            public void init(ProcessorContext processorContext) {
                this.processorContext = processorContext;
                this.store = processorContext.getStateStore("countStore");
                processorContext.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, this::forwallAll);
            }

            private void forwallAll(long timestamp){
                Iterator iterator = store.all();

                while (iterator.hasNext()){
                    KeyValue aaa = (KeyValue) iterator.next();
                    processorContext.forward("key", store.get("keyString"));
                }

            }


            @Override
            public void process(Object key, Object value) {
                String keyString = (String) key;
                String valueString = (String) value;
                Long longValue = Long.valueOf(valueString.length());

                if(store.get(keyString) != null){
                    Long oldLength = store.get(keyString);
                    store.put(keyString, oldLength + longValue);
                }else{
                    store.put(keyString, longValue);
                }

            }



            @Override
            public void close() {

            }
        };
    }


    @Override
    public Set<StoreBuilder<?>> stores() {
        final StoreBuilder<KeyValueStore<String, Long>> countsStoreBuilder =
                Stores
                        .keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("countStore"),
                                Serdes.String(),
                                Serdes.Long()
                        );
        return Collections.singleton(countsStoreBuilder);
    }

}

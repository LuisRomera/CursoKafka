package org.curso.kafka.utils;

import org.apache.kafka.streams.kstream.KStream;

public class StreamUtils {

    /**
     * Print stream
     *
     * @param stream
     */
    public static void printStream(String transform, KStream stream) {
        stream.foreach((k, v) -> {
            System.out.println("------  Transform " + transform + " ------");
            System.out.println("\t* key: " + k);
            System.out.println("\t* value: " + v.toString());
        });
    }
}

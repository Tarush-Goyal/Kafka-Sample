package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class Streams {
    public static void main(String[] args) {
        // Configure Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Define the processing topology
        KStream<String, String> inputStream = builder.stream("input-topic");
        KStream<String, String> processedStream = inputStream.mapValues(value -> value.toUpperCase());

        // Write the processed stream to the output topic
        processedStream.to("output-topic");

        // Build the topology
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Start the Kafka Streams application
        streams.start();

        // Add shutdown hook to gracefully stop the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

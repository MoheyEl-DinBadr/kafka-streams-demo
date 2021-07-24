package com.mohey.favouritecolour;

import lombok.extern.java.Log;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
@ComponentScan(basePackages = "com.mohey")
@Log
public class FavouriteColourApplication {
    public static void main(String[] args) {
        SpringApplication.run(FavouriteColourApplication.class);
    }

    @Bean("kafka-props")
    public Properties createProperties(KafkaProperties kafkaProperties,
                                       @Value("${spring.application.name:streams-starter-app}") String appName) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(StreamsConfig.STATE_DIR_CONFIG, "data");
        props.put(StreamsConfig.POLL_MS_CONFIG, 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        return props;
    }


    @Bean
    public StreamsBuilder favouriteColorStreams(){
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> favoriteColorInput = streamsBuilder.stream("favourite-color-input");

        KTable<String, Long> favoriteColorsTable = favoriteColorInput.filter((key, value) -> value.contains(","))
                .peek((key, value) -> System.out.println("Value: " + value))
                .map(((key, value) -> {

                    String[] keyValuePair = value.split(",");
                    String newKey = keyValuePair[0].trim().toLowerCase();
                    String newValue=keyValuePair[1].trim().toLowerCase();
                    KeyValue<String, String> keyValue= new KeyValue<>(newKey, newValue);

                    return keyValue;
                }))
                .filter(((key, value) -> Arrays.asList("green", "blue", "red").contains(value)))
                .peek(((key, value) -> log.info("Key: " + key + "\nValue: " + key)))
                .toTable()
                .groupBy(((key, value) -> KeyValue.pair(value,value)), Grouped.with(Serdes.String(), Serdes.String()))
                .count(Named.as("count"));

        favoriteColorsTable.toStream().to("favourite-color-output");



        return streamsBuilder;
    }
}

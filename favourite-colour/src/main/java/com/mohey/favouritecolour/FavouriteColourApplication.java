package com.mohey.favouritecolour;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.awt.*;

@SpringBootApplication
@ComponentScan(basePackages = "com.mohey")
public class FavouriteColourApplication {
    public static void main(String[] args) {
        SpringApplication.run(FavouriteColourApplication.class);
    }

    @Bean
    public StreamsBuilder favouriteColorStreams(){
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> favoriteColorInput = streamsBuilder.stream("favourite-color-input");

        KTable<String, Long> favoriteColorsTable = favoriteColorInput.filter((key, value) -> value.contains(","))
                .map(((key, value) -> {

                    String[] keyValuePair = value.split(",");
                    String newKey = keyValuePair[0].trim().toLowerCase();
                    String newValue=keyValuePair[1].trim().toLowerCase();
                    KeyValue<String, String> keyValue= new KeyValue<>(newKey, newValue);

                    return keyValue;
                }))
                .filter(((key, value) -> value.contains("blue") || value.contains("red") || value.contains("green")))
                .toTable()
                .groupBy(((key, value) -> KeyValue.pair(value,value)))
                .count();

        favoriteColorsTable.toStream().to("favourite-color-output");



        return streamsBuilder;
    }
}
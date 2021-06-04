package com.mohey.wordcountstreams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;

@Configuration
public class StreamBuilderConfig {

    @Bean
    public StreamsBuilder wordCountsStreamsBuilder(){
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> wordCountInput = streamsBuilder.stream("word-count-input");

        KTable<String, Long> wordCounts = wordCountInput.mapValues((value) -> value.toLowerCase())
                .flatMap((key, value)->{
                            ArrayList<KeyValue<String, String>> output = new ArrayList<>();
                            String[] values = value.split("\\s+");

                            for(String keyVal: values){
                                output.add(new KeyValue<>(keyVal, keyVal));
                            }


                            return output;
                        }
                )
                //.flatMapValues(value -> Arrays.asList(value.split("\\s+")))
                //.selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count(Named.as("Counts"));
        // streamsBuilder.stream("some_topic") etc ...

        wordCounts.toStream().to("word-count-output");

        return streamsBuilder;
    }
}

package com.mohey.wordcountstreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Properties;

@Configuration
public class StreamBuilderConfig {

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

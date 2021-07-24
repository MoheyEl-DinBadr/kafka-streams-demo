package com.mohey.streamsconfig;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.java.Log;
import netscape.javascript.JSObject;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Properties;

@Configuration
@Log
public class KafkaStreamsConfig {

    @Autowired
    private StreamsBuilder streamsBuilder;
    @Autowired
    @Qualifier("kafka-props")
    private Properties props;

    @Bean
    public KafkaStreams kafkaStreams(){
        System.out.println("props.keySet() = " + props.keySet());
        //log.info("appName: " + appName);
        //final Properties props = new Properties();
        /*props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde<Person>.class);

        props.put(StreamsConfig.STATE_DIR_CONFIG, "data");
        props.put(StreamsConfig.POLL_MS_CONFIG, 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);*/

        //props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, JsonNode.class);

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            kafkaStreams.close();
        }));
        // printed the topology
        log.info(kafkaStreams.toString());

        return kafkaStreams;
    }
}

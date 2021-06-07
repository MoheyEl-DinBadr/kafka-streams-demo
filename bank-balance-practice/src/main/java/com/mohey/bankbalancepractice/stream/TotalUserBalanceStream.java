package com.mohey.bankbalancepractice.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.mohey.bankbalancepractice.entities.Person;
import lombok.extern.java.Log;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.json.JSONObject;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Log
@Component
public class TotalUserBalanceStream {

    /*ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public StreamsBuilder calculateBalance(){
        Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();

        Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();

        Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeserializer);

        JsonSerde<JsonNode> jsonSerde = new JsonSerde<>();

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, JsonNode> stream = streamsBuilder.stream("transactions-input");

        stream.peek((key, value) -> log.info("Key: " + key + "\nValue: " + value.toString()))
        .groupByKey();

        return streamsBuilder;
    }*/
}

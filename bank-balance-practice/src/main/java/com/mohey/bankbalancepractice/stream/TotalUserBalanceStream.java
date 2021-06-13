package com.mohey.bankbalancepractice.stream;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Log
@Component
public class TotalUserBalanceStream {

    ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private Properties props;

    @Bean
    public StreamsBuilder calculateBalance(){

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        JsonSerde<JsonNode> serde = new JsonSerde<>();
        Grouped<String, JsonNode> grouped = Grouped.as("Json");
        KStream<String, String> stream = streamsBuilder.stream("transactions-input");

        final JsonSerializer jsonSerializer = new JsonSerializer();
        final JsonDeserializer jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.S");
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());
        JsonNode initialBalanceJson = initialBalance;

        Map<String, JsonNode> keyValue = new HashMap<>();

        stream.mapValues((readOnlyKey, value) -> {
            JsonNode valueNode = null;
            try {
                valueNode = (ObjectNode) objectMapper.readTree(value);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            return valueNode;
        }).peek((key, value) ->{
            log.info("\nStreams Peek  Key: " + key + "\nValue: " + value.toString() + "\nClass: " + value.getClass());
            ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
            JsonNode lastKeyValue = keyValue.get(key);
            if(lastKeyValue == null){
                lastKeyValue = JsonNodeFactory.instance.objectNode();
            }
            if(lastKeyValue.get("amount") == null){
                ((ObjectNode)lastKeyValue).put("amount", 0.0);
            }

            if(lastKeyValue.get("time") != null){
                try {
                    Date date = dateTimeFormatter.parse(lastKeyValue.get("time").asText());
                    Date date1 = dateTimeFormatter.parse(value.get("time").asText());
                    if(date1.after(date)){
                        objectNode.put("amount", value.get("amount").asDouble() + lastKeyValue.get("amount").asDouble());
                        objectNode.put("time", value.get("time"));
                        keyValue.put(key,objectNode);
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }else {
                objectNode.put("amount", value.get("amount").asDouble() + lastKeyValue.get("amount").asDouble());
                objectNode.put("time", value.get("time"));
                keyValue.put(key,objectNode);
            }

        })
        .mapValues(((readOnlyKey, value) -> keyValue.get(readOnlyKey).toString()))
                .peek((key, value) -> log.info("Streams peek2:  Key: " + key + "\nValue: " + value + "\nClass: " + value.getClass()))
        .to("transactions-output");

        return streamsBuilder;
    }

    @Bean
    public String createProperties(KafkaProperties kafkaProperties,
                                       @Value("${spring.application.name:streams-starter-app}") String appName){

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(StreamsConfig.STATE_DIR_CONFIG, "data");
        props.put(StreamsConfig.POLL_MS_CONFIG, 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        return "Hello";
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
}

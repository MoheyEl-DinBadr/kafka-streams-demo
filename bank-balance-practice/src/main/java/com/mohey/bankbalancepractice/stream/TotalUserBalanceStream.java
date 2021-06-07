package com.mohey.bankbalancepractice.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.mohey.bankbalancepractice.entities.Person;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class TotalUserBalanceStream {

    /*ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public StreamsBuilder calculateBalance(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("transactions-input");

        stream.map(((key, value) -> {
            String newKey = null;
            JSONObject jsonValue = new JSONObject(value);
            try {
                Person person = objectMapper.readValue(value,Person.class);
                newKey = person.getName();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            return KeyValue.pair(newKey, jsonValue);
        }))
        .groupByKey()
        .aggregate(() -> 0L,
                (key, value, aggregate) -> value.getLong("amount") + aggregate
                );
        return streamsBuilder;
    }*/
}

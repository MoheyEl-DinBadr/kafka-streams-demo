package com.mohey.bankbalancepractice.producers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mohey.bankbalancepractice.entities.Person;
import lombok.extern.java.Log;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;

@Component
@Log
public class UsersTransactions {

    ObjectMapper mapper = new ObjectMapper();

    public final static Person johnUser = new Person("John", 0, Instant.now());
    public final static Person moheyUser = new Person("Mohey El-Din", 0, Instant.now());
    public final static Person mahmoudUser = new Person("Mahmoud", 0, Instant.now());
    public final static Person ahmedUser = new Person("Ahmed", 0, Instant.now());
    public final static Person badrUser = new Person("Badr", 0, Instant.now());
    public final static Person zakiUser = new Person("Zaki", 0, Instant.now());



    @Autowired
    private ProducerFactory<String, String> producerFactory;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        configProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        configProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        configProps.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                "true");

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {

        return new KafkaTemplate<>(producerFactory);
    }


    public void sendMessage(String msg, String key, String topicName) {
        log.info("Topic: " + topicName + ", Key: " + key + ", Msg: " + msg);
        kafkaTemplate.send(topicName, key, msg);
    }

    @Bean
    public void sendTransactions(){
        mapper.registerModule(new JavaTimeModule());
        new Timer().schedule(new PersonUpdateTimer(), 0, 4000);
    }




    public class PersonUpdateTimer extends TimerTask {



        Random random = new Random();
        @Override
        public void run() {
            UsersTransactions.johnUser.setAmount(random.nextInt(100));
            UsersTransactions.johnUser.setTime(Instant.now());

            UsersTransactions.moheyUser.setAmount(random.nextInt(100));
            UsersTransactions.moheyUser.setTime(Instant.now());

            UsersTransactions.mahmoudUser.setAmount(random.nextInt(100));
            UsersTransactions.mahmoudUser.setTime(Instant.now());

            UsersTransactions.ahmedUser.setAmount(random.nextInt(100));
            UsersTransactions.ahmedUser.setTime(Instant.now());

            UsersTransactions.badrUser.setAmount(random.nextInt(100));
            UsersTransactions.badrUser.setTime(Instant.now());

            UsersTransactions.zakiUser.setAmount(random.nextInt(100));
            UsersTransactions.zakiUser.setTime(Instant.now());

            sendMessage(johnUser.toJSON().toPrettyString(), johnUser.getName(), "transactions-input");

            sendMessage(moheyUser.toJSON().toPrettyString(), moheyUser.getName(), "transactions-input");

            sendMessage(mahmoudUser.toJSON().toPrettyString(), mahmoudUser.getName(), "transactions-input");

            sendMessage(ahmedUser.toJSON().toPrettyString(), ahmedUser.getName(), "transactions-input");

            sendMessage(badrUser.toJSON().toPrettyString(), badrUser.getName(), "transactions-input");

            sendMessage(zakiUser.toJSON().toPrettyString(), zakiUser.getName(), "transactions-input");

        }
    }

}

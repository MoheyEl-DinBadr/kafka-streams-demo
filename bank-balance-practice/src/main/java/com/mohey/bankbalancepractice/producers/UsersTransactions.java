package com.mohey.bankbalancepractice.producers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohey.bankbalancepractice.entities.Person;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class UsersTransactions {

    public final static Person johnUser = new Person("John", 0, new Date());
    public final static Person moheyUser = new Person("Mohey El-Din", 0, new Date());
    public final static Person mahmoudUser = new Person("Mahmoud", 0, new Date());
    public final static Person ahmedUser = new Person("Ahmed", 0, new Date());
    public final static Person badrUser = new Person("Badr", 0, new Date());
    public final static Person zakiUser = new Person("Zaki", 0, new Date());



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


    public void sendMessage(String msg, String topicName) {
        kafkaTemplate.send(topicName, msg);
    }

    @Bean
    public void sendTransactions(){
        new Timer().schedule(new PersonUpdateTimer(), 0, 60);
    }




    public class PersonUpdateTimer extends TimerTask {


        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();
        @Override
        public void run() {
            UsersTransactions.johnUser.setAmount(random.nextInt(1000));
            UsersTransactions.johnUser.setTime(new Date());

            UsersTransactions.moheyUser.setAmount(random.nextInt(1000));
            UsersTransactions.moheyUser.setTime(new Date());

            UsersTransactions.mahmoudUser.setAmount(random.nextInt(1000));
            UsersTransactions.mahmoudUser.setTime(new Date());

            UsersTransactions.ahmedUser.setAmount(random.nextInt(1000));
            UsersTransactions.ahmedUser.setTime(new Date());

            UsersTransactions.badrUser.setAmount(random.nextInt(1000));
            UsersTransactions.badrUser.setTime(new Date());

            UsersTransactions.zakiUser.setAmount(random.nextInt(1000));
            UsersTransactions.zakiUser.setTime(new Date());

            try {
                sendMessage(mapper.writeValueAsString(johnUser), "transactions-input");

                sendMessage(mapper.writeValueAsString(moheyUser), "transactions-input");

                sendMessage(mapper.writeValueAsString(mahmoudUser), "transactions-input");

                sendMessage(mapper.writeValueAsString(ahmedUser), "transactions-input");

                sendMessage(mapper.writeValueAsString(badrUser), "transactions-input");

                sendMessage(mapper.writeValueAsString(zakiUser), "transactions-input");

            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

        }
    }

}

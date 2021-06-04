package com.mohey.wordcountstreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.mohey")
public class StreamsWordCountApplication {
    public static void main(String[] args){
        SpringApplication.run(StreamsWordCountApplication.class);
    }

}

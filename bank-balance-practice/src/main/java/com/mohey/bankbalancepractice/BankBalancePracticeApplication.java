package com.mohey.bankbalancepractice;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.mohey")
public class BankBalancePracticeApplication {
    public static void main(String[] args) {
        SpringApplication.run(BankBalancePracticeApplication.class);
    }
}

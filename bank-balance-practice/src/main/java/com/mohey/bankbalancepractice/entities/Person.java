package com.mohey.bankbalancepractice.entities;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@AllArgsConstructor
@Getter @Setter
@EqualsAndHashCode
public class Person {
    private String name;
    private int amount;
    private Date time;

}

package com.mohey.bankbalancepractice.entities;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.*;
import org.json.JSONObject;

import java.time.Instant;

@AllArgsConstructor
@Getter @Setter
@EqualsAndHashCode
@NoArgsConstructor
public class Person {
    private String name;
    private int amount;
    private Instant time;

    public ObjectNode toJSON() {

        ObjectNode jo = JsonNodeFactory.instance.objectNode();
        jo.put("name", name);
        jo.put("amount", amount);
        jo.put("time", time.toString());

        return jo;
    }

}

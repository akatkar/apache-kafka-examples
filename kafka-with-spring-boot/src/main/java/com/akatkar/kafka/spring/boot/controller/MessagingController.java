package com.akatkar.kafka.spring.boot.controller;

import java.time.OffsetDateTime;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.akatkar.kafka.spring.boot.model.ComplexModel;
import com.akatkar.kafka.spring.boot.model.EventBody;
import com.akatkar.kafka.spring.boot.model.SimpleModel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("api/kafka")
@RequiredArgsConstructor
public class MessagingController {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @PostMapping
    public void post(@RequestBody SimpleModel simpleModel) throws JsonProcessingException {
        kafkaTemplate.send("MyTopic", objectMapper.writeValueAsString(simpleModel));
    }

    @PostMapping("v2")
    public void postV2(@RequestBody ComplexModel complexModel) throws JsonProcessingException {
        kafkaTemplate.send("MyTopic2", objectMapper.writeValueAsString(complexModel));
    }

    @PostMapping("/topic/{topic}")
    public void postV3(@PathVariable String topic, @RequestBody EventBody eventBody) throws JsonProcessingException {
        eventBody.getHeader().setFiredAt(OffsetDateTime.now());
        kafkaTemplate.send(topic, "key data", objectMapper.writeValueAsString(eventBody));
    }
}

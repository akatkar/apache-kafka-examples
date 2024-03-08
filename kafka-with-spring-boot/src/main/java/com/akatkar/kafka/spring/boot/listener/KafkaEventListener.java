package com.akatkar.kafka.spring.boot.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.akatkar.kafka.spring.boot.model.ComplexModel;
import com.akatkar.kafka.spring.boot.model.SimpleModel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class KafkaEventListener {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "MyTopic")
    public void myTopicListener(String data) throws JsonProcessingException {
        System.out.println(objectMapper.readValue(data, SimpleModel.class));
    }

    @KafkaListener(topics = "MyTopic2")
    public void myTopic2Listener(String data) throws JsonProcessingException {
        System.out.println(objectMapper.readValue(data, ComplexModel.class));
    }
}

package com.akatkar.kafka.spring.boot.model;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class EventBody {

    private EventHeader header;
    private UUID reference;
    private Action action;

}

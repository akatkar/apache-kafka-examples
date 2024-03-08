package com.akatkar.kafka.spring.boot.model;

import java.time.OffsetDateTime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class EventHeader {
    private String source;
    private String schemaVersion;
    private OffsetDateTime firedAt;
}

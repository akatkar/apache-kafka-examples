package com.akatkar.kafka.spring.boot.listener;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MyClass {
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private OffsetDateTime offsetDateTime;

    // Getter ve Setter metodları

    public OffsetDateTime getOffsetDateTime() {
        return offsetDateTime;
    }

    public void setOffsetDateTime(OffsetDateTime offsetDateTime) {
        this.offsetDateTime = offsetDateTime;
    }

    public static void main(String[] args) throws Exception {
        // Test için bir örnek oluşturalım
        MyClass myClass = new MyClass();
        myClass.setOffsetDateTime(OffsetDateTime.parse("2023-01-16T17:32:18+01:00"));

        // ObjectMapper kullanarak JSON'e dönüştürelim
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonString = objectMapper.writeValueAsString(myClass);

        System.out.println(jsonString);
    }
}

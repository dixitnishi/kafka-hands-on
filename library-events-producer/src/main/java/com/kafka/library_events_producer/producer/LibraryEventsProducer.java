package com.kafka.library_events_producer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.library_events_producer.dto.LibraryEvent;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@NoArgsConstructor(force = true)
@AllArgsConstructor
@RequiredArgsConstructor
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    private String topic;

    private final ObjectMapper objectMapper;

    private final KafkaTemplate<Integer,String> kafkaTemplate;

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        var completableFuture = kafkaTemplate.send(topic,key,value);
        return completableFuture
                .whenComplete(((sendResult, throwable) -> {
                    if(throwable!=null){
                        handleFaliure(key,value,throwable);
                    }else{
                        handleSuccess(key,value,sendResult);
                    }
                }));
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent successfully for key: {} and teh value : {},partition is {}",
                key,value,sendResult.getRecordMetadata().partition());
    }

    private void handleFaliure(Integer key, String value, Throwable throwable) {
        log.error("Error Sending the message and the exception is {}",throwable.getMessage());
    }


}

package com.kafka.library_events_producer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.library_events_producer.dto.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    private String topic;

    private final ObjectMapper objectMapper;

    private final KafkaTemplate<Integer,String> kafkaTemplate;

    public LibraryEventsProducer(ObjectMapper objectMapper, KafkaTemplate<Integer, String> kafkaTemplate) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

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

    public SendResult<Integer, String> sendLibraryEventSync(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
//      We can also use Producer record in the .send(producerRecord)
        var sendResult = kafkaTemplate.send(topic,key,value).get(3, TimeUnit.SECONDS);
        handleSuccess(key,value,sendResult);
        return sendResult;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEventProducerRecordWithHeader(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        var producerRecord = buildProducerRecordWithHeader(topic,key,value);
        var completableFuture = kafkaTemplate.send(producerRecord);
        return completableFuture
                .whenComplete(((sendResult, throwable) -> {
                    if(throwable!=null){
                        handleFaliure(key,value,throwable);
                    }else{
                        handleSuccess(key,value,sendResult);
                    }
                }));
    }

    private ProducerRecord<Integer,String> buildProducerRecordWithHeader(String topic, Integer key, String value) {
        List<Header> headers = List.of(new RecordHeader("event-source","scanner".getBytes()));
        return new ProducerRecord<>(topic,null,key,value,headers);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent successfully for key: {} and teh value : {},partition is {}",
                key,value,sendResult.getRecordMetadata().partition());
    }

    private void handleFaliure(Integer key, String value, Throwable throwable) {
        log.error("Error Sending the message and the exception is {}",throwable.getMessage());
    }
}

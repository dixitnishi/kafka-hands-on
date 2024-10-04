package com.kafka.library_events_consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.library_events_consumer.entity.LibraryEvent;
import com.kafka.library_events_consumer.repository.LibraryEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsService {

    private final ObjectMapper objectMapper;
    private final LibraryEventRepository libraryEventRepository;

    public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("Library Event {}",libraryEvent);
        switch(libraryEvent.getLibraryEventType()){
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                break;
            default:
                log.info("Invalid Library Event Type");
        }
    }

    public void save(LibraryEvent libraryEvent){
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
        log.info("Successfully persisted the library event {} : ",libraryEvent);
    }

    public void update(LibraryEvent libraryEvent){
        LibraryEvent existingLibEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId()).get();
        existingLibEvent.setBook(libraryEvent.getBook());
    }
}

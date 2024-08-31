package com.kafka.library_events_producer.controller;

import com.kafka.library_events_producer.dto.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LibraryEventsController {

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> createLibraryEvent(@RequestBody LibraryEvent libraryEvent){
        log.info("libraryEvent : {}", libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

}

package com.kafka.library_events_producer.dto;

public record LibraryEvent (Integer libraryEventId, LibraryEventType libraryEventType,Book book){}

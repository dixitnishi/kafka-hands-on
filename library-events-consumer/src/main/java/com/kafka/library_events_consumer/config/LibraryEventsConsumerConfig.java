package com.kafka.library_events_consumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.xml.sax.helpers.DefaultHandler;

@Configuration
@Slf4j
public class LibraryEventsConsumerConfig {

    KafkaProperties kafkaProperties;

    public DefaultErrorHandler errorHandler(){
        var fixedBackOff = new FixedBackOff( 1000L,2);
        var errorHandler =  new DefaultErrorHandler(fixedBackOff);
        errorHandler.addNotRetryableExceptions();
        errorHandler.setRetryListeners( ((record, ex, deliveryAttempt) -> {
            log.info("Failed record in retry listner exception {}, delivery event {}",ex.getMessage(),deliveryAttempt);
        }));
        return errorHandler;
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(3);
        return factory;
    }
}

package com.example.account.config;

import com.example.account.messaging.MalformedEventException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Error handling for all @KafkaListener methods of this service
 * (Spring Boot plugs this bean into the auto-configured listener container factory).
 * <ul>
 *   <li>A listener exception is retried 2 times, 1 second apart, then logged and skipped.</li>
 *   <li>A message that cannot be deserialized, or whose body cannot be read, is NOT retried.</li>
 * </ul>
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, ex) -> log.error("[KAFKA] Giving up on message | key={} | topic={} partition={} offset={} | error={}",
                        record.key(), record.topic(), record.partition(), record.offset(), ex.getMessage()),
                new FixedBackOff(1000L, 2L));
        handler.addNotRetryableExceptions(MalformedEventException.class);
        return handler;
    }
}

package com.example.account.messaging;

import com.example.account.messaging.event.EventMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Converts the JSON "body" of an envelope into the body record of its eventType. */
@Component
@RequiredArgsConstructor
public class EventBodyReader {

    private final ObjectMapper objectMapper;

    public <T> T read(EventMessage message, Class<T> bodyType) {
        if (message.getBody() == null || message.getBody().isNull()) {
            throw new MalformedEventException("Event " + message.getEventType() + " has no body", null);
        }
        try {
            return objectMapper.treeToValue(message.getBody(), bodyType);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new MalformedEventException("Cannot read body of " + message.getEventType()
                    + " as " + bodyType.getSimpleName(), e);
        }
    }
}

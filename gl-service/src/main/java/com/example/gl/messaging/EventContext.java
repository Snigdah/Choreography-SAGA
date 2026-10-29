package com.example.gl.messaging;

import java.util.UUID;

/**
 * Remembers which incoming event is being processed on this thread,
 * so every event published as a result gets it as its causationId.
 * Set and cleared by the Kafka listener.
 */
public final class EventContext {

    private static final ThreadLocal<UUID> CURRENT_EVENT_ID = new ThreadLocal<>();

    private EventContext() {
    }

    public static void set(UUID eventId) {
        CURRENT_EVENT_ID.set(eventId);
    }

    public static UUID currentEventId() {
        return CURRENT_EVENT_ID.get();
    }

    public static void clear() {
        CURRENT_EVENT_ID.remove();
    }
}

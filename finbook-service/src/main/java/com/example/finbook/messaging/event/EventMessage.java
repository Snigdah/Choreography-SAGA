package com.example.finbook.messaging.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Envelope of every Kafka message. The same shape for every event type.
 * <pre>
 * {
 *   "eventId": "...", "eventType": "GL_POSTING_REVERSED", "eventVersion": 1,
 *   "source": "gl-service", "correlationId": "TXN-...", "causationId": "...",
 *   "occurredAt": "2026-10-28T04:30:15.412Z",
 *   "body": { ...fields of this event type, see EventBodies... }
 * }
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonPropertyOrder({"eventId", "eventType", "eventVersion", "source", "correlationId", "causationId", "occurredAt", "body"})
public class EventMessage {

    /** Unique id of this event. */
    private UUID eventId;

    /** What happened. Kept as a String so an unknown type never breaks deserialization. */
    private String eventType;

    /** Version of the body schema for this eventType. */
    private int eventVersion;

    /** Service that published the event (spring.application.name). */
    private String source;

    /** Business id that links all events of one transfer = transactionRef = Kafka key. */
    private String correlationId;

    /** eventId of the event that caused this one (null for the first event). */
    private UUID causationId;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    /** Event-specific data. A JSON object whose shape depends on eventType. */
    private JsonNode body;
}

package com.example.gl.messaging;

/** The event body cannot be read. Retrying will not help, so the error handler does not retry it. */
public class MalformedEventException extends RuntimeException {

    public MalformedEventException(String message, Throwable cause) {
        super(message, cause);
    }
}

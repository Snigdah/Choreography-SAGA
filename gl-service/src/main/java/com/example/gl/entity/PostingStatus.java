package com.example.gl.entity;

public enum PostingStatus {
    POSTED,     // entries booked
    FAILED,     // posting rule failed, nothing booked
    REVERSED,   // original posting undone by a REVERSAL journal
    CANCELLED   // compensation arrived before posting - posting is blocked
}

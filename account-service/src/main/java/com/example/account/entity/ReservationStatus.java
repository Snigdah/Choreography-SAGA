package com.example.account.entity;

public enum ReservationStatus {
    RESERVED,    // money on hold
    CONFIRMED,   // Saga completed - hold becomes final
    RELEASED     // compensation - hold removed
}

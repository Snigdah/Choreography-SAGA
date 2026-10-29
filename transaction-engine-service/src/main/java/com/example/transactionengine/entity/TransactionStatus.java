package com.example.transactionengine.entity;

public enum TransactionStatus {
    PENDING,        // Saga running
    COMPLETED,      // every step succeeded
    COMPENSATING,   // a step failed, earlier steps are being undone
    FAILED          // a step failed and all compensations finished
}

package com.example.transactionengine.dto.response;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
public class TransactionResponse {

    private UUID id;
    private String transactionRef;
    private String flowType;
    private String debitAccount;
    private String creditAccount;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String statusReason;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}

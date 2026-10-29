package com.example.account.dto.response;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
public class ReservationResponse {

    private UUID id;
    private String transactionRef;
    private String accountNumber;
    private BigDecimal reservedAmount;
    private String currency;
    private String status;
    private String reason;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}

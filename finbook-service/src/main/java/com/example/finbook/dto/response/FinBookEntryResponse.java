package com.example.finbook.dto.response;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
public class FinBookEntryResponse {

    private UUID id;
    private String transactionRef;
    private BigDecimal amount;
    private String currency;
    private String entryType;
    private String status;
    private String reason;
    private LocalDateTime createdAt;
}

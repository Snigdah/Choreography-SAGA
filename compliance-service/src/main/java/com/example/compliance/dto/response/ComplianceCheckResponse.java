package com.example.compliance.dto.response;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
public class ComplianceCheckResponse {

    private UUID id;
    private String transactionRef;
    private BigDecimal amount;
    private String decision;
    private String ruleCode;
    private String reason;
    private LocalDateTime checkedAt;
}

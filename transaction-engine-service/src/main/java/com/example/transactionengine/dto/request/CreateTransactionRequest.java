package com.example.transactionengine.dto.request;

import com.example.transactionengine.messaging.event.FlowType;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class CreateTransactionRequest {

    /** SEQUENTIAL = Scenario 1, PARALLEL = Scenario 2. Defaults to SEQUENTIAL. */
    private FlowType flowType;

    @NotBlank
    @Size(max = 50)
    private String debitAccount;

    @NotBlank
    @Size(max = 50)
    private String creditAccount;

    @NotNull
    @DecimalMin("0.01")
    private BigDecimal amount;

    @NotBlank
    @Size(min = 3, max = 3)
    private String currency;
}

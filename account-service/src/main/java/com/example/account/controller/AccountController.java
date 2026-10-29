package com.example.account.controller;

import com.example.account.dto.response.ReservationResponse;
import com.example.account.service.AccountService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    @GetMapping("/reservations/{transactionRef}")
    public ReservationResponse getReservation(@PathVariable String transactionRef) {
        return accountService.getReservation(transactionRef);
    }
}

package com.example.gl.controller;

import com.example.gl.dto.response.GlPostingResponse;
import com.example.gl.service.GlService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/gl")
@RequiredArgsConstructor
public class GlController {

    private final GlService glService;

    /** Returns the POSTING journal and, if compensated, its REVERSAL journal. */
    @GetMapping("/postings/{transactionRef}")
    public List<GlPostingResponse> getPostings(@PathVariable String transactionRef) {
        return glService.getPostings(transactionRef);
    }
}

package com.recommend.controller;

import com.recommend.dto.AiChatRequest;
import com.recommend.dto.ApiResponse;
import com.recommend.entity.AiConversation;
import com.recommend.repository.UserRepository;
import com.recommend.service.AiService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/ai")
@RequiredArgsConstructor
public class AiController {

    private final AiService aiService;
    private final UserRepository userRepository;

    @PostMapping("/chat")
    public ApiResponse<Map<String, String>> chat(@Valid @RequestBody AiChatRequest request,
                                                  @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = userRepository.findByUsername(userDetails.getUsername()).orElseThrow().getId();
        String answer = aiService.chat(userId, request.getQuestion());
        return ApiResponse.success(Map.of("answer", answer));
    }

    @PostMapping("/summarize/{materialId}")
    public ApiResponse<Map<String, String>> summarize(@PathVariable Long materialId,
                                                       @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = userRepository.findByUsername(userDetails.getUsername()).orElseThrow().getId();
        String summary = aiService.summarize(userId, materialId);
        return ApiResponse.success(Map.of("summary", summary));
    }

    @PostMapping("/suggestions")
    public ApiResponse<Map<String, String>> suggestions(@AuthenticationPrincipal UserDetails userDetails) {
        Long userId = userRepository.findByUsername(userDetails.getUsername()).orElseThrow().getId();
        String suggestions = aiService.getSuggestions(userId);
        return ApiResponse.success(Map.of("suggestions", suggestions));
    }

    @GetMapping("/search")
    public ApiResponse<Map<String, Object>> semanticSearch(@RequestParam String q,
                                                            @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = userRepository.findByUsername(userDetails.getUsername()).orElseThrow().getId();
        return ApiResponse.success(aiService.semanticSearch(userId, q));
    }

    @GetMapping("/history")
    public ApiResponse<List<AiConversation>> getHistory(@AuthenticationPrincipal UserDetails userDetails) {
        Long userId = userRepository.findByUsername(userDetails.getUsername()).orElseThrow().getId();
        return ApiResponse.success(aiService.getHistory(userId));
    }
}

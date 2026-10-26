package com.recommend.controller;

import com.recommend.dto.ApiResponse;
import com.recommend.entity.Material;
import com.recommend.repository.UserRepository;
import com.recommend.service.RecommendationService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/recommendations")
@RequiredArgsConstructor
public class RecommendationController {

    private final RecommendationService recommendationService;
    private final UserRepository userRepository;

    @GetMapping("/public")
    public ApiResponse<List<Material>> getPublicRecommendations() {
        return ApiResponse.success(recommendationService.getGuestRecommendations());
    }

    @GetMapping
    public ApiResponse<List<Material>> getRecommendations(@AuthenticationPrincipal UserDetails userDetails) {
        Long userId = userRepository.findByUsername(userDetails.getUsername()).orElseThrow().getId();
        return ApiResponse.success(recommendationService.getRecommendations(userId));
    }

    @PostMapping("/refresh")
    public ApiResponse<Void> refresh(@AuthenticationPrincipal UserDetails userDetails) {
        Long userId = userRepository.findByUsername(userDetails.getUsername()).orElseThrow().getId();
        recommendationService.refreshForUser(userId);
        return ApiResponse.success();
    }
}

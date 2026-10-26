package com.recommend.repository;

import com.recommend.entity.RecommendationFeedback;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface RecommendationFeedbackRepository extends JpaRepository<RecommendationFeedback, Long> {

    Optional<RecommendationFeedback> findByUserIdAndMaterialId(Long userId, Long materialId);
}

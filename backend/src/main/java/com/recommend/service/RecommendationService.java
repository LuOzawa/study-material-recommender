package com.recommend.service;

import com.recommend.entity.Material;
import com.recommend.entity.Recommendation;
import com.recommend.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class RecommendationService {

    private final RecommendationRepository recommendationRepository;
    private final MaterialRepository materialRepository;
    private final UserPreferenceService preferenceService;

    @Value("${recommendation.python-script}")
    private String pythonScript;

    @Value("${recommendation.top-n:10}")
    private int topN;

    public List<Material> getRecommendations(Long userId) {
        UserPreferenceService.PreferenceData preference = preferenceService.getPreferenceData(userId);
        int fetchSize = Math.max(topN, preference.topN());

        List<Recommendation> recs = recommendationRepository
                .findByUserIdOrderByScoreDesc(userId, PageRequest.of(0, fetchSize));

        if (recs.isEmpty()) {
            List<Material> fallback = applyPreference(getHotMaterials(fetchSize), preference);
            fallback.forEach(material -> {
                material.setRecommendationAlgorithm("HOT");
                material.setRecommendationScore(null);
            });
            return fallback;
        }

        List<Long> materialIds = recs.stream()
                .map(Recommendation::getMaterialId)
                .collect(Collectors.toList());

        Map<Long, Material> materialMap = materialRepository.findActiveByIds(materialIds).stream()
                .collect(Collectors.toMap(Material::getId, m -> m, (a, b) -> a));

        List<Material> materials = materialIds.stream()
                .map(materialMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        Map<Long, Recommendation> recommendationMap = recs.stream()
                .collect(Collectors.toMap(
                        Recommendation::getMaterialId,
                        rec -> rec,
                        (a, b) -> a
                ));

        materials.forEach(material -> {
            Recommendation rec = recommendationMap.get(material.getId());
            if (rec == null) {
                return;
            }
            if (rec.getAlgorithm() != null) {
                material.setRecommendationAlgorithm(rec.getAlgorithm().name());
            }
            material.setRecommendationScore(rec.getScore());
        });

        return applyPreference(materials, preference);
    }

    /**
     * Public recommendation list for guests (non-personalized hot materials).
     */
    public List<Material> getGuestRecommendations() {
        List<Material> materials = getHotMaterials(topN);
        materials.forEach(material -> {
            material.setRecommendationAlgorithm("HOT");
            material.setRecommendationScore(null);
        });
        return materials;
    }

    private List<Material> getHotMaterials(int size) {
        return materialRepository.findTopByViewCount(PageRequest.of(0, size));
    }

    private List<Material> applyPreference(List<Material> materials,
                                           UserPreferenceService.PreferenceData preference) {
        if (materials == null || materials.isEmpty()) {
            return materials;
        }

        List<ScoredMaterial> scored = new java.util.ArrayList<>();
        for (int i = 0; i < materials.size(); i++) {
            Material material = materials.get(i);
            double score = materials.size() - i;

            if (material.getCategoryId() != null && preference.preferredCategories().contains(material.getCategoryId())) {
                score += 2.0;
            }

            java.util.Set<String> tags = parseTags(material.getTags());
            int preferredMatches = intersectionSize(tags, preference.preferredTags());
            int excludedMatches = intersectionSize(tags, preference.excludedTags());

            score += preferredMatches * 0.9;
            if (excludedMatches > 0) {
                score -= excludedMatches * 2.0;
            }

            scored.add(new ScoredMaterial(material, score, i));
        }

        return scored.stream()
                .sorted((a, b) -> {
                    int byScore = Double.compare(b.score, a.score);
                    if (byScore != 0) return byScore;
                    return Integer.compare(a.originalIndex, b.originalIndex);
                })
                .limit(preference.topN())
                .map(item -> item.material)
                .collect(Collectors.toList());
    }

    private java.util.Set<String> parseTags(String rawTags) {
        if (rawTags == null || rawTags.isBlank()) {
            return java.util.Set.of();
        }
        return java.util.Arrays.stream(rawTags.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
    }

    private int intersectionSize(java.util.Set<String> a, java.util.Set<String> b) {
        if (a == null || b == null || a.isEmpty() || b.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (String item : a) {
            if (b.contains(item)) {
                count++;
            }
        }
        return count;
    }

    private static class ScoredMaterial {
        private final Material material;
        private final double score;
        private final int originalIndex;

        private ScoredMaterial(Material material, double score, int originalIndex) {
            this.material = material;
            this.score = score;
            this.originalIndex = originalIndex;
        }
    }

    /**
     * Trigger recommendation refresh for a specific user via Python script.
     */
    public void refreshForUser(Long userId) {
        runPythonScript("--user-id", String.valueOf(userId));
    }

    /**
     * Nightly full recommendation refresh (2:00 AM).
     */
    @Scheduled(cron = "0 0 2 * * ?")
    public void scheduledRefresh() {
        log.info("Starting scheduled recommendation refresh...");
        runPythonScript();
    }

    private void runPythonScript(String... args) {
        try {
            String[] cmd = new String[2 + args.length];
            cmd[0] = "python";
            cmd[1] = resolvePythonScriptPath();
            System.arraycopy(args, 0, cmd, 2, args.length);

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            Process process = pb.start();

            // Read output in background thread to prevent blocking
            Thread reader = new Thread(() -> {
                try (var reader2 = process.inputReader()) {
                    reader2.lines().forEach(line -> log.info("[python] {}", line));
                } catch (Exception ignored) {}
            });
            reader.setDaemon(true);
            reader.start();

            int exit = process.waitFor();
            if (exit != 0) {
                log.warn("Python recommendation script exited with code {}", exit);
            }
        } catch (Exception e) {
            log.error("Failed to run recommendation script: {}", e.getMessage());
        }
    }

    private String resolvePythonScriptPath() {
        Path configured = Path.of(pythonScript);
        if (configured.isAbsolute() && Files.exists(configured)) {
            return configured.toString();
        }

        Path userDir = Path.of(System.getProperty("user.dir"));

        Path fromUserDir = userDir.resolve(pythonScript).normalize();
        if (Files.exists(fromUserDir)) {
            return fromUserDir.toString();
        }

        Path inCurrentRoot = userDir.resolve("recommendation").resolve("run_recommendation.py").normalize();
        if (Files.exists(inCurrentRoot)) {
            return inCurrentRoot.toString();
        }

        Path parent = userDir.getParent();
        if (parent != null) {
            Path inParentRoot = parent.resolve("recommendation").resolve("run_recommendation.py").normalize();
            if (Files.exists(inParentRoot)) {
                return inParentRoot.toString();
            }
        }

        log.warn("Recommendation script path not found by fallback. Using raw config: {}", pythonScript);
        return pythonScript;
    }
}

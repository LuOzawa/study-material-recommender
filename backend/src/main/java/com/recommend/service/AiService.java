package com.recommend.service;

import com.recommend.entity.AiConversation;
import com.recommend.entity.Material;
import com.recommend.entity.User;
import com.recommend.repository.AiConversationRepository;
import com.recommend.repository.MaterialRepository;
import com.recommend.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.*;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class AiService {

    private final AiConversationRepository conversationRepository;
    private final MaterialRepository materialRepository;
    private final UserRepository userRepository;

    @Value("${deepseek.api.key}")
    private String apiKey;

    @Value("${deepseek.api.url}")
    private String apiUrl;

    @Value("${deepseek.api.model}")
    private String model;

    @Value("${deepseek.api.timeout:60000}")
    private int timeoutMs;

    private RestTemplate restTemplate;

    /**
     * 原实现直接 new RestTemplate()，默认无连接/读取超时。
     * DeepSeek 一旦不响应，请求线程会一直挂着，很容易把 Tomcat 线程池耗尽。
     * 这里把 application.yml 里早已声明却未使用的 deepseek.api.timeout 接上。
     */
    @jakarta.annotation.PostConstruct
    void initRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(timeoutMs);
        factory.setReadTimeout(timeoutMs);
        this.restTemplate = new RestTemplate(factory);
    }

    /**
     * 未配置 API Key 时不发请求，直接返回提示，避免每次调用都白等一个 401。
     */
    private boolean isApiKeyConfigured() {
        return apiKey != null && !apiKey.isBlank();
    }

    /**
     * Chat Q&A — general graduate school questions.
     */
    public String chat(Long userId, String question) {
        String systemPrompt = "你是一个专业的考研学习助手，熟悉各科考研知识、备考策略和学习方法。请用中文简洁专业地回答用户的问题。";
        String answer = callDeepSeek(systemPrompt, question);
        saveConversation(userId, question, answer, AiConversation.ConvType.CHAT, null);
        return answer;
    }

    /**
     * Summarize a material's content.
     */
    public String summarize(Long userId, Long materialId) {
        Material material = materialRepository.findById(materialId)
                .orElseThrow(() -> new IllegalArgumentException("资料不存在"));

        String prompt = String.format(
                "请为以下考研学习资料生成一段简洁的内容摘要（150字以内）：\n\n标题：%s\n\n简介：%s\n\n标签：%s",
                material.getTitle(),
                Optional.ofNullable(material.getDescription()).orElse("无简介"),
                Optional.ofNullable(material.getTags()).orElse("无标签")
        );

        String systemPrompt = "你是一个学术内容摘要专家，擅长提取学习资料的核心内容。";
        String answer = callDeepSeek(systemPrompt, prompt);
        saveConversation(userId, prompt, answer, AiConversation.ConvType.SUMMARY, materialId);
        return answer;
    }

    /**
     * Generate personalized learning suggestions based on user profile.
     */
    public String getSuggestions(Long userId) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("用户不存在"));

        String prompt = String.format(
                "用户备考信息：\n目标院校：%s\n目标专业：%s\n备考阶段：%s\n\n请根据该用户的备考情况，给出个性化的备考建议，包括学习计划、重点复习方向和推荐备考策略。",
                Optional.ofNullable(user.getTargetSchool()).orElse("未设置"),
                Optional.ofNullable(user.getMajor()).orElse("未设置"),
                Optional.ofNullable(user.getStudyPhase()).map(Enum::name).orElse("PREPARATION")
        );

        String systemPrompt = "你是一位经验丰富的考研辅导专家，了解各高校专业课要求，能够给出针对性的备考建议。";
        String answer = callDeepSeek(systemPrompt, prompt);
        saveConversation(userId, prompt, answer, AiConversation.ConvType.SUGGESTION, null);
        return answer;
    }

    /**
     * Semantic search — use AI to interpret and enhance the search query.
     */
    public Map<String, Object> semanticSearch(Long userId, String query) {
        String prompt = String.format(
                "用户在考研学习资料平台搜索：\"%s\"\n\n请分析该搜索意图，列出5个相关的考研知识点或资料关键词（逗号分隔），帮助扩展搜索范围。",
                query
        );
        String systemPrompt = "你是考研知识图谱专家，擅长理解考研学习需求。";
        String keywordsText = callDeepSeek(systemPrompt, prompt);
        List<String> keywords = extractKeywords(keywordsText);

        LinkedHashMap<Long, Material> matchedMap = new LinkedHashMap<>();
        for (String keyword : keywords) {
            materialRepository.searchActive(keyword, PageRequest.of(0, 5))
                    .forEach(material -> matchedMap.putIfAbsent(material.getId(), material));
            if (matchedMap.size() >= 10) {
                break;
            }
        }

        List<Map<String, Object>> matchedMaterials = matchedMap.values().stream()
                .limit(10)
                .map(m -> {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("id", m.getId());
                    item.put("title", m.getTitle());
                    item.put("tags", m.getTags());
                    item.put("description", m.getDescription());
                    return item;
                })
                .toList();

        saveConversation(userId, query, keywordsText, AiConversation.ConvType.SEARCH, null);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("keywords", keywords);
        result.put("materials", matchedMaterials);
        return result;
    }

    /**
     * Get conversation history for a user.
     */
    public List<AiConversation> getHistory(Long userId) {
        return conversationRepository.findByUserIdOrderByCreatedAtDesc(userId,
                org.springframework.data.domain.PageRequest.of(0, 20));
    }

    // -----------------------------------------------------------------------
    // Internal: call DeepSeek API
    // -----------------------------------------------------------------------

    private String callDeepSeek(String systemPrompt, String userMessage) {
        if (!isApiKeyConfigured()) {
            return "AI 功能未启用：请在服务端配置环境变量 DEEPSEEK_API_KEY。";
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(apiKey);

        Map<String, Object> body = new HashMap<>();
        body.put("model", model);
        body.put("messages", List.of(
                Map.of("role", "system", "content", systemPrompt),
                Map.of("role", "user", "content", userMessage)
        ));
        body.put("temperature", 0.7);
        body.put("max_tokens", 1024);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(body, headers);

        try {
            ResponseEntity<Map> response = restTemplate.exchange(apiUrl, HttpMethod.POST, entity, Map.class);
            Map responseBody = response.getBody();
            if (responseBody != null && responseBody.containsKey("choices")) {
                List choices = (List) responseBody.get("choices");
                if (!choices.isEmpty()) {
                    Map choice = (Map) choices.get(0);
                    Map message = (Map) choice.get("message");
                    return (String) message.get("content");
                }
            }
        } catch (Exception e) {
            log.error("DeepSeek API call failed: {}", e.getMessage());
            return "AI服务暂时不可用，请稍后再试。";
        }
        return "未能获取AI响应，请重试。";
    }

    private List<String> extractKeywords(String rawText) {
        if (rawText == null || rawText.isBlank()) {
            return List.of();
        }
        return Arrays.stream(rawText.replace("，", ",").replace("\n", ",").split(","))
                .map(String::trim)
                .filter(token -> !token.isBlank())
                .distinct()
                .limit(8)
                .toList();
    }

    private void saveConversation(Long userId, String question, String answer,
                                   AiConversation.ConvType type, Long materialId) {
        AiConversation conv = new AiConversation();
        conv.setUserId(userId);
        conv.setQuestion(question);
        conv.setAnswer(answer);
        conv.setType(type);
        conv.setMaterialId(materialId);
        conversationRepository.save(conv);
    }
}

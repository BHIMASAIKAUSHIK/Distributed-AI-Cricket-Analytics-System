package com.cricket.service;

import io.github.ollama4j.OllamaAPI;
import io.github.ollama4j.models.OllamaResult;
import io.github.ollama4j.utils.Options;

import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Spring service that wraps the Ollama4j client.
 *
 * Injected into CricketWorkerActor at construction time via AkkaConfig,
 * because actors cannot use Spring @Autowired directly — dependencies
 * must be passed as constructor arguments.
 *
 * Usage inside CricketWorkerActor:
 *   context().pipeToSelf(
 *       CompletableFuture.supplyAsync(() -> ollamaService.analyzeCricket(query)),
 *       (result, ex) -> ...
 *   );
 */
@Service
public class OllamaService {

    private static final Logger log = LoggerFactory.getLogger(OllamaService.class);

    private final OllamaAPI ollamaAPI;
    private final String model;

    public OllamaService(
            @Value("${ollama.base-url:http://localhost:11434}") String baseUrl,
            @Value("${ollama.model:llama3.2}") String model,
            @Value("${ollama.timeout-seconds:120}") long timeoutSeconds) {

        this.ollamaAPI = new OllamaAPI(baseUrl);
        this.ollamaAPI.setRequestTimeoutSeconds(timeoutSeconds);
        this.model = model;

        log.info("OllamaService initialized — baseUrl={} model={}", baseUrl, model);
    }

    /**
     * Sends a cricket analytics prompt to Llama 3.2 and returns the response text.
     * This is a blocking call; the caller (CricketWorkerActor) runs it on a
     * separate thread using CompletableFuture + pipeToSelf.
     */
    public String analyzeCricket(String query) {
        String prompt = """
                You are an expert cricket analyst with deep knowledge of statistics,
                player performance, match strategies, and historical data.

                Analyze the following query and provide detailed insights:

                Query: %s

                Provide a structured response with key insights and statistics.
                """.formatted(query);

        log.info("Sending to Ollama [model={}]: {}", model, query);

        try {
            OllamaResult result = ollamaAPI.generate(model, prompt, false, new Options(new HashMap<>()));
            String response = result.getResponse();
            log.info("Ollama response received ({} chars)", response.length());
            return response;
        } catch (Exception e) {
            log.error("Ollama call failed: {}", e.getMessage(), e);
            throw new RuntimeException("Ollama analysis failed: " + e.getMessage(), e);
        }
    }

    public String getModel() {
        return model;
    }
}

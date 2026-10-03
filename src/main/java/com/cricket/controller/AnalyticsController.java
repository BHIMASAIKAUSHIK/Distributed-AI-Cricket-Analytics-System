package com.cricket.controller;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import com.cricket.actors.SupervisorActor;
import com.cricket.dto.AnalysisRequest;
import com.cricket.dto.AnalysisResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * REST entry point — bridges Spring MVC and the Akka actor system.
 *
 * Message Flow initiated here:
 *
 *   POST /api/cricket/analyze
 *     │
 *     │  AskPattern.ask(actorSystem, AnalyzeQuery)  ← REST → Actor boundary
 *     ▼
 *   SupervisorActor  [ActorSystem guardian]
 *     │  context.ask(RouterActor)                   ← ASK PATTERN
 *     ▼
 *   RouterActor
 *     │  workerActor.tell(ProcessQuery)             ← FORWARD PATTERN
 *     ▼
 *   CricketWorkerActor  [Ollama + Persist]
 *     │  replyTo.tell(AnalysisResponse)
 *     ▼
 *   CompletionStage<AnalysisResponse>  →  ResponseEntity
 *
 * Note: CompletionStage return type makes Spring MVC handle this
 * asynchronously — the HTTP thread is NOT blocked during Ollama call.
 */

@RestController
@RequestMapping("/api/cricket")
public class AnalyticsController {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsController.class);

    private final ActorSystem<SupervisorActor.Command> actorSystem;
    private final Duration askTimeout;

    public AnalyticsController(
            ActorSystem<SupervisorActor.Command> actorSystem,
            @Value("${akka.ask.timeout-seconds:60}") long timeoutSeconds) {
        this.actorSystem = actorSystem;
        this.askTimeout = Duration.ofSeconds(timeoutSeconds);
    }

    /**
     * Main analytics endpoint.
     *
     * Example:
     *   curl -X POST http://localhost:8080/api/cricket/analyze \
     *        -H "Content-Type: application/json" \
     *        -d '{"query":"Analyze Rohit Sharma batting in ODI World Cup 2023"}'
     */
    @PostMapping("/analyze")
    public CompletionStage<ResponseEntity<AnalysisResponse>> analyze(
            @RequestBody AnalysisRequest request) {

        log.info("REST received query: '{}'", request.getQuery());

        // AskPattern.ask sends AnalyzeQuery to the ActorSystem guardian (SupervisorActor)
        // and returns a CompletionStage that completes when SupervisorActor replies.
        // Explicit type parameter <AnalysisResponse> required for Java type inference.
        CompletionStage<AnalysisResponse> future = AskPattern.<SupervisorActor.Command, AnalysisResponse>ask(
                actorSystem,
                replyTo -> new SupervisorActor.Command.AnalyzeQuery(request.getQuery(), replyTo),
                askTimeout,
                actorSystem.scheduler()
        );

        return future.thenApply(response -> {
            if (response.success()) {
                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.internalServerError().body(response);
            }
        });
    }

    /**
     * Health check for the actor system.
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("ActorSystem: " + actorSystem.name()
                + " | State: " + actorSystem.whenTerminated().isCompleted());
    }
}

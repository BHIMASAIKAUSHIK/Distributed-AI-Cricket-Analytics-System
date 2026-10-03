package com.cricket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * ╔══════════════════════════════════════════════════════════════════╗
 *  Distributed AI Cricket Analytics System
 *  University Course Project — Spring 2025
 *
 *  Tech Stack:
 *    - Java 21 (records, sealed interfaces, text blocks)
 *    - Spring Boot 3.3 (REST API, DI, Actuator)
 *    - Akka Typed 2.6.21 (Cluster, Persistence, Actor hierarchy)
 *    - Ollama + Llama 3.2 (AI analysis)
 *    - H2 (JDBC persistence journal for dev)
 *
 *  Architecture — Actor Hierarchy (all on same JVM, Req #1):
 *
 *    ActorSystem "CricketAnalyticsSystem"
 *      └── SupervisorActor  (guardian, handles Ask pattern)
 *            └── RouterActor  (handles Forward pattern)
 *                  └── CricketWorkerActor  (EventSourcedBehavior + Ollama)
 *
 *  Full Message Flow (Req #2, #3, #5):
 *
 *    POST /api/cricket/analyze
 *      │
 *      │ AskPattern.ask(actorSystem)
 *      ▼
 *    SupervisorActor.AnalyzeQuery
 *      │
 *      │ context.ask(routerActor)          ← ASK PATTERN  (Req #5)
 *      ▼
 *    RouterActor.RouteQuery
 *      │
 *      │ workerActor.tell(ProcessQuery)    ← FORWARD PATTERN (Req #5)
 *      ▼
 *    CricketWorkerActor.ProcessQuery
 *      │
 *      │ pipeToSelf(Ollama future)
 *      │ Effect().persist(QueryProcessed)  ← AKKA PERSISTENCE (Req #6)
 *      │
 *      ▼ replyTo.tell(AnalysisResponse)
 *    SupervisorActor callback
 *      │
 *      ▼ originalReplyTo.tell(response)
 *    REST caller receives JSON
 *
 *  Running 3 Cluster Nodes (Req #4) on same machine:
 *
 *    Node 1 (default):  java -jar app.jar
 *    Node 2:            java -jar app.jar -Dakka.node.port=2552 -Dserver.port=8081
 *    Node 3:            java -jar app.jar -Dakka.node.port=2553 -Dserver.port=8082
 * ╚══════════════════════════════════════════════════════════════════╝
 */
@SpringBootApplication
public class CricketAnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(CricketAnalyticsApplication.class, args);
    }
}

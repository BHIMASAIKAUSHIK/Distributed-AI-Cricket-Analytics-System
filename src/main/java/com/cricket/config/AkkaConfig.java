package com.cricket.config;

import akka.actor.typed.ActorSystem;
import com.cricket.actors.SupervisorActor;
import com.cricket.service.OllamaService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Spring @Configuration that bootstraps the Akka ActorSystem.
 *
 * Why not use @Autowired in actors?
 *   Akka creates actors inside its own runtime — Spring's DI container
 *   has no hook into that process. The solution is to pass Spring beans
 *   as constructor arguments through the actor hierarchy:
 *
 *     AkkaConfig
 *       → injects OllamaService (Spring bean)
 *       → passes to SupervisorActor.create(ollamaService)
 *         → SupervisorActor passes to RouterActor.create(ollamaService)
 *           → RouterActor passes to CricketWorkerActor.create(ollamaService, "1")
 *
 * Schema Initialization Ordering (critical):
 *   Spring Boot startup order for this config:
 *     1. DataSource bean created  (H2 in-memory "akkadb" created)
 *     2. spring.sql.init runs schema.sql  (event_journal + snapshot tables created)
 *     3. actorSystem(OllamaService, DataSource) bean created
 *        ↑ DataSource parameter forces Spring to complete steps 1+2 first
 *     4. Akka Persistence connects Slick to "akkadb" → tables already exist ✓
 *
 * Cluster Node Configuration:
 *   Override per node at startup:
 *     Node 1: java -jar app.jar                          (default: port 2551, HTTP 8080)
 *     Node 2: java -jar app.jar -Dakka.node.port=2552 -Dserver.port=8081
 *     Node 3: java -jar app.jar -Dakka.node.port=2553 -Dserver.port=8082
 */
@Configuration
public class AkkaConfig {

    private static final Logger log = LoggerFactory.getLogger(AkkaConfig.class);

    @Value("${akka.node.port:2551}")
    private int akkaNodePort;

    /**
     * Creates the Akka ActorSystem as a Spring bean.
     *
     * @param ollamaService injected by Spring; passed down through actor hierarchy
     * @param dataSource    injected to guarantee Spring has already run schema.sql
     *                      and created the event_journal + snapshot tables in H2
     *                      before Akka Persistence tries to connect.
     */
    @Bean(destroyMethod = "terminate")
    public ActorSystem<SupervisorActor.Command> actorSystem(OllamaService ollamaService,
                                                            DataSource dataSource) {
        log.info("Schema initialized (event_journal + snapshot tables exist in H2)");
        log.info("Bootstrapping Akka ActorSystem on cluster port {}", akkaNodePort);

        Config nodeConfig = ConfigFactory
                .parseString("akka.remote.artery.canonical.port=" + akkaNodePort)
                .withFallback(ConfigFactory.load());

        ActorSystem<SupervisorActor.Command> system = ActorSystem.create(
                SupervisorActor.create(ollamaService),
                "CricketAnalyticsSystem",
                nodeConfig
        );

        log.info("ActorSystem '{}' started on akka://CricketAnalyticsSystem@127.0.0.1:{}",
                system.name(), akkaNodePort);

        return system;
    }
}

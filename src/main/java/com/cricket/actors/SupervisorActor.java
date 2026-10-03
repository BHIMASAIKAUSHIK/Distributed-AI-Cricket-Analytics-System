package com.cricket.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.cricket.CricketSerializable;
import com.cricket.dto.AnalysisResponse;
import com.cricket.service.OllamaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;


 
public class SupervisorActor extends AbstractBehavior<SupervisorActor.Command> {

    public sealed interface Command extends CricketSerializable {

        record AnalyzeQuery(
                String query,
                ActorRef<AnalysisResponse> replyTo
        ) implements Command {}

        record WrappedResponse(
                AnalysisResponse response,
                ActorRef<AnalysisResponse> originalReplyTo,
                long startTimeMs
        ) implements Command {}

        record WrappedFailure(
                String reason,
                String query,
                ActorRef<AnalysisResponse> originalReplyTo,
                long startTimeMs
        ) implements Command {}
    }

    private static final Logger log = LoggerFactory.getLogger(SupervisorActor.class);
    private static final Duration ASK_TIMEOUT = Duration.ofSeconds(90);

    
    private final ActorRef<RouterActor.Command>  routerActor;
    private final ActorRef<LoggingActor.Command> loggingActor; 

    public static Behavior<Command> create(OllamaService ollamaService) {
        return Behaviors.setup(ctx -> new SupervisorActor(ctx, ollamaService));
    }

    private SupervisorActor(ActorContext<Command> context, OllamaService ollamaService) {
        super(context);

        this.routerActor = context.spawn(RouterActor.create(ollamaService), "router-actor");

        this.loggingActor = context.spawn(LoggingActor.create(), "logging-actor");

        log.info("SupervisorActor started.");
        log.info("Children: router={}, logger={}", routerActor.path(), loggingActor.path());
    }

    
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Command.AnalyzeQuery.class,    this::handleAnalyzeQuery)
                .onMessage(Command.WrappedResponse.class, this::handleWrappedResponse)
                .onMessage(Command.WrappedFailure.class,  this::handleWrappedFailure)
                .build();
    }

    private Behavior<Command> handleAnalyzeQuery(Command.AnalyzeQuery cmd) {
        long startMs = Instant.now().toEpochMilli();
        log.info("SupervisorActor received AnalyzeQuery: '{}'", cmd.query());

        
        loggingActor.tell(new LoggingActor.Command.QueryReceived(
                cmd.query(),
                getContext().getSelf().path().toString(),
                startMs
        ));

        
        getContext().ask(
                AnalysisResponse.class,
                routerActor,
                ASK_TIMEOUT,
                (ActorRef<AnalysisResponse> askAdapter) ->
                        new RouterActor.Command.RouteQuery(cmd.query(), askAdapter),
                (response, failure) -> {
                    if (failure != null) {
                        log.error("Ask to RouterActor failed: {}", failure.getMessage());
                        return new Command.WrappedFailure(
                                failure.getMessage(), cmd.query(), cmd.replyTo(), startMs);
                    }
                    return new Command.WrappedResponse(response, cmd.replyTo(), startMs);
                }
        );

        return this;
    }

    private Behavior<Command> handleWrappedResponse(Command.WrappedResponse msg) {
        long durationMs = Instant.now().toEpochMilli() - msg.startTimeMs();
        log.info("SupervisorActor forwarding successful result to REST caller ({}ms)", durationMs);

        
        loggingActor.tell(new LoggingActor.Command.QueryCompleted(
                msg.response().query(), true, durationMs));

        msg.originalReplyTo().tell(msg.response());
        return this;
    }

    private Behavior<Command> handleWrappedFailure(Command.WrappedFailure msg) {
        long durationMs = Instant.now().toEpochMilli() - msg.startTimeMs();
        log.error("Analysis failed for '{}': {}", msg.query(), msg.reason());

        loggingActor.tell(new LoggingActor.Command.QueryCompleted(
                msg.query(), false, durationMs));

        msg.originalReplyTo().tell(
                AnalysisResponse.failure(msg.query(), msg.reason(),
                        getContext().getSelf().path().toString(), "N/A")
        );
        return this;
    }
}

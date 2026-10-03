package com.cricket.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.*;
import com.cricket.CricketSerializable;
import com.cricket.dto.AnalysisResponse;
import com.cricket.service.OllamaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CricketWorkerActor
        extends EventSourcedBehavior<
                CricketWorkerActor.Command,
                CricketWorkerActor.Event,
                CricketWorkerActor.State> {

    private static final Logger log = LoggerFactory.getLogger(CricketWorkerActor.class);

    
    public sealed interface Command extends CricketSerializable {

       
        record ProcessQuery(
                String query,
                ActorRef<AnalysisResponse> replyTo
        ) implements Command {}

        record OllamaSucceeded(
                String query,
                String result,
                ActorRef<AnalysisResponse> replyTo
        ) implements Command {}

        record OllamaFailed(
                String query,
                String reason,
                ActorRef<AnalysisResponse> replyTo
        ) implements Command {}
    }

    // ----------------------------------------------------------------
    //  Events (persisted to JDBC journal)
    // ----------------------------------------------------------------
    public sealed interface Event extends CricketSerializable {

        /** Written to journal every time a query is successfully processed. */
        record QueryProcessed(
                String query,
                String result,
                String persistenceId,
                long timestampEpochMs
        ) implements Event {}

        /** Written to journal when a query fails (for audit trail). */
        record QueryFailed(
                String query,
                String reason,
                long timestampEpochMs
        ) implements Event {}
    }

    // ----------------------------------------------------------------
    //  State (rebuilt from events on actor restart)
    // ----------------------------------------------------------------
    public record State(List<String> processedQueries) implements CricketSerializable {

        public static State empty() {
            return new State(Collections.emptyList());
        }

        public State withProcessed(String query) {
            var updated = new ArrayList<>(processedQueries);
            updated.add(query);
            return new State(Collections.unmodifiableList(updated));
        }

        public int totalProcessed() {
            return processedQueries.size();
        }
    }

    // ----------------------------------------------------------------
    //  Fields
    // ----------------------------------------------------------------
    private final OllamaService ollamaService;
    private final String workerId;
    /**
     * ActorContext captured via Behaviors.setup() wrapper in create().
     * EventSourcedBehavior does not expose context() in its Java API,
     * so we capture it explicitly at setup time for use in pipeToSelf.
     */
    private final ActorContext<Command> actorContext;

    // ----------------------------------------------------------------
    //  Factory  — Behaviors.setup wrapper captures ActorContext
    // ----------------------------------------------------------------
    public static Behavior<Command> create(OllamaService ollamaService, String workerId) {
        // Behaviors.setup gives us an ActorContext at initialization time.
        // We pass it into the EventSourcedBehavior constructor so we can
        // call actorContext.pipeToSelf() for the async Ollama call.
        return Behaviors.setup(ctx ->
                new CricketWorkerActor(ollamaService, workerId, ctx));
    }

    private CricketWorkerActor(OllamaService ollamaService, String workerId,
                                ActorContext<Command> ctx) {
        // PersistenceId must be unique per actor instance (survives restarts)
        super(PersistenceId.ofUniqueId("cricket-worker-" + workerId));
        this.ollamaService = ollamaService;
        this.workerId = workerId;
        this.actorContext = ctx;

        log.info("[Worker-{}] Started. PersistenceId=cricket-worker-{}", workerId, workerId);
    }

    // ----------------------------------------------------------------
    //  EventSourcedBehavior contract
    // ----------------------------------------------------------------

    @Override
    public State emptyState() {
        return State.empty();
    }

    /**
     * Command handler: decides which Events to persist based on incoming Command.
     * Uses Effect() API:  Effect().persist(event).thenRun(sideEffect)
     */
    @Override
    public CommandHandler<Command, Event, State> commandHandler() {
        return newCommandHandlerBuilder()
                .forAnyState()
                .onCommand(Command.ProcessQuery.class,    this::handleProcessQuery)
                .onCommand(Command.OllamaSucceeded.class, this::handleOllamaSucceeded)
                .onCommand(Command.OllamaFailed.class,    this::handleOllamaFailed)
                .build();
    }

    /**
     * Step 1 – Start async Ollama call on a separate thread.
     * pipeToSelf() safely delivers the result back as a Command.
     * Returns Effect().none() — no event persisted yet.
     */
    private Effect<Event, State> handleProcessQuery(State state, Command.ProcessQuery cmd) {
        log.info("[Worker-{}] Received query #{}: {}", workerId,
                state.totalProcessed() + 1, cmd.query());

        // Run Ollama call off the actor thread — never block inside an actor
        CompletableFuture<String> ollamaFuture =
                CompletableFuture.supplyAsync(() -> ollamaService.analyzeCricket(cmd.query()));

        // pipeToSelf: when future completes, converts result into a Command
        // and delivers it to this actor's mailbox (thread-safe)
        actorContext.pipeToSelf(ollamaFuture, (result, ex) -> {
            if (ex != null) {
                return new Command.OllamaFailed(cmd.query(), ex.getMessage(), cmd.replyTo());
            }
            return new Command.OllamaSucceeded(cmd.query(), result, cmd.replyTo());
        });

        return Effect().none();  // No event yet; will persist in OllamaSucceeded handler
    }

    /**
     * Step 2a – Ollama succeeded: persist event, then reply to original caller.
     * Effect().persist() writes to journal, thenRun() sends the reply.
     */
    private Effect<Event, State> handleOllamaSucceeded(State state, Command.OllamaSucceeded cmd) {
        String pid = "cricket-worker-" + workerId;
        log.info("[Worker-{}] Ollama succeeded — persisting event", workerId);

        Event.QueryProcessed event = new Event.QueryProcessed(
                cmd.query(), cmd.result(), pid, Instant.now().toEpochMilli());

        return Effect()
                .persist(event)                    // ← written to H2 JDBC journal
                .thenRun(newState -> {
                    log.info("[Worker-{}] Event persisted. Total processed: {}",
                            workerId, newState.totalProcessed());
                    // Reply flows back up: Worker → ask adapter → SupervisorActor → REST
                    cmd.replyTo().tell(AnalysisResponse.success(
                            cmd.query(), cmd.result(), pid, pid));
                });
    }

    /**
     * Step 2b – Ollama failed: persist failure event, reply with error.
     */
    private Effect<Event, State> handleOllamaFailed(State state, Command.OllamaFailed cmd) {
        log.warn("[Worker-{}] Ollama failed: {}", workerId, cmd.reason());

        Event.QueryFailed event = new Event.QueryFailed(
                cmd.query(), cmd.reason(), Instant.now().toEpochMilli());

        String pid = "cricket-worker-" + workerId;
        return Effect()
                .persist(event)
                .thenRun(newState ->
                        cmd.replyTo().tell(AnalysisResponse.failure(cmd.query(), cmd.reason(), pid, pid))
                );
    }

    /**
     * Event handler: updates State from persisted Events.
     * Called both on new events AND during recovery (replay from journal).
     */
    @Override
    public EventHandler<State, Event> eventHandler() {
        return newEventHandlerBuilder()
                .forAnyState()
                .onEvent(Event.QueryProcessed.class,
                        (state, event) -> state.withProcessed(event.query()))
                .onEvent(Event.QueryFailed.class,
                        (state, event) -> state)   // state unchanged on failure
                .build();
    }
}

package com.cricket.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.cricket.CricketSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class LoggingActor extends AbstractBehavior<LoggingActor.Command> {

    private static final Logger log = LoggerFactory.getLogger(LoggingActor.class);

    // ----------------------------------------------------------------
    //  Commands — NO replyTo field (that's what makes it fire-and-forget)
    // ----------------------------------------------------------------
    public sealed interface Command extends CricketSerializable {

        /**
         * Fired when a new query enters the system.
         * Notice: no ActorRef<X> replyTo — sender expects nothing back.
         */
        record QueryReceived(
                String query,
                String senderNode,
                long timestampEpochMs
        ) implements Command {}

        /**
         * Fired when a query completes (success or failure).
         * Again: no replyTo field — pure fire-and-forget.
         */
        record QueryCompleted(
                String query,
                boolean success,
                long durationMs
        ) implements Command {}
    }

    // ----------------------------------------------------------------
    //  Internal state — counts logged events (visible in logs)
    // ----------------------------------------------------------------
    private int totalLogged = 0;

    // ----------------------------------------------------------------
    //  Factory
    // ----------------------------------------------------------------
    public static Behavior<Command> create() {
        return Behaviors.setup(LoggingActor::new);
    }

    private LoggingActor(ActorContext<Command> context) {
        super(context);
        log.info("[LoggingActor] Started — ready to receive fire-and-forget log events");
    }

    // ----------------------------------------------------------------
    //  Message handling
    // ----------------------------------------------------------------
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Command.QueryReceived.class,  this::handleQueryReceived)
                .onMessage(Command.QueryCompleted.class, this::handleQueryCompleted)
                .build();
    }

    private Behavior<Command> handleQueryReceived(Command.QueryReceived event) {
        totalLogged++;
        log.info("[LoggingActor] [FIRE-AND-FORGET] #{} Query received at {} from node '{}': \"{}\"",
                totalLogged,
                Instant.ofEpochMilli(event.timestampEpochMs()),
                event.senderNode(),
                event.query());
        return this;
    }

    private Behavior<Command> handleQueryCompleted(Command.QueryCompleted event) {
        totalLogged++;
        String status = event.success() ? "SUCCESS" : "FAILED";
        log.info("[LoggingActor] [FIRE-AND-FORGET] #{} Query {} in {}ms: \"{}\"",
                totalLogged, status, event.durationMs(), event.query());
        return this;
    }
}

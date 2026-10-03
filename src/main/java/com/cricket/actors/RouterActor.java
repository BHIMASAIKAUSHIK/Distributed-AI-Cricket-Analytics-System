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



public class RouterActor extends AbstractBehavior<RouterActor.Command> {

    
    public sealed interface Command extends CricketSerializable {

       
        record RouteQuery(
                String query,
                ActorRef<AnalysisResponse> replyTo     // ← preserved & forwarded
        ) implements Command {}
    }

   
    private static final Logger log = LoggerFactory.getLogger(RouterActor.class);

    private final ActorRef<CricketWorkerActor.Command> workerActor;

  
    public static Behavior<Command> create(OllamaService ollamaService) {
        return Behaviors.setup(ctx -> new RouterActor(ctx, ollamaService));
    }

    private RouterActor(ActorContext<Command> context, OllamaService ollamaService) {
        super(context);

       
        this.workerActor = context.spawn(
                CricketWorkerActor.create(ollamaService, "1"),
                "cricket-worker-1"
        );

        log.info("RouterActor started. Child worker spawned: {}",
                workerActor.path());
    }

  
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Command.RouteQuery.class, this::handleRouteQuery)
                .build();
    }

   
    private Behavior<Command> handleRouteQuery(Command.RouteQuery cmd) {
        log.info("RouterActor forwarding query to worker: '{}'", cmd.query());

       
        workerActor.tell(new CricketWorkerActor.Command.ProcessQuery(
                cmd.query(),
                cmd.replyTo()          
        ));

        return this;   
    }
}

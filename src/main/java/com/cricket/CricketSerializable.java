package com.cricket;

/**
 * Marker interface for Akka Jackson JSON serialization.
 *
 * Every class that crosses actor or cluster boundaries must implement this.
 * Configured in application.conf:
 *   akka.actor.serialization-bindings {
 *     "com.cricket.CricketSerializable" = jackson-json
 *   }
 *
 * Applies to:
 *  - Actor commands (SupervisorActor, RouterActor, CricketWorkerActor)
 *  - Persistence Events  (CricketWorkerActor.Event)
 *  - Persistence State   (CricketWorkerActor.State)
 *  - Response DTOs       (AnalysisResponse)
 */
public interface CricketSerializable {}

package com.cricket.dto;

import com.cricket.CricketSerializable;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Shared response DTO flowing through the entire pipeline:
 *
 *   CricketWorkerActor → RouterActor (via replyTo) → SupervisorActor → REST caller
 *
 * Implements CricketSerializable so Akka serializes it as JSON when it
 * crosses node boundaries in the cluster.
 */
public record AnalysisResponse(

        @JsonProperty("query")
        String query,

        @JsonProperty("analysis")
        String analysis,

        @JsonProperty("success")
        boolean success,

        @JsonProperty("errorMessage")
        String errorMessage,

        @JsonProperty("workerNode")
        String workerNode,           // which cluster node processed this

        @JsonProperty("persistenceId")
        String persistenceId         // Akka Persistence ID of the worker

) implements CricketSerializable {

    /** Factory for a successful analysis result. */
    public static AnalysisResponse success(String query, String analysis,
                                           String workerNode, String persistenceId) {
        return new AnalysisResponse(query, analysis, true, null, workerNode, persistenceId);
    }

    /** Factory for a failed analysis result. */
    public static AnalysisResponse failure(String query, String reason,
                                           String workerNode, String persistenceId) {
        return new AnalysisResponse(query, null, false, reason, workerNode, persistenceId);
    }
}

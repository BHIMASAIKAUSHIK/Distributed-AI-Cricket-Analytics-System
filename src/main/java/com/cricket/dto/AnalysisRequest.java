package com.cricket.dto;

/**
 * Incoming REST request body.
 *   POST /api/cricket/analyze
 *   { "query": "Analyze Virat Kohli's batting average in T20 World Cup 2024" }
 */
public class AnalysisRequest {

    private String query;

    public AnalysisRequest() {}

    public AnalysisRequest(String query) {
        this.query = query;
    }

    public String getQuery() { return query; }
    public void setQuery(String query) { this.query = query; }
}

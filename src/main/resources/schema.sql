-- Akka Persistence JDBC journal tables (H2 dialect)
-- Runs via spring.sql.init BEFORE the ActorSystem starts.

CREATE TABLE IF NOT EXISTS event_journal (
    ordering        BIGINT AUTO_INCREMENT,
    persistence_id  VARCHAR(255) NOT NULL,
    sequence_number BIGINT       NOT NULL,
    deleted         BOOLEAN      DEFAULT FALSE,
    tags            VARCHAR(255),
    message         BYTEA        NOT NULL,
    PRIMARY KEY (persistence_id, sequence_number)
);

CREATE UNIQUE INDEX IF NOT EXISTS event_journal_ordering_idx
    ON event_journal (ordering);

CREATE TABLE IF NOT EXISTS snapshot (
    persistence_id  VARCHAR(255) NOT NULL,
    sequence_number BIGINT       NOT NULL,
    created         BIGINT       NOT NULL,
    snapshot        BYTEA        NOT NULL,
    meta_ser_id     INTEGER,
    meta_payload    BYTEA,
    PRIMARY KEY (persistence_id, sequence_number)
);

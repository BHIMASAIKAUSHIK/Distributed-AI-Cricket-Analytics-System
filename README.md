# 🏏 Distributed AI Cricket Analytics System

A distributed, AI-powered cricket analytics system built with **Akka Typed Cluster**, **Spring Boot**, and **Ollama (Llama 3.2)**. The system accepts cricket queries via a REST API, processes them through a distributed actor hierarchy across a 3-node cluster, generates AI-driven responses using a local LLM, and persists every query as an event for full event-sourced recovery.

---

## 🛠️ Tech Stack

| Technology | Purpose |
| --- | --- |
| **Java 21** | Core language (records, sealed interfaces, text blocks) |
| **Akka Typed 2.6.21** | Actor system, cluster, persistence |
| **Spring Boot 3.3.5** | REST API framework, DI, Actuator |
| **Ollama + Llama 3.2** | Local LLM for AI responses |
| **H2 File Database** | Persistent event journal (dev) |
| **Akka Persistence JDBC** | Event sourcing journal |

---

## 🧩 Actor Hierarchy

```javascript
ActorSystem (CricketAnalyticsSystem)
└── SupervisorActor [Guardian/Root]
    ├── RouterActor [Dispatcher]
    │   └── CricketWorkerActor [Worker + Persistence]
    └── LoggingActor [Fire-and-Forget Logger]
```

### Actor Descriptions

**SupervisorActor** — Root actor of the system. Receives queries from the REST controller via the **ASK pattern** and uses `context.ask()` to communicate with the RouterActor. Handles responses and failures.

**RouterActor** — Sits between Supervisor and Worker. Demonstrates the **FORWARD pattern**: it passes the original `replyTo` reference unchanged to the WorkerActor and never replies itself — the worker replies directly to the original caller.

**CricketWorkerActor** — Extends `EventSourcedBehavior` for **Akka Persistence**. Calls the Ollama LLM using `pipeToSelf` (non-blocking) and persists every query as a `QueryProcessed` event to the H2 journal. State is rebuilt from events on restart — full **event sourcing**.

**LoggingActor** — Demonstrates the **Fire-and-Forget (Tell) pattern**. Receives log messages and never replies. Logs query received and query completed events.

---

## 🔄 Communication Patterns

| Pattern | From → To | Mechanism |
| --- | --- | --- |
| **ASK** | SupervisorActor → RouterActor | `getContext().ask()` |
| **FORWARD** | RouterActor → WorkerActor | `workerActor.tell(replyTo)` |
| **TELL (Fire-and-Forget)** | SupervisorActor → LoggingActor | `loggingActor.tell(...)` |

---

## 📡 Message Flow

```javascript
REST  POST /api/cricket/analyze
        │
        ▼  AskPattern.ask
   AnalyticsController
        │
        ▼  AnalyzeQuery
   SupervisorActor ──ASK──▶ RouterActor
        │                       │
        │                       ▼  FORWARD (ProcessQuery)
        │                  CricketWorkerActor
        │                       │
        │                       ▼  async pipeToSelf
        │                  OllamaService ──▶ Llama 3.2 (local LLM)
        │                       │
        │                       ▼  Effect().persist(QueryProcessed event) ──▶ H2 Database
        ▼                       │
   replyTo.tell(AnalysisResponse) ◀─────────┘
        │
        ▼
   JSON Response → REST caller
```

---

## 🚀 Getting Started

### Prerequisites

- Java 21
- Maven
- [Ollama](https://ollama.com/) running locally with the Llama 3.2 model:

```bash
  ollama pull llama3.2
  ollama serve
```

### Run the Cluster (3 nodes)

```bash
# Node 1 (seed node, default): port 2551, HTTP 8080
java -jar app.jar

# Node 2: port 2552, HTTP 8081
java -jar app.jar -Dakka.node.port=2552 -Dserver.port=8081

# Node 3: port 2553, HTTP 8082
java -jar app.jar -Dakka.node.port=2553 -Dserver.port=8082
```

### Cluster Configuration

- **Ports:** 2551, 2552, 2553
- **REST APIs:** 8080, 8081, 8082
- **Seed node:** 2551 (primary contact point)
- **Cluster name:** `CricketAnalyticsSystem`
- **Split Brain Resolver:** KeepMajority strategy

---

## 📮 API Usage

### Analyze a Cricket Query

```bash
curl -X POST http://localhost:8080/api/cricket/analyze \
     -H "Content-Type: application/json" \
     -d '{"query": "Analyze Virat Kohli'\''s batting average in T20 World Cup 2024"}'
```

**Response (success):**

```json
{
  "query": "Analyze Virat Kohli's batting average in T20 World Cup 2024",
  "analysis": "...",
  "success": true,
  "errorMessage": null,
  "workerNode": "cricket-worker-1",
  "persistenceId": "cricket-worker-1"
}
```

### Health Check

```bash
curl http://localhost:8080/api/cricket/health
```

---

## 💾 Akka Persistence — Event Sourcing

Every query processed by `CricketWorkerActor` is saved as an event.

**Events saved to the H2 journal:**

| Sequence | Event | Query |
| --- | --- | --- |
| 1 | `QueryProcessed` | "Virat Kohli analysis" |
| 2 | `QueryProcessed` | "Best bowler 2024" |
| 3 | `QueryProcessed` | "MS Dhoni leadership" |

**On node restart:**

1. Actor reads all past events by `PersistenceId` `cricket-worker-1`
2. Replays events through `eventHandler()`
3. State fully recovered — query count continues from where it left off

**Schema initialization ordering** (handled automatically by `AkkaConfig`):

1. `DataSource` bean created (H2 in-memory `akkadb`)
2. `spring.sql.init` runs `schema.sql` (creates `event_journal` + snapshot tables)
3. `ActorSystem` bean created (depends on `DataSource`, guaranteeing tables exist)
4. Akka Persistence connects via Slick → tables already exist ✓

---

## 🤖 LLM Integration — Ollama

- **Model:** Llama 3.2 running locally via Ollama
- **Endpoint:** `http://localhost:11434`
- **Library:** [ollama4j](https://github.com/amithkoujalgi/ollama4j)
- **Called in:** `OllamaService.analyzeCricket()`
- **Non-blocking:** called via `CompletableFuture.supplyAsync()` + `pipeToSelf` — the actor thread is never blocked
- Configurable via `application.properties`:

```properties
  ollama.base-url=http://localhost:11434
  ollama.model=llama3.2
  ollama.timeout-seconds=120
  akka.ask.timeout-seconds=60
```

---

## 🏗️ Project Structure

```javascript
src/main/java/com/cricket/
├── CricketAnalyticsApplication.java      # Spring Boot entry point
├── CricketSerializable.java              # Marker interface for Akka Jackson JSON serialization
├── actors/
│   ├── SupervisorActor.java              # Guardian — ASK pattern, response/failure handling
│   ├── RouterActor.java                  # Dispatcher — FORWARD pattern
│   ├── CricketWorkerActor.java           # Worker — EventSourcedBehavior + Ollama + pipeToSelf
│   └── LoggingActor.java                 # Fire-and-forget logger
├── config/
│   └── AkkaConfig.java                   # Bootstraps ActorSystem as a Spring bean
├── controller/
│   └── AnalyticsController.java          # REST endpoints (non-blocking via CompletionStage)
├── dto/
│   ├── AnalysisRequest.java              # Incoming request body
│   └── AnalysisResponse.java             # Response DTO flowing through the pipeline
└── service/
    └── OllamaService.java                # Ollama4j client wrapper
```

> **Why no `@Autowired` in actors?** Akka creates actors inside its own runtime — Spring's DI container has no hook into that process. Dependencies (e.g., `OllamaService`) are passed as constructor arguments through the actor hierarchy: `AkkaConfig` → `SupervisorActor` → `RouterActor` → `CricketWorkerActor`.

---

## ✨ Key Features

- **Same JVM:** All nodes run Java 21 HotSpot JVM
- **Actor Communication:** 4 actors with clear message types
- **Clear Message Flow:** Logged at every step
- **3-Node Cluster:** Akka Cluster with seed nodes
- **Ask Pattern:** SupervisorActor → RouterActor
- **Forward Pattern:** RouterActor → WorkerActor
- **Fire-and-Forget:** SupervisorActor → LoggingActor
- **Akka Persistence:** `EventSourcedBehavior` with JDBC journal
- **Event Sourcing:** State rebuilt from events on restart
- **LLM Integration:** Ollama + Llama 3.2, fully local
- **Fault Tolerance:** KeepMajority Split Brain Resolver
- **Non-blocking I/O:** HTTP thread never blocked during Ollama calls

---

## 🎬 Demo Highlights

1. **3-node cluster forming** — all nodes joining via seed node
2. **Live query** — message flow visible in logs
3. **Persistence proof** — query number resumes after Node 1 restart
4. **Fault tolerance** — Node 2 becomes leader when Node 1 crashes
5. **H2 console** — visual proof of persisted events

---

*University Course Project — Spring 2025*
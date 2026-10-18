# CDC PoC: SQL Server → Debezium → Redpanda → .NET

Proof of concept for **Change Data Capture (CDC)**: every `INSERT`, `UPDATE`, and `DELETE` on a SQL Server table is published as an event to Redpanda (Kafka-compatible) and consumed by a .NET application, with logs and traces visible in the Aspire dashboard.

## Architecture

```mermaid
flowchart LR
    U[SQL Client] -->|INSERT UPDATE DELETE| SQL[(SQL Server<br/>CDC enabled)]
    SQL -->|captured changes| DBZ[Debezium<br/>Kafka Connect]
    DBZ -->|JSON events| RP[["Redpanda<br/>poc.poc-cdc.dbo.Orders"]]
    RP --> APP[.NET Consumer<br/>BackgroundService]
    RP --> UI[Redpanda Console]
    APP -.->|OTLP logs and traces| AD[Aspire Dashboard]
```

## Change flow

```mermaid
sequenceDiagram
    participant U as SQL Client
    participant DB as SQL Server
    participant D as Debezium
    participant K as Redpanda
    participant C as .NET Consumer
    participant A as Aspire Dashboard

    U->>DB: INSERT / UPDATE / DELETE
    DB->>DB: CDC capture job records the change
    D->>DB: Reads captured changes
    D->>K: Publishes event to the topic
    K->>C: Consume()
    C->>C: Processes the event
    C->>A: Sends logs and trace (OTLP)
    C->>K: Commits the offset
```

## Consumer logic

```mermaid
flowchart TD
    A[Consume message] --> B{Value is null?}
    B -->|Yes, tombstone| E[Commit offset]
    B -->|No| C[Deserialize OrderRow]
    C --> D[Process, log and trace]
    D --> E
    E --> A
```

## Stack

| Component | Technology |
|---|---|
| Database | SQL Server 2022 (Docker) |
| Change capture | Debezium (Kafka Connect) |
| Broker | Redpanda |
| Topic exploration | Redpanda Console |
| Observability | Aspire Dashboard (standalone) + OpenTelemetry |
| Consumer | .NET Worker Service + Confluent.Kafka |

## Project structure

```text
CdcConsumer/                      # Solution
├── ExternalData/
│   ├── docker-compose.yml
│   └── README.md
└── src/
    └── CdcConsumer/              # Worker Service project
        ├── Models/
        ├──── CdcJsonContext.cs
        ├──── OrderRow.cs
        ├── appsettings.json
        ├── appsettings.Development.json
        ├── KafkaOptions.cs
        ├── OrdersCdcConsumer.cs
        └── Program.cs
```

## Requirements

- Docker Desktop
- .NET SDK
- A SQL client (DBeaver, Azure Data Studio, etc.)
- On Apple Silicon Macs: enable *Use Rosetta for x86_64/amd64 emulation* in Docker Desktop

## How to run

### 1. Start the infrastructure

From the `ExternalData/` folder:

```bash
docker compose up -d
docker compose ps
```

### 2. Create the database and enable CDC

The database name contains a hyphen, so it must be wrapped in brackets in T-SQL:

```sql
CREATE DATABASE [poc-cdc];
GO
USE [poc-cdc];
GO
CREATE TABLE dbo.Orders (
    Id INT IDENTITY PRIMARY KEY,
    Customer NVARCHAR(100),
    Total DECIMAL(10,2)
);
GO
EXEC sys.sp_cdc_enable_db;
GO
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'Orders',
    @role_name     = NULL;
GO
```

### 3. Register the connector

Set `database.names` to `poc-cdc` and `database.password` to the password configured in your `docker-compose.yml` (`<SA_PASSWORD>`) in `connector.json`, then:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @connector.json \
  http://localhost:8083/connectors

curl http://localhost:8083/connectors/sqlserver-orders/status
```

Both the connector and its task should be `RUNNING`.

The topic name is built as `topic.prefix` + database + schema + table:

```text
poc . poc-cdc . dbo . Orders
```

The topic is created when the first event arrives, so insert a row before looking for it.

### 4. Run the consumer

```bash
cd src/CdcConsumer
dotnet run
```

### 5. Try it out

```sql
INSERT INTO dbo.Orders (Customer, Total) VALUES ('Ana', 100);
UPDATE dbo.Orders SET Total = 150 WHERE Customer = 'Ana';
DELETE FROM dbo.Orders WHERE Customer = 'Ana';
```

Events show up in the consumer log, in Redpanda Console (<http://localhost:8080>), and in the Aspire dashboard (<http://localhost:18888>).

## Configuration (`appsettings.json`)

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:19092",
    "GroupId": "orders-poc",
    "Topic": "poc.poc-cdc.dbo.Orders"
  }
}
```

Read through the *Options Pattern* and validated at startup (`ValidateOnStart`).

## Message format

The connector uses the `ExtractNewRecordState` SMT, which flattens the event:

```json
{
  "Id": 1,
  "Customer": "Ana",
  "Total": "100.00",
  "__op": "c",
  "__deleted": "false"
}
```

| `__op` | Meaning |
|---|---|
| `r` | Initial snapshot read |
| `c` | Create (INSERT) |
| `u` | Update (UPDATE) |
| `d` | Delete (DELETE), arrives with `__deleted = "true"` |

`DECIMAL` values arrive as strings (`decimal.handling.mode=string`) to avoid base64 encoding.

## Observability

The consumer exports logs and traces over OTLP to a standalone Aspire dashboard.

| What | Where |
|---|---|
| Dashboard UI | <http://localhost:18888> (login token is printed in the container logs) |
| OTLP/gRPC endpoint | `http://localhost:4317` |
| Logs | **Structured logs**, filter by the `cdc-consumer` resource |
| Traces | **Traces**: one `process order change` span per message, tagged with the Kafka partition and offset, `order.id`, and `cdc.op` |

Debezium does not propagate trace context in the messages, so each trace contains only the consumer span.

## Technical decisions

- **Manual offset commits** after processing: guarantees *at-least-once* delivery.
- **Source generators** for JSON (`JsonSerializerContext`) and logging (`[LoggerMessage]`): no runtime reflection.
- **Graceful shutdown**: the Generic Host cancels the `CancellationToken` on SIGTERM/Ctrl+C and the consumer exits cleanly with `Close()`.
- **Manual `ActivitySource`** for tracing, instead of a Kafka instrumentation package, to keep control over span tags.

## Considerations

- Being *at-least-once*, an event may be delivered more than once: processing must be **idempotent**.
- After a `DELETE`, Debezium usually emits a message with a `null` value (*tombstone*); the consumer ignores it.
- This is a PoC: no persistent volumes, TLS, or secrets management.

## Cleanup

```bash
docker compose down
```
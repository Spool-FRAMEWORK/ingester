# Spool — Ingester

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/build-Maven-C71A36.svg)](https://maven.apache.org/)

**Ingester** is the **Spool** framework module responsible for **buffering published events and flushing them in batches to a data lake**. It listens for `ItemPublished` events on the event bus, accumulates them in a thread-safe buffer, and writes them to your storage backend according to a configurable flush policy.

---

## Table of Contents

- [Architecture](#architecture)
- [Key Concepts](#key-concepts)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Examples](#examples)
  - [Reactive (Immediate) Ingestion](#reactive-immediate-ingestion)
  - [Buffered Ingestion with Size Limit](#buffered-ingestion-with-size-limit)
  - [Time-based Flush Policy](#time-based-flush-policy)
  - [Combined Flush Policy](#combined-flush-policy)
  - [Custom DataLakeWriter (S3)](#custom-datalakewriter-s3)
  - [Custom Error Routing](#custom-error-routing)
- [API Reference](#api-reference)
- [Flush Policies](#flush-policies)
- [Event Model](#event-model)
- [License](#license)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                          Ingester                            │
│                                                              │
│  EventBus ──► ItemPublished ──► FlushCoordinator             │
│                                     │                        │
│                              ┌──────┴──────┐                 │
│                              ▼             ▼                 │
│                           Buffer      FlushPolicy            │
│                              │       (shouldFlush?)          │
│                              ▼                               │
│                     ItemPublishedHandler                      │
│                      │         │        │                    │
│                      ▼         ▼        ▼                    │
│              DataLakeWriter  InboxUpdater  EventBus           │
│              (write batch)   (→PERSISTED)  (ItemPersisted)   │
│                                                              │
│  Errors ──► ErrorRouter ──► EventBus.emit(FailureEvent)      │
└──────────────────────────────────────────────────────────────┘
```

The ingestion flow:

1. **`EventBus`** — delivers `ItemPublished` events to the ingester.
2. **`Buffer`** — accumulates events in a thread-safe list.
3. **`FlushPolicy`** — evaluates whether the buffer should be flushed (by size, time, or both).
4. **`DataLakeWriter.write()`** — persists the batch to the data lake.
5. **`InboxUpdater`** — marks each inbox item as `PERSISTED`.
6. **`EventBus`** — emits `ItemPersisted` for each processed item.

---

## Key Concepts

| Concept | Description |
|---|---|
| **`Ingester`** | API entry point. Manages the ingestion lifecycle (`startIngestion` / `stopIngestion`). |
| **`DataLakeWriter`** | Interface you implement to persist event batches (database, S3, data warehouse…). |
| **`FlushPolicy`** | Rules for when to flush: `immediate()`, `whenReaches(n)`, `every(duration)`, or combinations. |
| **`FlushCoordinator`** | Internal coordinator that manages the buffer and applies the flush policy. |
| **`Buffer`** | Thread-safe in-memory buffer for `ItemPublished` events. |
| **`IngesterBuilderFactory`** | DSL entry point with `reactive()` and `buffered()` presets. |
| **`IngesterBuilder`** | Fluent builder for custom ingester configuration. |

---

## Installation

### Maven (GitHub Packages)

```xml
<dependency>
    <groupId>io.github.spool-framework</groupId>
    <artifactId>ingester</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Configure the GitHub Packages repository in your `pom.xml` or `settings.xml`:

```xml
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/Spool-FRAMEWORK/*</url>
    </repository>
</repositories>
```

> Requires **Java 17+**.

---

## Quick Start

```java
// 1. Create your infrastructure
InMemoryEventBus bus = new InMemoryEventBus();
InMemoryDataLake dataLake = new InMemoryDataLake();
InMemoryInbox inbox = new InMemoryInbox();

// 2. Create the ingester with buffered defaults (flush at 100 items or 60s)
Ingester ingester = IngesterBuilderFactory.buffered()
        .from(bus)                // listen for ItemPublished events
        .storesWith(dataLake)     // write batches here
        .updatedWith(inbox)       // update inbox status
        .on(bus)                  // emit ItemPersisted events
        .create();

// 3. Start and stop
ingester.startIngestion();
// ... events flow through the pipeline ...
ingester.stopIngestion();
```

---

## Examples

### Reactive (Immediate) Ingestion

Every `ItemPublished` event is flushed to the data lake immediately, with no buffering.

```java
Ingester ingester = IngesterBuilderFactory.reactive()
        .from(eventBusListener)
        .storesWith(myDataLakeWriter)
        .updatedWith(inboxUpdater)
        .on(eventBusEmitter)
        .create();

ingester.startIngestion();
```

---

### Buffered Ingestion with Size Limit

Flush the buffer when it accumulates 50 items.

```java
Ingester ingester = IngesterBuilderFactory.buffered()
        .from(eventBusListener)
        .storesWith(myDataLakeWriter)
        .updatedWith(inboxUpdater)
        .on(eventBusEmitter)
        .flushWith(FlushPolicy.whenReaches(50))
        .create();

ingester.startIngestion();
```

---

### Time-based Flush Policy

Flush the buffer every 10 seconds regardless of how many items are buffered.

```java
Ingester ingester = IngesterBuilderFactory.buffered()
        .from(eventBusListener)
        .storesWith(myDataLakeWriter)
        .updatedWith(inboxUpdater)
        .on(eventBusEmitter)
        .flushWith(FlushPolicy.every(Duration.ofSeconds(10)))
        .create();

ingester.startIngestion();
```

---

### Combined Flush Policy

Flush when the buffer reaches 200 items **or** every 30 seconds, whichever comes first.

```java
FlushPolicy policy = FlushPolicy.whenReaches(200).orEvery(Duration.ofSeconds(30));

Ingester ingester = IngesterBuilderFactory.buffered()
        .from(eventBusListener)
        .storesWith(myDataLakeWriter)
        .updatedWith(inboxUpdater)
        .on(eventBusEmitter)
        .flushWith(policy)
        .create();

ingester.startIngestion();
```

---

### Custom DataLakeWriter (S3)

Implement `DataLakeWriter` to persist batches to any storage backend.

```java
public class S3DataLakeWriter implements DataLakeWriter {
    private final S3Client s3;
    private final String bucket;

    public S3DataLakeWriter(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    @Override
    public <E extends Event> void write(Collection<E> items) {
        String json = objectMapper.writeValueAsString(items);
        String key = "ingested/" + Instant.now().toEpochMilli() + ".json";
        s3.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromString(json)
        );
    }
}

Ingester ingester = IngesterBuilderFactory.buffered()
        .from(eventBusListener)
        .storesWith(new S3DataLakeWriter(s3Client, "my-data-lake"))
        .updatedWith(inboxUpdater)
        .on(eventBusEmitter)
        .flushWith(FlushPolicy.whenReaches(500).orEvery(Duration.ofMinutes(1)))
        .create();

ingester.startIngestion();
```

---

### Custom Error Routing

Override the default error routing to add logging or custom recovery.

```java
ErrorRouter customRouter = IngesterErrorRouter.defaults(eventBusEmitter);

// Or build your own:
ErrorRouter customRouter = new ErrorRouter()
        .on(DataLakeWriteException.class, (e, cause) -> {
            logger.error("Data lake write failed: {}", e.getMessage());
            eventBusEmitter.emit(DataLakePersistFailed.builder()
                    .errorMessage(e.getMessage()).build());
        });
```

---

## API Reference

### Builder DSL

| Step | Method | Description |
|---|---|---|
| Entry point | `IngesterBuilderFactory.reactive()` | Start with immediate flush policy. |
| Entry point | `IngesterBuilderFactory.buffered()` | Start with buffered flush policy (100 items / 60s). |
| Listener | `.from(eventBusListener)` | Set the event bus to subscribe to. |
| Writer | `.storesWith(dataLakeWriter)` | Set the data lake writer. |
| Updater | `.updatedWith(inboxUpdater)` | Set the inbox updater. |
| Emitter | `.on(eventBusEmitter)` | Set the event bus to emit events on. |
| Policy | `.flushWith(flushPolicy)` | Override the flush policy. |
| Create | `.create()` | Return the configured `Ingester`. |

### Ingester Lifecycle

| Method | Description |
|---|---|
| `startIngestion()` | Subscribe to `ItemPublished` events and start the flush scheduler. |
| `stopIngestion()` | Cancel the subscription and shut down the flush scheduler. |

---

## Flush Policies

| Factory Method | Behavior |
|---|---|
| `FlushPolicy.immediate()` | Flush on every single item (buffer limit = 1). |
| `FlushPolicy.whenReaches(n)` | Flush when the buffer accumulates `n` items. |
| `FlushPolicy.every(duration)` | Flush at fixed time intervals. |
| `policy.orEvery(duration)` | Combine a size-based policy with a time interval (composite). |

---

## Event Model

| Event | When Emitted |
|---|---|
| `ItemPublished` | _Consumed_ — received from the event bus, triggers ingestion. |
| `ItemPersisted` | After a batch is successfully written to the data lake. |
| `DataLakePersistFailed` | When `DataLakeWriter.write()` throws an exception. |

---

## License

Distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

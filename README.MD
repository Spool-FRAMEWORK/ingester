# Spool — Ingester

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/build-Maven-C71A36.svg)](https://maven.apache.org/)

**Ingester** is the **Spool** framework module responsible for **consuming events from the bus and writing envelopes in batches to a data lake**. It subscribes to events, accumulates the corresponding envelopes in a buffer, and flushes them to your storage backend based on a configurable flush policy.

---

## Table of Contents

- [What it does](#what-it-does)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Examples](#examples)
  - [Reactive (Immediate) Ingestion](#reactive-immediate-ingestion)
  - [Buffered Ingestion with Size Limit](#buffered-ingestion-with-size-limit)
  - [Time-based Flush Policy](#time-based-flush-policy)
  - [Combined Flush Policy](#combined-flush-policy)
  - [Custom DataLakeWriter (S3)](#custom-datalakewriter-s3)
  - [Quarantine Store](#quarantine-store)
  - [Watchdog Integration](#watchdog-integration)
- [Key Concepts](#key-concepts)
- [Flush Policies](#flush-policies)
- [License](#license)

---

## What it does

```
EventBus ──► (event received) ──► Buffer
                                     │
                                FlushPolicy
                               (shouldFlush?)
                                     │
                                     ▼
                        InboxEnvelopeResolver.findByIds()
                                     │
                                     ▼
                        DataLakeWriter.write(envelopes)
                                     │
                              ┌──────┴──────┐
                              ▼             ▼
                        QuarantineStore  EventPublisher
                        (on failure)     (ItemPersisted)

Errors ──► ErrorRouter ──► failure event
```

The Ingester resolves full `Envelope` records from the inbox, writes them in batches to the data lake, and emits a confirmation event for each persisted item.

---

## Installation

```xml
<dependency>
    <groupId>io.github.spool-framework</groupId>
    <artifactId>ingester</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```xml
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/Spool-FRAMEWORK/*</url>
    </repository>
</repositories>
```

> Requires **Java 21+**.

---

## Quick Start

```java
// Buffered defaults: flush at 100 items or every 60 seconds
Ingester ingester = IngesterBuilderFactory.buffered()
        .from(eventBus)
        .storesWith(dataLakeWriter)
        .readWith(inboxEnvelopeResolver)
        .on(eventBus)
        .create();

SpoolNode node = SpoolNode.create()
        .register(ingester);

node.start();
```

---

## Examples

### Reactive (Immediate) Ingestion

Every event triggers an immediate write to the data lake. No buffering.

```java
Ingester ingester = IngesterBuilderFactory.reactive()
        .from(eventBus)
        .storesWith(dataLakeWriter)
        .readWith(inboxEnvelopeResolver)
        .on(eventBus)
        .create();
```

---

### Buffered Ingestion with Size Limit

Flush the buffer when it accumulates 50 items.

```java
Ingester ingester = IngesterBuilderFactory.buffered()
        .from(eventBus)
        .storesWith(dataLakeWriter)
        .readWith(inboxEnvelopeResolver)
        .on(eventBus)
        .flushPolicy(FlushPolicy.whenReaches(50))
        .create();
```

---

### Time-based Flush Policy

Flush every 10 seconds regardless of how many items are buffered.

```java
Ingester ingester = IngesterBuilderFactory.buffered()
        .from(eventBus)
        .storesWith(dataLakeWriter)
        .readWith(inboxEnvelopeResolver)
        .on(eventBus)
        .flushPolicy(FlushPolicy.every(Duration.ofSeconds(10)))
        .create();
```

---

### Combined Flush Policy

Flush at 200 items **or** every 30 seconds, whichever comes first.

```java
Ingester ingester = IngesterBuilderFactory.buffered()
        .from(eventBus)
        .storesWith(dataLakeWriter)
        .readWith(inboxEnvelopeResolver)
        .on(eventBus)
        .flushPolicy(FlushPolicy.whenReaches(200).orEvery(Duration.ofSeconds(30)))
        .create();
```

---

### Custom DataLakeWriter (S3)

Implement `DataLakeWriter` to write batches to any storage backend. The method returns the keys of successfully written envelopes.

```java
public class S3DataLakeWriter implements DataLakeWriter {
    private final S3Client s3;
    private final String bucket;

    @Override
    public Stream<IdempotencyKey> write(Collection<Envelope> items) {
        String json = objectMapper.writeValueAsString(items);
        String key = "ingested/" + Instant.now().toEpochMilli() + ".json";
        s3.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromString(json)
        );
        return items.stream().map(Envelope::idempotencyKey);
    }
}

Ingester ingester = IngesterBuilderFactory.buffered()
        .from(eventBus)
        .storesWith(new S3DataLakeWriter(s3Client, "my-data-lake"))
        .readWith(inboxEnvelopeResolver)
        .on(eventBus)
        .flushPolicy(FlushPolicy.whenReaches(500).orEvery(Duration.ofMinutes(1)))
        .create();
```

---

### Quarantine Store

Provide a `QuarantineStore` to capture envelopes that fail validation instead of discarding them.

```java
Ingester ingester = IngesterBuilderFactory.buffered()
        .from(eventBus)
        .storesWith(dataLakeWriter)
        .readWith(inboxEnvelopeResolver)
        .on(eventBus)
        .quarantineStore(quarantinedRecord ->
                deadLetterQueue.send(quarantinedRecord))
        .create();
```

---

### Watchdog Integration

```java
Ingester ingester = IngesterBuilderFactory
        .watchdog("http://spool-watchdog:8080", "my-ingester")
        .buffered()
        .from(eventBus)
        .storesWith(dataLakeWriter)
        .readWith(inboxEnvelopeResolver)
        .on(eventBus)
        .create();
```

---

## Key Concepts

| Concept | Description |
|---|---|
| **`Ingester`** | Main module implementing `SpoolModule`. Lifecycle via `SpoolNode`. |
| **`IngesterBuilderFactory`** | DSL entry point with `reactive()` and `buffered()` presets. |
| **`DataLakeWriter`** | Interface you implement to persist batches. Returns successfully written keys. |
| **`InboxEnvelopeResolver`** | Resolves full `Envelope` records by id for the batch. |
| **`FlushPolicy`** | Rules for when to flush the buffer. |
| **`QuarantineStore`** | Optional sink for envelopes that fail validation. |

---

## Flush Policies

| Factory Method | Behaviour |
|---|---|
| `FlushPolicy.immediate()` | Flush on every single item. |
| `FlushPolicy.whenReaches(n)` | Flush when the buffer accumulates `n` items. |
| `FlushPolicy.every(duration)` | Flush at fixed time intervals. |
| `policy.orEvery(duration)` | Combine a size-based policy with a time interval. |

---

## License

Distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

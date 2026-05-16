# local-collection

A lightweight `List`/`Map` alternative backed by a local embedded database to reduce JVM memory pressure in large-data scenarios.

> Suitable for: report generation jobs, large data extraction, offline transformation pipelines, and any case where objects would otherwise consume a lot of heap.

## Core idea

`local-collection` mimics the familiar `List` / `Map` usage style while storing data in local database files (instead of keeping everything in heap memory). The default engine is **SQLite**, with **H2** supported as an alternative.

## Current capabilities

- Implements disk-backed `LocalList` and `LocalMap` APIs.
- Automatically initializes local storage paths and data files on startup.
- Basic CRUD and iteration support.
- Optional write caching for small datasets to reduce IO overhead.
- Prefetching in iterator paths for better large-scan throughput.

## Limitations

- Modifying a value object after reading it from the collection will not be automatically persisted.
- Only primitive-wrapper/basic persistence-friendly types are well supported; see the type whitelist in `ColumnNameUtil`.
- `stream()` usage should be careful: avoid relying on mutating behavior via `peek`.
- Behavior and coverage may evolve with versions; verify against your target release.

## Quick start (SQLite)

### 1) Add dependency

```xml
<dependency>
    <groupId>io.github.lordeath</groupId>
    <artifactId>local-collection</artifactId>
    <version>1.0.20250306.1</version>
</dependency>
```

### 2) Optional: switch engine

```java
// SQLite is the default, only set this when using H2:
System.setProperty("dbEngine", "h2");
```

### 3) Usage

```java
try (LocalList<String> list = new LocalList<>(String.class)) {
    list.add("a");
    list.add("b");

    // resources are cleaned up on close (and best-effort cleanup may happen later by GC)
}
```

## Roadmap

- [x] Convert `LocalList` to `Map` for aggregation use cases
- [x] Database-backed `Map` implementation
- [x] Fallback to in-memory path for small datasets
- [x] Startup config validation and data cleanup
- [x] App-level data-path isolation
- [x] Add read cache with threshold tuning
- [x] Prefetch strategy in iterator flow
- [ ] Use `spring.application.name` for safer default directories
- [ ] Document SSD path strategy and related configuration
- [ ] Clarify and enforce restricted `stream().peek(...)` mutation semantics

## Related references

- Example usage: `src/test/java/lordeath/local/collection/test/LocalListTest.java`
- Type mapping: `src/main/java/lordeath/local/collection/db/util/ColumnNameUtil.java`


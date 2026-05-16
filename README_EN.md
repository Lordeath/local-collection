# local-collection

A lightweight local-backed alternative to `List` / `Map` that stores data in an embedded database to reduce heap pressure and OOM risk.

## Core model

- `LocalList` implements `List<T>, AutoCloseable` and persist data through local database tables.
- `LocalMap` implements `Map<String, V>` on top of `LocalList`, including aggregation-style construction from a source list.
- Database operations are routed by `IDatabaseOpt` and selected via `DatabaseFactory` as:
  - `sqlite` (default)
  - `h2`
- Thread-safety: current implementation is **not thread-safe**.

## Runtime behavior

- `LocalList` initializes DB table metadata on first use (`init`) based on the element type.
- `cacheSize` is controlled by `lordeath.local.collection.cache.size` (default `10000`).
  - When `cacheSize > 0` and no flush happened yet, writes go to in-memory `cache`.
  - When threshold is exceeded, `restoreCacheToDB()` flushes cache to database in batch.
- After flush, reads/iteration use DB-backed query paths.
- `close()` always flushes cache once and closes DB resources.
- `finalize()` also tries to close, but execution is not guaranteed; `try-with-resources` is strongly recommended.

## Configuration (system property / environment variable)

- `lordeath.local.collection.db.engine` (default `sqlite`)
  - `sqlite`: `SqliteConfig`
  - `h2`: `H2Config`
- `lordeath.local.collection.db.init.delete` (default `true`)
  - delete DB file on startup if it exists
- `spring.application.name` (default `unknow_app_name`)
  - used for app-level directory isolation
- `lordeath.local.collection.cache.size` (default `10000`)
  - in-memory write cache size for `LocalList`
- `lordeath.local.collection.sqlite.file.path` / `...h2.file.path`
  - custom DB file path
- `lordeath.local.collection.sqlite.file.username` / `...password`
  - SQLite credentials (optional)
- `lordeath.local.collection.h2.file.username` / `...password`
  - H2 credentials (optional)

`MainConfig` resolves each value in this order: `System.getProperty` -> `System.getenv` -> default value.

## API support & behavior

### LocalList (partial `List`)

- Supported: `add`, `addAll`, `remove(index)`, `clear`, `get`, `set`, `size`, `isEmpty`, `iterator`, `listIterator`, `subList`, `pk(index)`.
- Explicitly unsupported (`UnsupportedOperationException`):
  - `contains`, `toArray`, `remove(Object)`, `containsAll`, `add(index, E)`, `removeAll`, `retainAll`, `indexOf`, `lastIndexOf`, etc.
- Alternatives:
  - For full-list membership checks, use external filtering or explicit DB-side query through `LocalMap`/`subList` pipelines.
  - For element replacement by value, use `indexOf`-equivalent in a temporary iterator loop and then `set(index, value)`.
  - For positional list semantics that require shifts on insert/delete in the middle, avoid direct `LocalList` usage.
- Important: mutating objects returned by `get` is **not** auto-persisted; call `set(index, value)` to write back.
- `subList` is immutable (`Collections.unmodifiableList`) when reading from DB path.
- Stream usage boundary:
  - `stream()` and `parallelStream()` are available through `List` default methods.
  - These are **read-oriented traversal** paths; heavy stream processing should prefer `iterator()` or `listIterator()` to avoid surprising DB access patterns.
  - Do not rely on short-circuit side effects from stream terminals to sync cached in-memory state with the database.

### LocalMap

- Use either:
  - `new LocalMap<>()` for direct key/value storage
  - `LocalMap.from(sourceList).where(...).groupBy(...).select(...).resultClass(...).keyField(...).build()` for grouped aggregation
- `put` returns old value, `remove` returns removed value.
- `keySet()`, `entrySet()`, `values()` are backed by DB queries.

## Persistable types

`ColumnNameUtil` currently supports the following mapped types:

- `String`
- `Integer` / `int`
- `Long` / `long`
- `Double` / `double`
- `Float` / `float`
- `Boolean` / `boolean`
- `Character` / `char`
- `Date` / `java.sql.Date` / `java.util.Date`
- `BigDecimal`

Unsupported field types fail fast with an exception.

To persist non-native types, register a `TypeCodec` in `TypeCodecRegistry` first:

- Implement and register `TypeCodec`:
  - `TypeCodecRegistry.register(new TypeCodec() { ... })`
- After registration, `LocalList`/`LocalMap` automatically applies codec `serialize`/`deserialize` on read/write.
- `TypeCodec#getDbType()` defines the SQL column type (for example, `VARCHAR`).

## Usage examples

### Maven dependency

```xml
<dependency>
  <groupId>io.github.lordeath</groupId>
  <artifactId>local-collection</artifactId>
  <version>1.0.20250306.1</version>
</dependency>
```

### LocalList

```java
System.setProperty("lordeath.local.collection.db.engine", "h2");

try (LocalList<String> list = new LocalList<>(String.class)) {
  list.add("a");
  list.add("b");
  list.set(1, "bb");
  String value = list.get(1);
} // auto close and cleanup
```

### LocalMap direct usage

```java
try (LocalMap<String, String> map = new LocalMap<>()) {
  map.put("a", "1");
  String old = map.put("a", "2"); // returns "1"
  map.remove("a");                // returns "2"
}
```

### Thread-safe wrappers (for multi-threaded use)

`LocalList` / `LocalMap` itself is not thread-safe. For concurrent use, wrap it with synchronized variants:

```java
try (SynchronizedLocalList<String> list = LocalList.synchronizedList(new LocalList<>(String.class))) {
  list.add("a");
  list.add("b");
}
```

```java
try (SynchronizedLocalMap<String, String> map = LocalMap.synchronizedMap(new LocalMap<>())) {
  map.put("a", "1");
  map.put("a", "2");
}
```

### LocalMap from list (grouped)

```java
try (LocalMap<String, Bean> map = LocalMap.from(sourceList)
    .where("age >= 18")
    .groupBy("name")
    .select("name", "sum(score) AS score")
    .resultClass(Bean.class)
    .keyField(FieldUtils.getDeclaredField(Bean.class, "name", true))
    .build()) {

  Bean v = map.get("Alice");
}
```

### Concurrency and synchronization strategy

`LocalList` / `LocalMap` are intentionally not internally synchronized.

- This keeps the core implementation lightweight.
- It avoids hidden global locking that could interfere with upper-layer transaction or batch logic.

`SynchronizedLocalList` / `SynchronizedLocalMap` provide **instance-level** synchronization:

- Single API calls are thread-safe (for example `add`, `get`, `put`, `remove`).
- Compound operations still need external coordination, such as check-then-act (`containsKey` + `put`), to avoid race conditions. You can synchronize on the wrapper mutex via `getMutex()`.

```java
SynchronizedLocalMap<String, String> map = LocalMap.synchronizedMap(new LocalMap<>());
try {
  synchronized (map.getMutex()) {
    if (!map.containsKey("a")) {
      map.put("a", "1");
    }
  }
} finally {
  map.close();
}
```

If you have heavy concurrent writes, consider sharding or write-queueing at a higher level instead of relying only on coarse global locking.

Prefer wrapping compound logic in a dedicated helper to keep lock scope explicit and stable:

```java
static void putIfAbsentUnderLock(SynchronizedLocalMap<String, String> map, String key, String value) {
  synchronized (map.getMutex()) {
    if (!map.containsKey(key)) {
      map.put(key, value);
    }
  }
}
```

Note: `putIfAbsent`, `compute`, `computeIfAbsent`, and `merge` should still be treated as composite operations. In complex concurrent cases, wrap your own critical section with the shared mutex to avoid race conditions with compound logic.

## Concurrency helper templates (recommended)

Here are reusable helper methods for common atomic patterns.

```java
public final class LocalCollectionConcurrencyKit {
  public static <V> void putIfAbsent(
      SynchronizedLocalMap<String, V> map,
      String key,
      java.util.function.Supplier<V> supplier) {
    synchronized (map.getMutex()) {
      if (!map.containsKey(key)) {
        map.put(key, supplier.get());
      }
    }
  }

  public static <V> V computeIfAbsent(
      SynchronizedLocalMap<String, V> map,
      String key,
      java.util.function.Function<String, V> computer) {
    synchronized (map.getMutex()) {
      V current = map.get(key);
      if (current == null) {
        current = computer.apply(key);
        map.put(key, current);
      }
      return current;
    }
  }

  public static <V> boolean removeIf(
      SynchronizedLocalList<V> list,
      int index,
      java.util.function.Predicate<V> matcher) {
    synchronized (list.getMutex()) {
      V value = list.get(index);
      if (matcher.test(value)) {
        list.remove(index);
        return true;
      }
      return false;
    }
  }

  private LocalCollectionConcurrencyKit() {
  }
}
```

This keeps each compound operation locked only for the smallest required scope while preserving atomic behavior.

## Spring Boot integration (auto-configuration)

The library reads configuration from `MainConfig` via Java system properties. In Spring Boot, add a bootstrap bean to sync values at startup.

```properties
lordeath.local.collection.db.engine=sqlite
lordeath.local.collection.cache.size=10000
lordeath.local.collection.cache.flush.interval.millis=0
lordeath.local.collection.cache.flush.chunk.size=0
lordeath.local.collection.db.create.index=true
```

```java
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;

@Configuration
public class LocalCollectionBootstrap implements ApplicationRunner {
    private final Environment environment;

    public LocalCollectionBootstrap(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void run(ApplicationArguments args) {
        System.setProperty("lordeath.local.collection.db.engine",
                environment.getProperty("lordeath.local.collection.db.engine", "sqlite"));
        System.setProperty("lordeath.local.collection.cache.size",
                environment.getProperty("lordeath.local.collection.cache.size", "10000"));
        System.setProperty("lordeath.local.collection.cache.flush.interval.millis",
                environment.getProperty("lordeath.local.collection.cache.flush.interval.millis", "0"));
        System.setProperty("lordeath.local.collection.cache.flush.chunk.size",
                environment.getProperty("lordeath.local.collection.cache.flush.chunk.size", "0"));
        System.setProperty("lordeath.local.collection.db.create.index",
                environment.getProperty("lordeath.local.collection.db.create.index", "true"));
    }
}
```

If you publish a starter, wrap this bootstrap logic into an auto-configuration module (suggested artifact `local-collection-spring-boot-starter`) and expose it via `@ConfigurationProperties` + auto-configuration registration.

## Deployment and operations

- Configure DB path and credentials:
  - `lordeath.local.collection.sqlite.file.path`
  - `lordeath.local.collection.h2.file.path`
  - `lordeath.local.collection.sqlite.file.username`
  - `lordeath.local.collection.sqlite.file.password`
  - `lordeath.local.collection.h2.file.username`
  - `lordeath.local.collection.h2.file.password`
- Tune write behavior:
  - `lordeath.local.collection.cache.size`
  - `lordeath.local.collection.cache.flush.interval.millis` (0=off)
  - `lordeath.local.collection.cache.flush.chunk.size` (0=single flush)
  - `lordeath.local.collection.db.create.index` (`true`/`false`)
- Use try-with-resources to ensure `close()` executes and temp tables are dropped.
- In multi-app environments, isolate workspace paths to avoid table collisions.

## Readiness checks and troubleshooting

- Pre-flight checks before first production traffic:
  - Confirm DB engine property (`sqlite`/`h2`) and file/workspace path are explicitly set.
  - Confirm cache and flush settings are intended for workload:
    - `cache.size`
    - `cache.flush.interval.millis`
    - `cache.flush.chunk.size`
    - `db.create.index`
  - Run a short smoke test with both write and read paths (`add`, `get`, `iterator`, `size`).
  - Validate close path by running a loop workload and ensuring no temp tables/file locks are left behind.
- Incident playbook:
  - If inserts appear missing after crash, verify cache flushing on close and reopen, then replay startup initialization.
  - If query/read path is slow, check index creation flag and inspect query plan before scaling.
  - If tables fail to initialize, confirm workspace path ownership and remove stale tables only with a maintenance window.
  - If `isRecoveryRequired()` returns true at startup, it usually means a previous flush was interrupted or the DB may be inconsistent; import from a known-good JSON/CSV snapshot and call `recoveryComplete()` after verification.

## Roadmap

- [x] `LocalList` to `LocalMap` aggregation path
- [x] DB-backed `LocalMap` implementation
- [x] Cache + bulk flush strategy
- [x] Startup cleanup and app-level directory isolation
- [x] Iterator prefetch strategy (`preReadCacheSize = 5000`)
- [x] Extend operational documentation and deployment guidance
- [x] Clarify stream usage boundaries
- [x] Add SQL dialect compatibility layer for `LocalMap` grouping expressions (SQLite/H2)
- [x] Add atomic composite operations for concurrent use (`putIfAbsent`, `computeIfAbsent`, `removeIfEquals`)
- [x] Expand `LocalList`/`LocalMap` supported APIs and document intentional non-supports with alternatives
- [x] Add observable runtime metrics (cache hit rate, cache size, flush count/time, db size)
- [x] Add failure recovery strategy for DB corruption / partial writes / abnormal shutdown
- [x] Add configurable write strategy controls (flush interval, flush chunk size, index/create flags)
- [x] Add snapshot/import/export support (JSON/CSV) and backup restore workflow
- [x] Add pluggable serialization path for non-native types (e.g. JSON serializer)
- [x] Add Spring Boot integration starter and auto-configuration docs
- [x] Expand `Synchronized*` wrappers with atomic batch operations
- [x] Add operational readiness checks and production troubleshooting playbook

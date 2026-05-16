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
- Important: mutating objects returned by `get` is **not** auto-persisted; call `set(index, value)` to write back.
- `subList` is immutable (`Collections.unmodifiableList`) when reading from DB path.

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

## Roadmap

- [x] `LocalList` to `LocalMap` aggregation path
- [x] DB-backed `LocalMap` implementation
- [x] Cache + bulk flush strategy
- [x] Startup cleanup and app-level directory isolation
- [x] Iterator prefetch strategy (`preReadCacheSize = 5000`)
- [ ] Extend operational documentation and deployment guidance
- [ ] Clarify stream usage boundaries

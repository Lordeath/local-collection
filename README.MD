# local-collection

一个轻量级的本地 `List` / `Map` 封装，核心目标是在大内存压力场景下将数据落盘，降低 OOM 风险。

## 核心模型

- `LocalList` 通过 `LocalList<T> implements List<T>, AutoCloseable` 提供列表能力。
- `LocalMap` 通过 `LocalList` 实现 `Map<String, V>`，用于键值场景和分组聚合。
- 实际数据库操作由 `IDatabaseOpt` 抽象，底层由 `DatabaseFactory` 选择实现：`sqlite`（默认）或 `h2`。
- 线程模型：当前实现声明为线程不安全（`not thread-safe`）。

## 运行机制（按源码）

- `LocalList` 首次使用时会按元素类型初始化数据库表结构（`init`）。
- `cacheSize` 通过 `lordeath.local.collection.cache.size` 控制（默认 `10000`）。
  - 当 `cacheSize > 0` 且尚未触发落库时，新增数据先写入内存 `cache`。
  - 命中阈值后会调用 `restoreCacheToDB()`，一次性刷入数据库。
  - 落库后读取、遍历将走数据库路径。
- `close()` 会触发一次 `restoreCacheToDB()`，并 `close` 数据库连接。
- `finalize()`（兼容旧清理机制）会尝试执行 `close()`，但不保证一定执行，因此建议显式 `try-with-resources`。

## 配置项（系统属性 / 环境变量）

- `lordeath.local.collection.db.engine`（默认 `sqlite`）
  - `sqlite`：使用 `SqliteConfig`
  - `h2`：使用 `H2Config`
- `lordeath.local.collection.db.init.delete`（默认 `true`）
  - 启动时删除数据库文件（若存在）
- `spring.application.name`（默认 `unknow_app_name`）
  - 用于数据库路径隔离（本项目支持按应用目录分离）
- `lordeath.local.collection.cache.size`（默认 `10000`）
  - LocalList 内存缓存上限
- `lordeath.local.collection.sqlite.file.path` / `...h2.file.path`
  - 自定义数据库文件目录
- `lordeath.local.collection.sqlite.file.username` / `...password`
  - SQLite 连接凭据（如需）
- `lordeath.local.collection.h2.file.username` / `...password`
  - H2 连接凭据（如需）

> `MainConfig` 的配置解析优先级为：`System.getProperty` → `System.getenv` → 默认值。

## API 覆盖与行为（按源码）

### LocalList（部分 `List`）

- 已支持：`add`、`addAll`、`remove(index)`、`clear`、`get`、`set`、`size`、`isEmpty`、`iterator`、`listIterator`、`subList`、`pk(index)`。
- 明确不支持（抛 `UnsupportedOperationException`）：`contains`、`toArray`、`remove(Object)`、`containsAll`、`add(index, E)`、`removeAll`、`retainAll`、`indexOf`、`lastIndexOf` 等。
- 关键注意事项：读取出的对象（例如 `get`）即使被修改，也不会自动回写，必须通过 `set(index, element)` 持久化。
- `subList` 在数据库路径下返回的是不可变列表。

### LocalMap（`Map`）

- 构造方式：
  - `new LocalMap<>()`（直接 key-value）
  - `LocalMap.from(sourceList).where(...).groupBy(...).select(...).resultClass(...).keyField(...).build()`（聚合）
- `put` 返回旧值；`remove` 返回被删值。
- `keySet()`、`entrySet()`、`values()` 基于底层数据库查询实现。
- 未覆盖全部 `Map` 的语义优化，适合以数据库持久化为主的场景。

## 类型持久化能力

`ColumnNameUtil` 目前按类型白名单映射到数据库类型，支持：

- `String`
- `Integer` / `int`
- `Long` / `long`
- `Double` / `double`
- `Float` / `float`
- `Boolean` / `boolean`
- `Character` / `char`
- `Date` / `java.sql.Date` / `java.util.Date`
- `BigDecimal`

不在白名单的字段类型会抛异常。

## 使用示例

### Maven 依赖

```xml
<dependency>
  <groupId>io.github.lordeath</groupId>
  <artifactId>local-collection</artifactId>
  <version>1.0.20250306.1</version>
</dependency>
```

### LocalList

```java
// 默认引擎为 sqlite，可选：
System.setProperty("lordeath.local.collection.db.engine", "h2");

try (LocalList<String> list = new LocalList<>(String.class)) {
  list.add("a");
  list.add("b");
  list.set(1, "bb"); // 可写回
  String v = list.get(1);
} // 自动触发 close，避免残留
```

### LocalMap（直接 put/get）

```java
try (LocalMap<String, String> map = new LocalMap<>()) {
  map.put("a", "1");
  String old = map.put("a", "2"); // 返回 "1"
  map.remove("a");                // 返回 "2"
}
```

### LocalMap from LocalList（分组）

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

## 后续计划（对齐代码方向）

- [x] `LocalList` 转 `LocalMap` 聚合链路
- [x] `LocalMap` 数据库落盘实现
- [x] 分层缓存与批量刷库策略
- [x] 启动初始化清理与应用名目录隔离
- [x] 读缓存与迭代预读策略（`preReadCacheSize = 5000`）
- [ ] 继续补齐文档与可运维参数说明
- [ ] 更细化的 stream / iterator 使用边界说明

## 英文版

> For English documentation, open [README_EN.md](README_EN.md).

# local-collection

一个基于本地数据库的轻量级 `List` / `Map` 替代方案，用于在内存受限场景下降低 OOM 风险。

> 适用于：报表批处理、全量导出、离线清洗、需要大量随机访问但不希望占满 JVM 堆的场景。

## 核心思路

项目在 `ArrayList` / `HashMap` 的 API 习惯上做适配，把数据持久化到本地数据库文件中，以减少堆内存占用。当前默认数据库引擎为 **SQLite**，也支持 **H2** 作为替代。

## 已有能力

- 提供基于数据库实现的 `LocalList` / `LocalMap`（详见 `LocalListTest` / 相关测试）。
- 自动在启动时按配置初始化本地存储目录和数据文件。
- 基础增删改查与遍历支持。
- 数据量较小场景下可采用写缓存以减少读写开销，提升性能。
- 迭代器采用预读策略提升大规模遍历性能。

## 使用限制

- 修改从集合中读取出的对象后，不会自动持久化；需通过 `LocalList.set(int index, Object value)` 明确写回。
- 当前主要支持基本类型及可持久化字段，不建议放入复杂对象图（参见 `ColumnNameUtil` 中支持类型白名单）。
- `stream()` 使用场景仍需谨慎，避免依赖 `peek` 做修改。
- 当前行为和特性会随版本演进持续优化，建议结合对应版本说明检查细节。

## 使用方式（以 SQLite 为例）

### 1）引入依赖

```xml
<dependency>
    <groupId>io.github.lordeath</groupId>
    <artifactId>local-collection</artifactId>
    <version>1.0.20250306.1</version>
</dependency>
```

### 2）可选：切换数据库引擎

```java
// 默认使用 sqlite，通常无需设置。仅在使用 h2 时手动指定：
System.setProperty("dbEngine", "h2");
```

### 3）使用示例

```java
try (LocalList<String> list = new LocalList<>(String.class)) {
    list.add("a");
    list.add("b");

    // 使用结束后会尝试在关闭时回收本地资源
}
```

## 后续计划

- [x] 支持 `LocalList` 与 `Map` 的转换
- [x] Map 基于 H2 / 本地库实现
- [x] 小数据量场景回退到内存实现以兼顾吞吐
- [x] 初始化配置检查与文件清理
- [x] 支持 `app` 级隔离目录，避免共享目录误删
- [x] 增加读缓存与阈值控制
- [x] 引入迭代预读提升扫描速度
- [ ] 集成 `spring.application.name` 作为默认目录命名依据
- [ ] 文档化 SSD 存储路径策略与配置
- [ ] 明确禁用/限制 `stream().peek(...)` 的修改语义

## 相关文件

- 实现示例：`src/test/java/lordeath/local/collection/test/LocalListTest.java`
- 类型映射：`src/main/java/lordeath/local/collection/db/util/ColumnNameUtil.java`

## English version

> If you prefer English, open [README_EN.md](/mnt/c/project/local-collection/README_EN.md).


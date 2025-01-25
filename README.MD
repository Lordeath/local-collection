我想做一个本地缓存，用作替代ArrayList或者HashMap，使用本地的硬盘上的缓存来降低内存的要求

基本思路是使用H2或者SQLite等本地数据库，优先使用H2，之后也会考虑集成其他的数据库引擎

已经完成了一些基础的操作，操作方法详见[H2Test](src/test/java/lordeath/local/collection/test/H2Test.java)


后续想要实现的功能：

- LocalList转换成Map，实现基本的聚合
- Map 也是基于H2或者其他的数据库实现的


集成方式，以h2举例：
```xml
<!-- 手动添加需要的数据库类型，当前支持h2,sqlite,hsqldb,derby -->
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <version>2.3.232</version>
</dependency>
<dependency>
    <groupId>fxm</groupId>
    <artifactId>local-collection</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```java
// 全局设置一遍即可
System.setProperty(CONST_DB_ENGINE, "sqlite");

// 使用时要记得关闭，否则数据会残留在磁盘上
try (LocalList<String> list = new LocalList<>(String.class)) {
    list.add("a");
    list.add("b");
}
```

心得体会：

速度要求高的话，还是h2或者hsqldb比较好，sqlite的性能不如这2个，derby的性能和社区活跃度都不如以上的这些，不建议选择derby

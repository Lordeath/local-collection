一个本地缓存，用作替代ArrayList或者HashMap，使用本地的硬盘上的缓存来降低内存的要求

基本思路是使用H2或者SQLite等本地数据库，优先使用SQLite，之后也会考虑集成其他的数据库引擎

已经完成了一些基础的操作，操作方法详见[H2Test](src/test/java/lordeath/local/collection/test/H2Test.java)


后续想要实现的功能：

- [X] LocalList转换成Map，实现基本的聚合
- [X] Map 也是基于H2或者其他的数据库实现的
- [ ] 数据量较少时，不进行持久化相关操作。当数据量超过某个数值时，进行持久化，否则就用ArrayList相关的实现方式来做。这样操作起来会快，而且使用范围更广泛
- [ ] 初始化时检查 tmp开头的表，如果存在表，就进行drop，清空当前文件？直接删数据文件是不是好一点，重启的时候删除，探测一下是哪个微服务，文件不能被共用




集成方式，以sqlite举例：
```xml
<dependency>
    <groupId>io.github.lordeath</groupId>
    <artifactId>local-collection</artifactId>
    <version>1.0.20250208.2</version>
</dependency>
```

```java
// 全局设置一遍即可（默认是sqlite不需要设置，在用h2的时候才设置）
// 这一块可能是过度设计，用户不需要关心内部实现是sqlite还是h2
System.setProperty(CONST_DB_ENGINE, "sqlite");

// 使用时要记得关闭，否则数据会残留在磁盘上
try (LocalList<String> list = new LocalList<>(String.class)) {
    list.add("a");
    list.add("b");
}
```

心得体会：

速度要求高的话，还是h2或者hsqldb比较好，sqlite的性能不如这2个，derby的性能和社区活跃度都不如以上的这些，不建议选择derby
sqlite在固态硬盘上的表现和h2类似，但是内存开销是sqlite大大优于其他
hsqldb在内存上的表现还不如直接用ArrayList，所以hsqldb之后也会放弃相关开发

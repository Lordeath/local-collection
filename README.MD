一个本地缓存，用作替代ArrayList或者HashMap，使用本地的硬盘上的缓存来降低内存的要求

基本思路是使用H2或者SQLite等本地数据库，优先使用SQLite，之后也会考虑集成其他的数据库引擎

已经完成了一些基础的操作，操作方法详见[LocalListTest](src/test/java/lordeath/local/collection/test/LocalListTest.java)


后续想要实现的功能：

- [X] LocalList转换成Map，实现基本的聚合
- [X] Map 也是基于H2或者其他的数据库实现的
- [X] 数据量较少时，不进行持久化相关操作。当数据量超过某个数值时，进行持久化，否则就用ArrayList相关的实现方式来做。这样操作起来会快，而且使用范围更广泛
- [X] 初始化时检查配置，清空当前文件
- [X] 数据文件和 app 相关，防止app共用一个目录的时候文件删错
- [X] 支持List -> LocalList、Map -> LocalMap
- [ ] 增加读缓存，在10000条数据以下时，使用写缓存进行读；写缓存的阈值改到10000


集成方式，以sqlite举例：
```xml
<dependency>
    <groupId>io.github.lordeath</groupId>
    <artifactId>local-collection</artifactId>
    <version>1.0.20250215.3</version>
</dependency>
```

```java
// 全局设置一遍即可（默认是sqlite不需要设置，在用h2的时候才设置）
// 这一块可能是过度设计，用户不需要关心内部实现是sqlite还是h2
System.setProperty(CONST_DB_ENGINE, "sqlite");

// 使用时要记得关闭，否则数据会残留在磁盘上（自动也会回收，当对象被内存回收时，会进行本地表的drop）
try (LocalList<String> list = new LocalList<>(String.class)) {
    list.add("a");
    list.add("b");
}
```


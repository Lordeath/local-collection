一个本地缓存，用作替代ArrayList或者HashMap，使用本地的硬盘上的缓存来降低内存的要求

# 开发的目的
线上的项目经常会遇到OOM的问题，这些OOM的问题通常由报表或者一些需要大规模取数的作业触发，引发OOM的对象是List或者Map居多。那有没有办法让List、Map持久化在硬盘上，不使用内存？以此为思路，开发的工具。


基本思路是使用H2或者SQLite等本地数据库，优先使用SQLite，也尝试了derby、dsqldb等数据库，最后发现还是sqlite和h2最好，默认是sqlite。


已经完成了一些基础的操作，操作方法详见[LocalListTest](src/test/java/lordeath/local/collection/test/LocalListTest.java)

但是这个local-collection使用上有一些限制：
- 取出来的对象修改之后不会自动进行持久化，需要手动使用 `LocalList.set(int i, Object o)` 才能持久化到硬盘，所以这个LocalList不适合修改对象内容
- 支持的类型有限，不支持复杂类型，只支持一些基础类型，可以进行持久化的类型，比如Integer、String等，详见[代码链接](src/main/java/lordeath/local/collection/db/util/ColumnNameUtil.java#L59)

# 后续想要实现的功能：

- [X] LocalList转换成Map，实现基本的聚合
- [X] Map 也是基于H2或者其他的数据库实现的
- [X] 数据量较少时，不进行持久化相关操作。当数据量超过某个数值时，进行持久化，否则就用ArrayList相关的实现方式来做。这样操作起来会快，而且使用范围更广泛
- [X] 初始化时检查配置，清空当前文件
- [X] 数据文件和 app 相关，防止app共用一个目录的时候文件删错
- [X] 支持List -> LocalList、Map -> LocalMap
- [X] 增加读缓存，在10000条数据以下时，使用写缓存进行读；写缓存的阈值改到10000
- [X] 使用迭代器时，使用预读的方式提高读取的速度
- [ ] 集成spring.application.name，读取这个的值来配置默认的目录，防止在共享目录时，启动时删除其他的微服务的数据库文件
- [ ] 本地磁盘的目录可以选定成SSD的，这个本身已经有配置，TODO 文档化


# 集成方式，以sqlite举例：
```xml
<!-- https://mvnrepository.com/artifact/io.github.lordeath/local-collection -->
<dependency>
    <groupId>io.github.lordeath</groupId>
    <artifactId>local-collection</artifactId>
    <version>1.0.20250306.1</version>
</dependency>
```

```java
// 全局设置一遍即可（默认是sqlite不需要设置，在用h2的时候才设置）
// 这一块可能是过度设计，用户不需要关心内部实现是sqlite还是h2
System.setProperty(CONST_DB_ENGINE, "sqlite");

// 使用时最好要记得关闭，否则数据可能会残留在磁盘上（自动也会回收，当对象被内存回收时，会进行本地表的drop，这个逻辑通过Cleaner和finalize实现，不保证执行）
try (LocalList<String> list = new LocalList<>(String.class)) {
    list.add("a");
    list.add("b");
}
```


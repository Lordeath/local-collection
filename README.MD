我想做一个本地缓存，用作替代ArrayList或者HashMap，使用本地的硬盘上的缓存来降低内存的要求

基本思路是使用H2或者SQLite等本地数据库，优先使用H2，之后也会考虑集成其他的数据库引擎

已经完成了一些基础的操作，操作方法详见[NormalTest](src/test/java/com/fxm/local/collection/test/NormalTest.java)

后续想要实现的功能：

- LocalList转换成Map，实现基本的聚合
- Map 也是基于H2或者其他的数据库实现的
- 支持更多的数据库，比如SQLite

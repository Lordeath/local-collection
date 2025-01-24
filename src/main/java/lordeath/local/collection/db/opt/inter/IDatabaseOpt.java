package lordeath.local.collection.db.opt.inter;

import lordeath.local.collection.db.bean.LocalColumn;
import lordeath.local.collection.db.bean.LocalColumnForMap;

import java.util.Collection;
import java.util.List;

/**
 * 数据库操作接口
 * @param <T> 数据类型
 */
public interface IDatabaseOpt<T> {
    /**
     * 添加一个对象
     * @param obj 要添加的对象
     * @return 是否添加成功
     */
    boolean add(T obj);

    /**
     * 批量添加对象
     * @param c 要添加的对象集合
     * @return 是否添加成功
     */
    boolean addAll(Collection<? extends T> c);

    /**
     * 移除指定索引的对象
     * @param index 要移除的对象索引
     * @return 被移除的对象
     */
    T remove(int index);

    /**
     * 清空所有数据
     */
    void clear();

    /**
     * 关闭数据库连接
     */
    void close();

    /**
     * 获取数据总数
     * @return 数据总数
     */
    int size();

    /**
     * 获取指定索引的对象
     * @param index 对象索引
     * @return 指定索引的对象
     */
    T get(int index);

    /**
     * 设置指定索引的对象
     * @param index 对象索引
     * @param element 要设置的对象
     * @return 原对象
     */
    T set(int index, T element);

    /**
     * 获取指定索引的主键值
     * @param index 对象索引
     * @return 主键值
     */
    long pk(int index);

    /**
     * 批量查询
     * @param fromIndex 开始索引
     * @param toIndex 结束索引
     * @return 查询结果列表
     */
    List<T> batchQuery(int fromIndex, int toIndex);

    /**
     * 获取表名
     * @return 表名
     */
    String getTableName();

    /**
     * 创建一个新表，用于存储分组后的数据
     * @param newTableName 新表名
     * @param groupByColumns 分组字段
     * @param whereClause 过滤条件
     * @param keyColumn 键列名
     * @param resultColumns 结果列
     * @return 是否创建成功
     */
    boolean createGroupedTable(String newTableName, List<String> groupByColumns,
                             String whereClause, String keyColumn, List<LocalColumn> resultColumns);

    /**
     * 将数据从源表插入到目标表
     * @param sourceTableName 源表名
     * @param targetTableName 目标表名
     * @param groupByColumns 分组字段
     * @param whereClause 过滤条件
     * @param columnForMapList 列映射列表
     * @return 是否插入成功
     */
    boolean insertGroupedData(String sourceTableName, String targetTableName,
                            List<String> groupByColumns, String whereClause,
                            List<LocalColumnForMap> columnForMapList);

    /**
     * 根据key查询单个对象
     * @param keyColumn key列名
     * @param keyValue key值
     * @return 查询结果
     */
    T getByKey(String keyColumn, Object keyValue);

    /**
     * 根据key存储对象
     * @param keyColumn key列名
     * @param key key值
     * @param value 要存储的对象
     * @return 原对象（如果存在）
     */
    T putByKey(String keyColumn, String key, T value);

    /**
     * 根据key删除对象
     * @param keyColumn key列名
     * @param keyValue key值
     * @return 是否删除成功
     */
    boolean removeByKey(String keyColumn, Object keyValue);

    /**
     * 获取所有的key值
     * @param keyColumn key列名
     * @return key值列表
     */
    List<String> getAllKeys(String keyColumn);
}

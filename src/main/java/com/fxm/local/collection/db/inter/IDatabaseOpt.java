package com.fxm.local.collection.db.inter;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.fxm.local.collection.db.bean.LocalColumnForMap;

import java.util.Collection;
import java.util.List;

public interface IDatabaseOpt<T> {
    boolean add(T obj);

    boolean addAll(Collection<? extends T> c);

    T remove(int index);

    void clear();

    void close();

    int size();

    T get(int index);

    T set(int index, T element);

    long pk(int index);

    /**
     * 批量查询
     *
     * @param fromIndex 开始索引
     * @param toIndex   结束索引
     * @return 查询结果列表
     */
    List<T> batchQuery(int fromIndex, int toIndex);

    /**
     * 获取表名
     *
     * @return 表名
     */
    String getTableName();

    /**
     * 创建一个新表，用于存储分组后的数据
     *
     * @param newTableName   新表名
     * @param groupByColumns 分组字段
     * @param whereClause    过滤条件
     * @param keyColumn      键列名
     * @param resultColumns  结果列
     * @return 是否创建成功
     */
    boolean createGroupedTable(String newTableName, List<String> groupByColumns,
                               String whereClause, String keyColumn, List<LocalColumn> resultColumns);

    /**
     * 将数据从源表插入到目标表
     *
     * @param sourceTableName 源表名
     * @param targetTableName 目标表名
     * @param groupByColumns  分组字段
     * @param whereClause     过滤条件
     * @param keyColumn       键列名
     * @param resultColumns   结果列
     * @return 是否插入成功
     */
    boolean insertGroupedData(String sourceTableName, String targetTableName,
                              List<String> groupByColumns, String whereClause,
                              List<LocalColumnForMap> columnForMapList);

    /**
     * 根据key查询单个对象
     *
     * @param keyColumn key列名
     * @param keyValue  key值
     * @return 查询结果
     */
    T getByKey(String keyColumn, Object keyValue);
    T putByKey(String keyColumn, String key, T value);

    /**
     * 根据key删除对象
     *
     * @param keyColumn key列名
     * @param keyValue  key值
     * @return 是否删除成功
     */
    boolean removeByKey(String keyColumn, Object keyValue);

    /**
     * 获取所有的key值
     *
     * @param keyColumn key列名
     * @return key值列表
     */
    List<String> getAllKeys(String keyColumn);

}

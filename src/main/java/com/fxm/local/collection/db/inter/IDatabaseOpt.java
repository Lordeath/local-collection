package com.fxm.local.collection.db.inter;

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
     * 批量查询指定范围的数据
     * @param fromIndex 起始索引（包含）
     * @param toIndex 结束索引（不包含）
     * @return 查询结果列表
     */
    List<T> batchQuery(int fromIndex, int toIndex);
}

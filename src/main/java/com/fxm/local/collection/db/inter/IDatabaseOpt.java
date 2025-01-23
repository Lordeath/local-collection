package com.fxm.local.collection.db.inter;

import java.util.Collection;

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
}

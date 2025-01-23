package com.fxm.local.collection.db.inter;

import java.util.Collection;

public interface IDatabaseOpt<T> {
    boolean add(T obj);
    boolean addAll(Collection<? extends T> c);

    void remove(T obj);
    T remove(int index);

    void update(T obj);

    void query(T obj);

    void queryAll();

    void clear();

    void close();

    int size();

    T get(int index);

    T set(int index, T element);

}

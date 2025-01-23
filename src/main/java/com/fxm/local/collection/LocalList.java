package com.fxm.local.collection;

import com.fxm.local.collection.db.impl.H2Opt;
import com.fxm.local.collection.db.inter.IDatabaseOpt;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static com.fxm.local.collection.db.config.MainConfig.CONST_DB_ENGINE;

/**
 * 参考的是ArrayList，但是实现方式是H2数据库或者其他数据库
 */
@Slf4j
public class LocalList<T> implements AutoCloseable, List<T> {

    private final IDatabaseOpt<T> databaseOpt;

    public LocalList(Class<T> clazz) {
        String dbEngine = System.getProperty(CONST_DB_ENGINE);
        if (dbEngine == null || "h2".equals(dbEngine)) {
            databaseOpt = new H2Opt<>(clazz);
        } else {
            throw new IllegalArgumentException("其他的数据库暂时不支持: " + dbEngine);
        }
    }

    @Override
    public void close() throws Exception {
        databaseOpt.close();
    }

    @Override
    public int size() {
        return databaseOpt.size();
    }

    @Override
    public boolean isEmpty() {
        return databaseOpt.size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(T t) {
        return databaseOpt.add(t);
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return databaseOpt.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        databaseOpt.clear();
    }

    @Override
    public T get(int index) {
        return databaseOpt.get(index);
    }

    @Override
    public T set(int index, T element) {
        return databaseOpt.set(index, element);
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T remove(int index) {
        return databaseOpt.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }
}
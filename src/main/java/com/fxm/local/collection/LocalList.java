package com.fxm.local.collection;

import com.fxm.local.collection.db.impl.DerbyOpt;
import com.fxm.local.collection.db.impl.H2Opt;
import com.fxm.local.collection.db.impl.HSQLDBOpt;
import com.fxm.local.collection.db.impl.SqliteOpt;
import com.fxm.local.collection.db.inter.IDatabaseOpt;
import com.fxm.local.collection.db.util.DBUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static com.fxm.local.collection.db.config.MainConfig.CONST_DB_ENGINE;

/**
 * 参考的是ArrayList，但是实现方式是H2数据库或者其他数据库
 * 注意，这个类是线程不安全的
 */
@Slf4j
public class LocalList<T> implements AutoCloseable, List<T> {

    private IDatabaseOpt<T> databaseOpt;

    public LocalList(Class<T> clazz) {
        init(clazz);
    }

    private void init(Class<T> clazz) {
        String dbEngine = System.getProperty(CONST_DB_ENGINE);
        if (dbEngine == null || "h2".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new H2Opt<>(clazz);
        } else if ("sqlite".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new SqliteOpt<>(clazz);
        } else if ("hsqldb".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new HSQLDBOpt<>(clazz);
        } else if ("derby".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new DerbyOpt<>(clazz);
        } else {
            throw new IllegalArgumentException("其他的数据库暂时不支持: " + dbEngine);
        }
    }

    public LocalList() {
        databaseOpt = null;
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
        return new LocalListIterator();
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
        if (databaseOpt == null) {
            init((Class<T>) t.getClass());
        }
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
        return new LocalListIterator();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return new LocalListIterator(index);
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        if (fromIndex < 0)
            throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
        if (toIndex > size())
            throw new IndexOutOfBoundsException("toIndex = " + toIndex);
        if (fromIndex > toIndex)
            throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");

        // 使用批量查询获取数据
        List<T> batchResult = databaseOpt.batchQuery(fromIndex, toIndex);

        // 返回一个不可修改的List视图
        return Collections.unmodifiableList(batchResult);
    }

    /**
     * 获取一个可以修改的子列表。这个子列表会将数据加载到内存中，提高访问效率。
     * 注意：对返回的列表的修改不会影响原始列表。
     *
     * @param fromIndex 起始索引（包含）
     * @param toIndex   结束索引（不包含）
     * @return 包含指定范围元素的新ArrayList
     */
    public List<T> subListInMemory(int fromIndex, int toIndex) {
        if (fromIndex < 0)
            throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
        if (toIndex > size())
            throw new IndexOutOfBoundsException("toIndex = " + toIndex);
        if (fromIndex > toIndex)
            throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");

        // 使用批量查询获取数据并返回一个新的ArrayList
        return Collections.unmodifiableList(databaseOpt.batchQuery(fromIndex, toIndex));
    }

    public long pk(int index) {
        return databaseOpt.pk(index);
    }

    private class LocalListIterator implements ListIterator<T> {
        private int cursor;
        private int lastRet = -1;

        LocalListIterator() {
            this(0);
        }

        LocalListIterator(int index) {
            if (index < 0 || index > size())
                throw new IndexOutOfBoundsException("Index: " + index);
            cursor = index;
        }

        @Override
        public boolean hasNext() {
            return cursor < size();
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new java.util.NoSuchElementException();
            lastRet = cursor;
            return get(cursor++);
        }

        @Override
        public boolean hasPrevious() {
            return cursor > 0;
        }

        @Override
        public T previous() {
            if (!hasPrevious())
                throw new java.util.NoSuchElementException();
            lastRet = --cursor;
            return get(cursor);
        }

        @Override
        public int nextIndex() {
            return cursor;
        }

        @Override
        public int previousIndex() {
            return cursor - 1;
        }

        @Override
        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();

            try {
                LocalList.this.remove(lastRet);
                if (lastRet < cursor)
                    cursor--;
                lastRet = -1;
            } catch (IndexOutOfBoundsException e) {
                throw new java.util.ConcurrentModificationException();
            }
        }

        @Override
        public void set(T t) {
            if (lastRet < 0)
                throw new IllegalStateException();

            try {
                LocalList.this.set(lastRet, t);
            } catch (IndexOutOfBoundsException e) {
                throw new java.util.ConcurrentModificationException();
            }
        }

        @Override
        public void add(T t) {
            try {
                LocalList.this.add(cursor++, t);
                lastRet = -1;
            } catch (IndexOutOfBoundsException e) {
                throw new java.util.ConcurrentModificationException();
            }
        }
    }
}

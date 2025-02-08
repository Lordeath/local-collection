package lordeath.local.collection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lordeath.local.collection.db.bean.LocalColumn;
import lordeath.local.collection.db.bean.LocalColumnForMap;
import lordeath.local.collection.db.opt.factory.DatabaseFactory;
import lordeath.local.collection.db.opt.impl.H2Opt;
import lordeath.local.collection.db.opt.impl.SqliteOpt;
import lordeath.local.collection.db.opt.inter.IDatabaseOpt;
import lordeath.local.collection.db.util.DBUtil;

import javax.sql.DataSource;
import java.lang.ref.Cleaner;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static lordeath.local.collection.db.config.MainConfig.CONST_DB_ENGINE;

/**
 * 参考的是ArrayList，但是实现方式是H2数据库或者其他数据库
 * 注意，这个类是线程不安全的
 */
@Getter
@Slf4j
public class LocalList<T> implements AutoCloseable, List<T> {

    /**
     * 数据库操作对象
     */
    IDatabaseOpt<T> databaseOpt;

    /**
     * 列定义
     */
    List<LocalColumn> columns;

    /**
     * 删除标志
     */
    private final AtomicBoolean removeFlag = new AtomicBoolean(false);

    /**
     * 大小计数器
     */
    private final AtomicInteger sizeCounter = new AtomicInteger(0);

    /**
     * 清理器
     */
    private static final Cleaner cleaner = Cleaner.create();

    /**
     * 清理对象
     */
    private Cleaner.Cleanable cleanable;

    /**
     * 创建一个空的LocalList
     */
    public LocalList() {
        databaseOpt = null;
    }

    /**
     * 使用指定的列定义创建LocalList
     *
     * @param clazz 元素类型
     */
    public LocalList(Class<T> clazz) {
        init(clazz);
    }

    /**
     * 使用指定的列定义创建LocalList
     *
     * @param clazz         元素类型
     * @param tableName     表名
     * @param columnsForMap 列映射定义
     */
    public LocalList(Class<T> clazz, String tableName, List<LocalColumnForMap> columnsForMap) {
        this.columns = columnsForMap.stream().map(LocalColumnForMap::getSinkColumn).collect(Collectors.toList());

        // 创建数据库操作对象
        this.databaseOpt = DatabaseFactory.createDatabaseOpt(clazz, tableName, columnsForMap);
        DataSource dataSource = databaseOpt.getDataSource();
        cleanable = cleaner.register(this, () -> DBUtil.drop(tableName, dataSource));
    }

    /**
     * 初始化数据库操作对象
     *
     * @param clazz 元素类型
     */
    void init(Class<T> clazz) {
        String dbEngine = System.getProperty(CONST_DB_ENGINE);
        if (dbEngine == null || "sqlite".equalsIgnoreCase(dbEngine)) {
            // 默认的改成sqlite，因为sqlite的内存降低是最明显的
            databaseOpt = new SqliteOpt<>(clazz);
        } else if ("h2".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new H2Opt<>(clazz);
        } else {
            throw new IllegalArgumentException("其他的数据库暂时不支持: " + dbEngine);
        }

        String tableName = databaseOpt.getTableName();
        DataSource dataSource = databaseOpt.getDataSource();
        cleanable = cleaner.register(this, () -> DBUtil.drop(tableName, dataSource));
    }

    /**
     * 关闭数据库连接
     */
    @Override
    public void close() {
        try {
            Optional.ofNullable(databaseOpt).ifPresent(IDatabaseOpt::close);
        } catch (Throwable ignored) {
        }
    }

    /**
     * 确保对象回收时，删除表
     *
     * @throws Throwable 可能抛出异常
     */
    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    /**
     * 获取列表大小
     *
     * @return 列表大小
     */
    @Override
    public int size() {
        return sizeCounter.get();
    }

    /**
     * 判断列表是否为空
     *
     * @return 列表为空返回true，否则返回false
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * 判断列表是否包含指定元素
     *
     * @param o 元素
     * @return 列表包含元素返回true，否则返回false
     */
    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取列表迭代器
     *
     * @return 列表迭代器
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<T> iterator() {
        return new LocalListIterator();
    }

    /**
     * 将列表转换为数组
     *
     * @return 列表数组
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    /**
     * 将列表转换为指定类型数组
     *
     * @param a 数组类型
     * @return 列表数组
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public <T1> T1[] toArray(T1[] a) {
        throw new UnsupportedOperationException();
    }

    /**
     * 添加元素到列表，并更新计数器。
     *
     * @param t 要添加的元素
     * @return 添加成功返回true，否则返回false
     */
    @Override
    public boolean add(T t) {
        if (databaseOpt == null) {
            //noinspection unchecked
            init((Class<T>) t.getClass());
        }
        boolean b = databaseOpt.add(t);
        if (b) {
            sizeCounter.incrementAndGet();
        }
        return b;
    }

    /**
     * 移除指定元素
     *
     * @param o 元素
     * @return 移除成功返回true，否则返回false
     */
    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * 判断列表是否包含指定集合
     *
     * @param c 集合
     * @return 列表包含集合返回true，否则返回false
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * 添加指定集合到列表
     *
     * @param c 集合
     * @return 添加成功返回true，否则返回false
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public boolean addAll(Collection<? extends T> c) {
        if (databaseOpt == null && c != null && !c.isEmpty()) {
            Class<T> clazz = (Class<T>) c.iterator().next().getClass();
            init(clazz);
        }
        if (databaseOpt == null) {
            throw new RuntimeException("数据源操作初始化失败");
        }
        boolean b = databaseOpt.addAll(c);
        if (b) {
            sizeCounter.addAndGet(c.size());
        }
        return b;
    }

    /**
     * 添加指定集合到列表指定位置
     *
     * @param index 索引
     * @param c     集合
     * @return 添加成功返回true，否则返回false
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * 移除指定集合
     *
     * @param c 集合
     * @return 移除成功返回true，否则返回false
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * 保留指定集合
     *
     * @param c 集合
     * @return 保留成功返回true，否则返回false
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * 清空列表
     */
    @Override
    public void clear() {
        databaseOpt.clear();
        sizeCounter.set(0);
    }

    /**
     * 获取指定索引元素
     *
     * @param index 索引
     * @return 元素
     */
    @Override
    public T get(int index) {
        return databaseOpt.get(index, removeFlag.get());
    }

    /**
     * 设置指定索引元素
     *
     * @param index  索引
     * @param element 元素
     * @return 被设置的元素
     */
    @Override
    public T set(int index, T element) {
        return databaseOpt.set(index, element);
    }

    /**
     * 添加元素到指定索引
     *
     * @param index 索引
     * @param element 元素
     */
    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    /**
     * 移除指定索引处的元素，并更新计数器。
     *
     * @param index 要移除的元素索引
     * @return 被移除的元素
     */
    @Override
    public T remove(int index) {
        removeFlag.set(true);
        T t = databaseOpt.remove(index);
        if (t != null) {
            sizeCounter.decrementAndGet();
        }
        return t;
    }

    /**
     * 获取指定元素索引
     *
     * @param o 元素
     * @return 索引
     */
    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取指定元素最后索引
     *
     * @param o 元素
     * @return 索引
     */
    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取列表迭代器
     *
     * @return 列表迭代器
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public ListIterator<T> listIterator() {
        return new LocalListIterator();
    }

    /**
     * 获取列表迭代器指定索引
     *
     * @param index 索引
     * @return 列表迭代器
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public ListIterator<T> listIterator(int index) {
        return new LocalListIterator(index);
    }

    /**
     * 获取一个可以修改的子列表。这个子列表会将数据加载到内存中，提高访问效率。
     * 注意：对返回的列表的修改不会影响原始列表。
     *
     * @param fromIndex 起始索引（包含）
     * @param toIndex   结束索引（不包含）
     * @return 包含指定范围元素的新ArrayList
     */
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
     * 检索指定索引处的主键。
     *
     * @param index 要检索的主键的索引
     * @return 主键的长整型值
     */
    public long pk(int index) {
        if (!removeFlag.get()) {
            return index + 1;
        }
        return databaseOpt.pk(index);
    }

    /**
     * 根据给定的键向指定表中添加或更新对象。
     *
     * @param keyColumn 键列名
     * @param key      键值
     * @param value    对象值
     * @return 被更新或添加的对象
     */
    public T putByKey(String keyColumn, String key, T value) {
        AtomicBoolean removed = new AtomicBoolean(false);
        T t = databaseOpt.putByKey(keyColumn, key, value, removed);
        if (!removed.get()) {
            // 新加的元素，计数+1
            sizeCounter.incrementAndGet();
        }
        return t;
    }

    /**
     * 根据给定的键删除对象。
     *
     * @param keyColumn 键列名
     * @param key      键值
     */
    public void removeByKey(String keyColumn, Object key) {
        boolean b = databaseOpt.removeByKey(keyColumn, key);
        if (b) {
            sizeCounter.decrementAndGet();
        }
    }

    /**
     * 插入分组数据。
     *
     * @param tableName      源表名
     * @param newTableName   目标表名
     * @param groupByColumns 分组列
     * @param whereClause     where条件
     * @param columnForMapList 列映射
     */
    public void insertGroupedData(String tableName, String newTableName, List<String> groupByColumns, String whereClause, List<LocalColumnForMap> columnForMapList) {
        databaseOpt.insertGroupedData(tableName, newTableName, groupByColumns, whereClause, columnForMapList);
        // 刷新计数器
        sizeCounter.set(databaseOpt.size());
    }

    /**
     * 列表迭代器实现
     */
    private class LocalListIterator implements ListIterator<T> {
        /**
         * 游标
         */
        private int cursor;

        /**
         * 最后返回的索引
         */
        private int lastRet = -1;

        /**
         * 构造函数
         */
        LocalListIterator() {
            this(0);
        }

        /**
         * 构造函数
         *
         * @param index 索引
         */
        LocalListIterator(int index) {
            if (index < 0 || index > size())
                throw new IndexOutOfBoundsException("Index: " + index);
            cursor = index;
        }

        /**
         * 判断是否有下一个元素
         *
         * @return 有下一个元素返回true，否则返回false
         */
        @Override
        public boolean hasNext() {
            return cursor < size();
        }

        /**
         * 获取下一个元素
         *
         * @return 下一个元素
         */
        @Override
        public T next() {
            if (!hasNext())
                throw new java.util.NoSuchElementException();
            lastRet = cursor;
            return get(cursor++);
        }

        /**
         * 判断是否有前一个元素
         *
         * @return 有前一个元素返回true，否则返回false
         */
        @Override
        public boolean hasPrevious() {
            return cursor > 0;
        }

        /**
         * 获取前一个元素
         *
         * @return 前一个元素
         */
        @Override
        public T previous() {
            if (!hasPrevious())
                throw new java.util.NoSuchElementException();
            lastRet = --cursor;
            return get(cursor);
        }

        /**
         * 获取下一个索引
         *
         * @return 下一个索引
         */
        @Override
        public int nextIndex() {
            return cursor;
        }

        /**
         * 获取前一个索引
         *
         * @return 前一个索引
         */
        @Override
        public int previousIndex() {
            return cursor - 1;
        }

        /**
         * 移除当前元素
         */
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

        /**
         * 设置当前元素
         *
         * @param t 元素
         */
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

        /**
         * 添加元素
         *
         * @param t 元素
         */
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

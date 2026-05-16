package lordeath.local.collection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lordeath.local.collection.db.bean.LocalColumn;
import lordeath.local.collection.db.bean.LocalColumnForMap;
import lordeath.local.collection.db.config.MainConfig;
import lordeath.local.collection.db.opt.impl.DatabaseFactory;
import lordeath.local.collection.db.opt.inter.IDatabaseOpt;
import lordeath.local.collection.db.util.ColumnNameUtil;
import lordeath.local.collection.db.util.DBUtil;
import lordeath.local.collection.serialize.TypeCodec;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.math.BigDecimal;
import java.lang.ref.Cleaner;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.stream.Collectors;

/**
 * 参考的是ArrayList，但是实现方式是H2数据库或者其他数据库
 * 注意，这个类是线程不安全的
 *
 * @param <T> the type of elements in this collection
 */
@Getter
@Slf4j
public class LocalList<T> implements AutoCloseable, List<T> {

    /**
     * 获取线程安全包装的LocalList。
     *
     * @param list 原始LocalList
     * @param <T>  元素类型
     * @return 线程安全包装实例
     */
    public static <T> SynchronizedLocalList<T> synchronizedList(LocalList<T> list) {
        if (list == null) {
            throw new IllegalArgumentException("list is null");
        }
        return new SynchronizedLocalList<>(list);
    }

    /**
     * 数据库操作对象
     */
    IDatabaseOpt<T> databaseOpt;

    /**
     * 列定义
     */
    List<LocalColumn> columns;

    final int cacheSize;
    final ArrayList<T> cache;

    /**
     * 删除标志
     */
    private final AtomicBoolean removeFlag = new AtomicBoolean(false);
    /**
     * 最近一次刷盘时间
     */
    private volatile long lastFlushMillis = System.currentTimeMillis();

    /**
     * 大小计数器
     */
    private final AtomicInteger sizeCounter = new AtomicInteger(0);
    /**
     * 运行时指标
     */
    private final LocalListRuntimeMetrics runtimeMetrics = new LocalListRuntimeMetrics();
    private volatile boolean recoveryRequired = false;
    private static final String RECOVERY_TABLE_PREFIX = "lc_recovery_";
    private static final String RECOVERY_STATE_NORMAL = "NORMAL";
    private static final String RECOVERY_STATE_FLUSHING = "FLUSHING";
    private static final String RECOVERY_STATE_CORRUPTED = "CORRUPTED";
    /**
     * 缓存持久化计数器
     */
//    private final AtomicInteger cacheToDBCounter = new AtomicInteger(0);
//    private final AtomicBoolean cacheToDBFlag = new AtomicBoolean(false);
    private volatile boolean cacheToDBFlag = false;

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
        cacheSize = MainConfig.CACHE_SIZE.getPropertyInt();
        cache = new ArrayList<>(cacheSize);
    }

    /**
     * 使用指定的列定义创建LocalList
     *
     * @param clazz 元素类型
     */
    public LocalList(Class<T> clazz) {
        init(clazz);
        cacheSize = MainConfig.CACHE_SIZE.getPropertyInt();
        cache = new ArrayList<>(cacheSize);
    }

    /**
     * 使用指定的列定义创建LocalList
     *
     * @param clazz         元素类型
     * @param tableName     表名
     * @param columnsForMap 列映射定义
     */
    /**
     * 获取运行时指标对象。
     *
     * @return 运行时指标
     */
    public LocalListRuntimeMetrics getRuntimeMetrics() {
        return runtimeMetrics;
    }
    public LocalList(Class<T> clazz, String tableName, List<LocalColumnForMap> columnsForMap) {
        this.columns = columnsForMap.stream().map(LocalColumnForMap::getSinkColumn).collect(Collectors.toList());

        // 创建数据库操作对象
        this.databaseOpt = DatabaseFactory.createDatabaseOptForMap(clazz, tableName, columnsForMap);
        DataSource dataSource = databaseOpt.getDataSource();
        cleanable = cleaner.register(this, () -> DBUtil.drop(tableName, dataSource));
//        cacheSize = MainConfig.CACHE_SIZE.getPropertyInt();
        // map不进行缓存，直接使用db的数据
        cacheSize = 0;
        cache = new ArrayList<>(cacheSize);
        initRecoveryState();
    }

    /**
     * 初始化数据库操作对象
     *
     * @param clazz 元素类型
     */
    void init(Class<T> clazz) {
        databaseOpt = DatabaseFactory.createDatabaseOptForList(clazz);
        columns = ColumnNameUtil.getFields(clazz);
        String tableName = databaseOpt.getTableName();
        DataSource dataSource = databaseOpt.getDataSource();
        cleanable = cleaner.register(this, () -> DBUtil.drop(tableName, dataSource));
        initRecoveryState();
    }

    /**
     * 关闭数据库连接
     */
    @Override
    public void close() {
        try {
            restoreCacheToDB();
            recoveryComplete();
        } catch (Throwable ignored) {
            markRecoveryState(RECOVERY_STATE_CORRUPTED, "close-failed");
        } finally {
            Optional.ofNullable(databaseOpt).ifPresent(IDatabaseOpt::close);
        }
    }

    /**
     * 确保对象回收时，删除表
     *
     * @throws Throwable 可能抛出异常
     */
    @SuppressWarnings({"removal", "deprecation"})
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
        boolean b = cacheSize > 0 && !cacheToDBFlag;
        if (b) {
            return cache.size();
        }
        restoreCacheToDB();
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
        boolean b = cacheSize > 0 && !cacheToDBFlag;
        if (b) {
            return cache.iterator();
        }
        restoreCacheToDB();
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
    @SuppressWarnings("unchecked")
    @Override
    public boolean add(T t) {
        if (databaseOpt == null) {
            init((Class<T>) t.getClass());
        }
        if (cacheSize > 0 && !cacheToDBFlag) {
            flushByIntervalIfNeeded();
        }
        if (cacheSize <= 0) {
            boolean b = databaseOpt.add(t);
            if (b) {
                sizeCounter.incrementAndGet();
                runtimeMetrics.recordDatabaseWrite(1);
                runtimeMetrics.recordDatabaseSize(sizeCounter.get());
            }
            return b;
        }
        if (cache.size() >= cacheSize) {
            restoreCacheToDB();
        }
        boolean b = cache.add(t);
        if (b) {
            sizeCounter.incrementAndGet();
            runtimeMetrics.recordCacheWrite();
            runtimeMetrics.recordDatabaseSize(sizeCounter.get());
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
    @SuppressWarnings({"NullableProblems", "unchecked"})
    @Override
    public boolean addAll(Collection<? extends T> c) {
        if (databaseOpt == null && c != null && !c.isEmpty()) {
            Class<T> clazz = (Class<T>) c.iterator().next().getClass();
            init(clazz);
        }
        if (databaseOpt == null) {
            throw new RuntimeException("数据源操作初始化失败");
        }

        if (cacheSize > 0 && !cacheToDBFlag) {
            flushByIntervalIfNeeded();
        }
        if (cacheSize <= 0) {
            return addAllLocally(c);
        }
        if (c.size() >= cacheSize) {
            // 如果传入的列表大于缓存上限，直接不使用缓存
            restoreCacheToDB();
            return addAllLocally(c);
        }

        if (cache.size() >= cacheSize) {
            restoreCacheToDB();
        }
        boolean b = cache.addAll(c);
        if (cache.size() >= cacheSize) {
            restoreCacheToDB();
        }
        if (b) {
            sizeCounter.addAndGet(c.size());
            runtimeMetrics.recordCacheWrite(c.size());
            runtimeMetrics.recordDatabaseSize(sizeCounter.get());
        }
        return b;
    }

    private boolean addAllLocally(Collection<? extends T> c) {
        boolean b = databaseOpt.addAll(c);
        if (b) {
            sizeCounter.addAndGet(c.size());
            runtimeMetrics.recordDatabaseWrite(c.size());
            runtimeMetrics.recordDatabaseSize(sizeCounter.get());
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
        cache.clear();
        databaseOpt.clear();
        sizeCounter.set(0);
        cacheToDBFlag = false;
        runtimeMetrics.recordDatabaseSize(0);
        recoveryComplete();
    }

    /**
     * 当前实例是否需要恢复处理。
     *
     * @return true 表示检测到非正常关闭或上次刷新未完成
     */
    public boolean isRecoveryRequired() {
        return recoveryRequired;
    }

    /**
     * 将当前实例标记为恢复完成。恢复流程一般在读取快照并恢复后调用。
     */
    public void recoveryComplete() {
        markRecoveryState(RECOVERY_STATE_NORMAL, "recovery-complete");
        recoveryRequired = false;
    }

    private String getRecoveryTableName() {
        if (databaseOpt == null) {
            return null;
        }
        return RECOVERY_TABLE_PREFIX + databaseOpt.getTableName();
    }

    private void initRecoveryState() {
        if (databaseOpt == null) {
            return;
        }
        try {
            String tableName = getRecoveryTableName();
            DataSource dataSource = databaseOpt.getDataSource();
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("create table if not exists " + tableName
                        + " (id integer primary key, state varchar(64), updated_at bigint, detail varchar(255))");
                statement.execute("INSERT INTO " + tableName
                        + " (id, state, updated_at, detail) "
                        + "SELECT 1, '" + RECOVERY_STATE_NORMAL + "', " + System.currentTimeMillis() + ", '' "
                        + "WHERE NOT EXISTS (SELECT 1 FROM " + tableName + " WHERE id = 1)");
            }
            String state = readRecoveryState();
            if (!RECOVERY_STATE_NORMAL.equals(state)) {
                recoveryRequired = true;
                log.warn("检测到异常恢复状态，table={} state={}", databaseOpt.getTableName(), state);
            } else {
                recoveryRequired = false;
            }
        } catch (Throwable e) {
            recoveryRequired = true;
            log.error("恢复状态初始化失败，table={}", databaseOpt.getTableName(), e);
        }
    }

    /**
     * 获取指定索引元素
     *
     * @param index 索引
     * @return 元素
     */
    @Override
    public T get(int index) {
        boolean b = cacheSize > 0 && !cacheToDBFlag;
        if (b) {
            T t = cache.get(index);
            runtimeMetrics.recordCacheHit();
            return t;
        }
        restoreCacheToDB();
        T t = databaseOpt.get(index, removeFlag.get());
        runtimeMetrics.recordCacheMiss();
        return t;
    }

    /**
     * 设置指定索引元素
     *
     * @param index   索引
     * @param element 元素
     * @return 被设置的元素
     */
    @Override
    public T set(int index, T element) {
        boolean b = cacheSize > 0 && !cacheToDBFlag;
        if (b) {
            T old = cache.set(index, element);
            runtimeMetrics.recordCacheWrite();
            return old;
        }
        restoreCacheToDB();
        T old = databaseOpt.get(index, removeFlag.get());
        if (old == null) {
            return null;
        }
        databaseOpt.set(index, element);
        if (element != null) {
            runtimeMetrics.recordDatabaseWrite(1);
            runtimeMetrics.recordDatabaseSize(sizeCounter.get());
        }
        return old;
    }

    /**
     * 添加元素到指定索引
     *
     * @param index   索引
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
        boolean b = cacheSize > 0 && !cacheToDBFlag;
        if (b) {
            runtimeMetrics.recordCacheWrite();
            sizeCounter.decrementAndGet();
            runtimeMetrics.recordDatabaseSize(sizeCounter.get());
            return cache.remove(index);
        }
        restoreCacheToDB();
        removeFlag.set(true);
        T t = databaseOpt.remove(index);
        if (t != null) {
            sizeCounter.decrementAndGet();
            runtimeMetrics.recordDatabaseWrite(1);
            runtimeMetrics.recordDatabaseSize(sizeCounter.get());
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
    @SuppressWarnings("NullableProblems")
    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        boolean b = cacheSize > 0 && !cacheToDBFlag;
        if (b) {
            return cache.subList(fromIndex, toIndex);
        }
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
        restoreCacheToDB();
        if (!removeFlag.get()) {
            return index + 1;
        }
        return databaseOpt.pk(index);
    }

    /**
     * 根据给定的键向指定表中添加或更新对象。
     *
     * @param keyColumn 键列名
     * @param key       键值
     * @param value     对象值
     * @return 被更新或添加的对象
     */
    T putByKey(String keyColumn, String key, T value) {
        restoreCacheToDB();
        AtomicBoolean removed = new AtomicBoolean(false);
        T t = databaseOpt.putByKey(keyColumn, key, value, removed);
        runtimeMetrics.recordDatabaseWrite(1);
        if (!removed.get()) {
            // 新加的元素，计数+1
            sizeCounter.incrementAndGet();
            runtimeMetrics.recordDatabaseSize(sizeCounter.get());
        }
        return t;
    }

    /**
     * 根据给定的键删除对象。
     *
     * @param keyColumn 键列名
     * @param key       键值
     */
    void removeByKey(String keyColumn, Object key) {
        restoreCacheToDB();
        boolean b = databaseOpt.removeByKey(keyColumn, key);
        if (b) {
            sizeCounter.decrementAndGet();
            runtimeMetrics.recordDatabaseWrite(1);
            runtimeMetrics.recordDatabaseSize(sizeCounter.get());
        }
    }

    /**
     * 插入分组数据。
     *
     * @param tableName        源表名
     * @param newTableName     目标表名
     * @param groupByColumns   分组列
     * @param whereClause      where条件
     * @param columnForMapList 列映射
     */
    void insertGroupedData(String tableName, String newTableName, List<String> groupByColumns, String whereClause, List<LocalColumnForMap> columnForMapList) {
        restoreCacheToDB();
        int before = sizeCounter.get();
        databaseOpt.insertGroupedData(tableName, newTableName, groupByColumns, whereClause, columnForMapList);
        // 刷新计数器
        sizeCounter.set(databaseOpt.size());
        runtimeMetrics.recordDatabaseWrite(Math.max(0, sizeCounter.get() - before));
        runtimeMetrics.recordDatabaseSize(sizeCounter.get());
    }

    void restoreCacheToDB() {
        if (cache.isEmpty()) {
            return;
        }
        markRecoveryState(RECOVERY_STATE_FLUSHING, "flush-start");
        cacheToDBFlag = true;
        int flushSize = cache.size();
        int chunk = MainConfig.CACHE_FLUSH_CHUNK_SIZE.getPropertyInt();
        if (chunk <= 0) {
            chunk = flushSize;
        }
        long start = System.nanoTime();
        try {
            for (int i = 0; i < cache.size(); i += chunk) {
                int end = Math.min(cache.size(), i + chunk);
                databaseOpt.addAll(cache.subList(i, end));
            }
            cache.clear();
            runtimeMetrics.recordCacheFlush(flushSize, System.nanoTime() - start);
            runtimeMetrics.recordDatabaseWrite(flushSize);
            runtimeMetrics.recordDatabaseSize(sizeCounter.get());
            markRecoveryState(RECOVERY_STATE_NORMAL, "flush-complete");
        } catch (Throwable e) {
            markRecoveryState(RECOVERY_STATE_CORRUPTED, "flush-failed");
            recoveryRequired = true;
            throw e;
        }
        lastFlushMillis = System.currentTimeMillis();
//        cacheToDBFlag.set(true);
//        cacheToDBCounter.incrementAndGet();
//        cacheToDBCounter.get();
    }

    private void flushByIntervalIfNeeded() {
        if (cache.isEmpty()) {
            return;
        }
        int interval = MainConfig.CACHE_FLUSH_INTERVAL_MILLIS.getPropertyInt();
        if (interval <= 0) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastFlushMillis >= interval) {
            restoreCacheToDB();
        }
    }

    private void markRecoveryState(String state, String detail) {
        if (databaseOpt == null) {
            return;
        }
        String tableName = getRecoveryTableName();
        try (Connection connection = databaseOpt.getDataSource().getConnection();
             PreparedStatement query = connection.prepareStatement("select 1 from " + tableName + " where id = 1");
             ResultSet resultSet = query.executeQuery()) {
            if (resultSet.next()) {
                try (PreparedStatement update = connection.prepareStatement(
                        "update " + tableName + " set state = ?, updated_at = ?, detail = ? where id = 1")) {
                    update.setString(1, state);
                    update.setLong(2, System.currentTimeMillis());
                    update.setString(3, detail);
                    update.executeUpdate();
                }
            } else {
                try (PreparedStatement insert = connection.prepareStatement(
                        "insert into " + tableName + " (id, state, updated_at, detail) values (1, ?, ?, ?)")) {
                    insert.setString(1, state);
                    insert.setLong(2, System.currentTimeMillis());
                    insert.setString(3, detail);
                    insert.executeUpdate();
                }
            }
            if (RECOVERY_STATE_CORRUPTED.equals(state) || RECOVERY_STATE_FLUSHING.equals(state)) {
                recoveryRequired = true;
            } else if (RECOVERY_STATE_NORMAL.equals(state)) {
                recoveryRequired = false;
            }
        } catch (SQLException ignored) {
            recoveryRequired = true;
        }
    }

    private String readRecoveryState() {
        if (databaseOpt == null) {
            return RECOVERY_STATE_CORRUPTED;
        }
        String tableName = getRecoveryTableName();
        try (Connection connection = databaseOpt.getDataSource().getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select state from " + tableName + " where id = 1")) {
            if (resultSet.next()) {
                String state = resultSet.getString(1);
                return state == null ? RECOVERY_STATE_NORMAL : state;
            }
            return RECOVERY_STATE_NORMAL;
        } catch (SQLException e) {
            return RECOVERY_STATE_CORRUPTED;
        }
    }

    /**
     * 将当前列表快照导出为 JSON 文件。
     *
     * @param snapshotFile 快照文件
     */
    public void exportToJson(File snapshotFile) {
        if (snapshotFile == null) {
            throw new IllegalArgumentException("snapshotFile is null");
        }
        if (databaseOpt == null || columns == null || columns.isEmpty()) {
            throw new IllegalStateException("列表尚未初始化，无法导出快照");
        }
        restoreCacheToDB();
        try (BufferedWriter writer = Files.newBufferedWriter(snapshotFile.toPath(), StandardCharsets.UTF_8)) {
            writer.write("{\"schema\":[");
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    writer.write(",");
                }
                writer.write('"');
                writer.write(escapeJson(columns.get(i).getColumnName()));
                writer.write('"');
            }
            writer.write("],\"rows\":[");
            for (int i = 0; i < size(); i++) {
                if (i > 0) {
                    writer.write(",");
                }
                T value = get(i);
                writer.write('[');
                for (int j = 0; j < columns.size(); j++) {
                    LocalColumn column = columns.get(j);
                    if (j > 0) {
                        writer.write(",");
                    }
                    Object fieldValue = resolveFieldValue(value, column);
                    if (fieldValue == null) {
                        writer.write("null");
                    } else {
                        writer.write('"');
                        writer.write(escapeJson(toStoredValue(fieldValue, column)));
                        writer.write('"');
                    }
                }
                writer.write(']');
            }
            writer.write("]}");
        } catch (IOException e) {
            throw new RuntimeException("导出JSON快照失败", e);
        }
    }

    /**
     * 从 JSON 快照恢复列表，恢复前会先清空当前列表。
     *
     * @param snapshotFile 快照文件
     */
    public void importFromJson(File snapshotFile) {
        if (snapshotFile == null) {
            throw new IllegalArgumentException("snapshotFile is null");
        }
        if (databaseOpt == null || columns == null || columns.isEmpty()) {
            throw new IllegalStateException("列表尚未初始化，无法恢复快照");
        }
        if (!snapshotFile.exists()) {
            throw new IllegalArgumentException("快照文件不存在: " + snapshotFile);
        }
        clear();
        String text;
        try {
            text = Files.readString(snapshotFile.toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("读取JSON快照失败", e);
        }
        SnapshotRows snapshot = parseJsonSnapshot(text);
        if (snapshot.columnNames.length != columns.size()) {
            throw new IllegalArgumentException("快照 schema 与当前列表结构不一致");
        }
        for (int i = 0; i < columns.size(); i++) {
            if (!columns.get(i).getColumnName().equals(snapshot.columnNames[i])) {
                throw new IllegalArgumentException("快照 schema 与当前列表结构不一致");
            }
        }
        for (List<String> row : snapshot.rows) {
            add(createRecordFromSnapshot(row));
        }
    }

    /**
     * 将当前列表快照导出为 CSV 文件。
     *
     * @param snapshotFile 快照文件
     */
    public void exportToCsv(File snapshotFile) {
        if (snapshotFile == null) {
            throw new IllegalArgumentException("snapshotFile is null");
        }
        if (databaseOpt == null || columns == null || columns.isEmpty()) {
            throw new IllegalStateException("列表尚未初始化，无法导出快照");
        }
        restoreCacheToDB();
        try (BufferedWriter writer = Files.newBufferedWriter(snapshotFile.toPath(), StandardCharsets.UTF_8)) {
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    writer.write(',');
                }
                writer.write(escapeCsv(columns.get(i).getColumnName()));
            }
            writer.newLine();
            for (int i = 0; i < size(); i++) {
                T value = get(i);
                for (int j = 0; j < columns.size(); j++) {
                    if (j > 0) {
                        writer.write(',');
                    }
                    LocalColumn column = columns.get(j);
                    Object fieldValue = resolveFieldValue(value, column);
                    writer.write(escapeCsv(toStoredValue(fieldValue, column)));
                }
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("导出CSV快照失败", e);
        }
    }

    /**
     * 从 CSV 快照恢复列表，恢复前会先清空当前列表。
     *
     * @param snapshotFile 快照文件
     */
    public void importFromCsv(File snapshotFile) {
        if (snapshotFile == null) {
            throw new IllegalArgumentException("snapshotFile is null");
        }
        if (databaseOpt == null || columns == null || columns.isEmpty()) {
            throw new IllegalStateException("列表尚未初始化，无法恢复快照");
        }
        if (!snapshotFile.exists()) {
            throw new IllegalArgumentException("快照文件不存在: " + snapshotFile);
        }
        clear();
        try (BufferedReader reader = Files.newBufferedReader(snapshotFile.toPath(), StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                return;
            }
            List<String> schema = Arrays.asList(parseCsvLine(header));
            if (schema.size() != columns.size()) {
                throw new IllegalArgumentException("快照 schema 与当前列表结构不一致");
            }
            for (int i = 0; i < columns.size(); i++) {
                if (!columns.get(i).getColumnName().equals(schema.get(i))) {
                    throw new IllegalArgumentException("快照 schema 与当前列表结构不一致");
                }
            }
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                List<String> row = Arrays.asList(parseCsvLine(line));
                add(createRecordFromSnapshot(row));
            }
        } catch (IOException e) {
            throw new RuntimeException("读取CSV快照失败", e);
        }
    }

    private String[] parseCsvLine(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder value = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (inQuote && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    value.append('"');
                    i++;
                    continue;
                }
                inQuote = !inQuote;
                continue;
            }
            if (ch == ',' && !inQuote) {
                values.add(value.toString());
                value.setLength(0);
            } else {
                value.append(ch);
            }
        }
        values.add(value.toString());
        return values.toArray(new String[0]);
    }

    private T createRecordFromSnapshot(List<String> row) {
        if (columns.isEmpty()) {
            throw new IllegalStateException("列表未初始化");
        }
        if (row.size() != columns.size()) {
            throw new IllegalArgumentException("快照行列数与当前列表结构不一致");
        }
        if (columns.size() == 1 && columns.get(0).getField() == null) {
            return parseSimpleValue(row.get(0), columns.get(0));
        }
        Class<?> valueClass = columns.get(0).getField().getDeclaringClass();
        Object bean;
        try {
            bean = valueClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("对象实例化失败: " + valueClass, e);
        }
        for (int i = 0; i < columns.size(); i++) {
            LocalColumn column = columns.get(i);
            if (column.getField() == null) {
                continue;
            }
            try {
                column.getField().setAccessible(true);
                column.getField().set(bean, parseSimpleValue(row.get(i), column));
            } catch (IllegalAccessException e) {
                throw new RuntimeException("恢复字段失败: " + column.getColumnName(), e);
            }
        }
        @SuppressWarnings("unchecked")
        T value = (T) bean;
        return value;
    }

    @SuppressWarnings("unchecked")
    private T parseSimpleValue(String raw, LocalColumn column) {
        if (raw == null || raw.isEmpty()) {
            return (T) null;
        }
        TypeCodec codec = column.getTypeCodec();
        if (codec != null) {
            return (T) codec.deserialize(raw, column.getColumnType());
        }
        Object parsed;
        Class<?> targetType = column.getColumnType();
        if (targetType == String.class) {
            parsed = raw;
        } else if (targetType == int.class || targetType == Integer.class) {
            parsed = Integer.valueOf(raw);
        } else if (targetType == long.class || targetType == Long.class) {
            parsed = Long.valueOf(raw);
        } else if (targetType == double.class || targetType == Double.class) {
            parsed = Double.valueOf(raw);
        } else if (targetType == float.class || targetType == Float.class) {
            parsed = Float.valueOf(raw);
        } else if (targetType == boolean.class || targetType == Boolean.class) {
            parsed = Boolean.valueOf(raw);
        } else if (targetType == char.class || targetType == Character.class) {
            parsed = raw.isEmpty() ? null : raw.charAt(0);
        } else if (targetType == Date.class) {
            parsed = new Date(Long.parseLong(raw));
        } else if (targetType == BigDecimal.class) {
            parsed = new BigDecimal(raw);
        } else {
            throw new UnsupportedOperationException("不支持类型恢复: " + targetType);
        }
        return (T) parsed;
    }

    private String toStoredValue(Object value, LocalColumn column) {
        if (value == null) {
            return "";
        }
        TypeCodec codec = column.getTypeCodec();
        if (codec != null) {
            return codec.serialize(value);
        }
        if (value instanceof String) {
            return value.toString();
        }
        if (value instanceof Date) {
            return Long.toString(((Date) value).getTime());
        }
        if (value instanceof java.sql.Date) {
            return Long.toString(((java.sql.Date) value).getTime());
        }
        return String.valueOf(value);
    }

    private Object resolveFieldValue(T value, LocalColumn column) {
        if (columns.size() == 1 && column.getField() == null) {
            return value;
        }
        try {
            if (column.getField() != null) {
                column.getField().setAccessible(true);
                return column.getField().get(value);
            }
            return null;
        } catch (IllegalAccessException e) {
            throw new RuntimeException("读取字段失败: " + column.getColumnName(), e);
        }
    }

    private String escapeCsv(String text) {
        if (text == null) {
            return "";
        }
        String escaped = text.replace("\"", "\"\"");
        boolean needQuote = escaped.contains(",") || escaped.contains("\"") || escaped.contains("\n") || escaped.contains("\r");
        if (needQuote) {
            return "\"" + escaped + "\"";
        }
        return escaped;
    }

    private String escapeJson(String text) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            switch (ch) {
                case '"':
                    result.append("\\\"");
                    break;
                case '\\':
                    result.append("\\\\");
                    break;
                case '\n':
                    result.append("\\n");
                    break;
                case '\r':
                    result.append("\\r");
                    break;
                case '\t':
                    result.append("\\t");
                    break;
                default:
                    result.append(ch);
                    break;
            }
        }
        return result.toString();
    }

    private static class SnapshotRows {
        final String[] columnNames;
        final List<List<String>> rows;

        SnapshotRows(String[] columnNames, List<List<String>> rows) {
            this.columnNames = columnNames;
            this.rows = rows;
        }
    }

    private SnapshotRows parseJsonSnapshot(String text) {
        JsonParser parser = new JsonParser(text);
        return parser.parse();
    }

    private static class JsonParser {
        private final String text;
        private int idx;

        private JsonParser(String text) {
            this.text = text;
        }

        SnapshotRows parse() {
            skipWs();
            consume('{');
            skipWs();
            consumeKey("schema");
            skipWs();
            consume(':');
            skipWs();
            String[] schema = parseStringArray();
            skipWs();
            consume(',');
            skipWs();
            consumeKey("rows");
            skipWs();
            consume(':');
            skipWs();
            List<List<String>> rows = parseRows();
            skipWs();
            consume('}');
            return new SnapshotRows(schema, rows);
        }

        private List<List<String>> parseRows() {
            List<List<String>> rows = new ArrayList<>();
            consume('[');
            skipWs();
            if (peek() == ']') {
                idx++;
                return rows;
            }
            while (true) {
                skipWs();
                String[] row = parseStringArray();
                rows.add(Arrays.asList(row));
                skipWs();
                char ch = peek();
                if (ch == ',') {
                    idx++;
                    continue;
                }
                consume(']');
                return rows;
            }
        }

        private String[] parseStringArray() {
            consume('[');
            skipWs();
            List<String> values = new ArrayList<>();
            if (peek() == ']') {
                idx++;
                return new String[0];
            }
            while (true) {
                skipWs();
                if (startsWith("null")) {
                    idx += 4;
                    values.add(null);
                } else {
                    values.add(parseJsonString());
                }
                skipWs();
                char ch = peek();
                if (ch == ',') {
                    idx++;
                    continue;
                }
                consume(']');
                return values.toArray(new String[0]);
            }
        }

        private String parseJsonString() {
            consume('"');
            StringBuilder result = new StringBuilder();
            while (idx < text.length()) {
                char ch = text.charAt(idx++);
                if (ch == '\\') {
                    char next = text.charAt(idx++);
                    switch (next) {
                        case '"':
                            result.append('"');
                            break;
                        case '\\':
                            result.append('\\');
                            break;
                        case 'n':
                            result.append('\n');
                            break;
                        case 'r':
                            result.append('\r');
                            break;
                        case 't':
                            result.append('\t');
                            break;
                        default:
                            result.append(next);
                            break;
                    }
                    continue;
                }
                if (ch == '"') {
                    return result.toString();
                }
                result.append(ch);
            }
            throw new IllegalArgumentException("JSON 字符串解析失败");
        }

        private void skipWs() {
            while (idx < text.length()) {
                char ch = text.charAt(idx);
                if (ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t') {
                    idx++;
                } else {
                    return;
                }
            }
        }

        private char peek() {
            if (idx >= text.length()) {
                throw new IllegalArgumentException("意外结束");
            }
            return text.charAt(idx);
        }

        private boolean startsWith(String target) {
            return idx + target.length() <= text.length() && text.startsWith(target, idx);
        }

        private void consume(char ch) {
            if (idx >= text.length() || text.charAt(idx) != ch) {
                throw new IllegalArgumentException("预期 '" + ch + "' 在 " + idx);
            }
            idx++;
        }

        private void consumeKey(String key) {
            String wrapped = "\"" + key + "\"";
            if (!text.startsWith(wrapped, idx)) {
                throw new IllegalArgumentException("预期 key: " + key);
            }
            idx += wrapped.length();
        }

        private void consume(String token) {
            if (!startsWith(token)) {
                throw new IllegalArgumentException("预期 token: " + token);
            }
            idx += token.length();
        }
    }

    private final int preReadCacheSize = 5000;
    private List<T> preReadCache = new ArrayList<>(preReadCacheSize);

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
         * 当前预读缓存的起始索引
         */
        private int preReadStartIndex = -1;

        /**
         * 当前预读缓存的结束索引
         */
        private int preReadEndIndex = -1;

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
            // 初始化时预读数据
            if (cursor < size()) {
                preReadData();
            }
        }

        /**
         * 预读数据到缓存
         */
        private void preReadData() {
            preReadStartIndex = cursor;
            preReadEndIndex = Math.min(cursor + preReadCacheSize, size());
            List<T> batchData = databaseOpt.batchQuery(preReadStartIndex, preReadEndIndex);
            preReadCache.clear();
            preReadCache.addAll(batchData);
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
                throw new NoSuchElementException();
            lastRet = cursor;

            // 如果当前游标不在预读缓存范围内，重新预读
            if (cursor >= preReadEndIndex) {
                preReadData();
            }

            // 从预读缓存中获取数据
            return preReadCache.get(cursor++ - preReadStartIndex);
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
                throw new NoSuchElementException();
            lastRet = --cursor;

            // 如果当前游标不在预读缓存范围内，重新预读
            if (cursor < preReadStartIndex) {
                preReadData();
            }

            // 从预读缓存中获取数据
            return preReadCache.get(cursor - preReadStartIndex);
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
                throw new ConcurrentModificationException();
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
                throw new ConcurrentModificationException();
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
                throw new ConcurrentModificationException();
            }
        }
    }
}

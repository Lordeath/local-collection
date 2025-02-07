package lordeath.local.collection.db.opt.impl;

import lordeath.local.collection.db.bean.LocalColumn;
import lordeath.local.collection.db.bean.LocalColumnForMap;
import lordeath.local.collection.db.config.SqliteConfig;
import lordeath.local.collection.db.opt.inter.IDatabaseOpt;
import lordeath.local.collection.db.util.ColumnNameUtil;
import lordeath.local.collection.db.util.DBUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * SQLite数据库操作实现类
 * 通过操作SQLite数据库来实现对数据的操作，注意，这个类是线程不安全的
 * @param <T> 数据类型
 */
@Slf4j
public class SqliteOpt<T> implements IDatabaseOpt<T> {

    /**
     * 数据源
     */
    private final DataSource dataSource;
    /**
     * 操作的表名
     */
    @Getter
    private final String tableName;
    /**
     * 主键列名
     */
    private final String pkColumnName;
    /**
     * 列定义
     */
    private final List<LocalColumn> columns;
    /**
     * 元素类型
     */
    private final Class<T> clazz;

    /**
     * 使用指定的元素类型构造数据库操作对象
     * @param clazz 元素类型
     */
    public SqliteOpt(Class<T> clazz) {
        this.clazz = clazz;
        dataSource = SqliteConfig.getDataSource();
        tableName = "tmp_" + UUID.randomUUID().toString().replace("-", "");
        pkColumnName = "id" + UUID.randomUUID().toString().replace("-", "");
        log.debug("开始初始化数据源: {} {}", dataSource, tableName);
        columns = Collections.unmodifiableList(ColumnNameUtil.getFields(clazz));
        // 创建表
        // 1. 获取到表名
        // 2. 获取到列名和类型
        // 3. 创建表
        StringBuilder sql = new StringBuilder("create table ").append(tableName)
                .append(" (").append(pkColumnName).append(" INTEGER PRIMARY KEY AUTOINCREMENT");
        for (LocalColumn column : columns) {
            sql.append(", ").append(column.getColumnName()).append(" ").append(column.getDbType());
        }
        sql.append(");");
        log.debug("创建表的sql: {}", sql);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());
        log.debug("数据源初始化完毕: {} {}", dataSource, tableName);
    }

    /**
     * 使用指定的元素类型、表名和列映射构造数据库操作对象
     * @param clazz 元素类型
     * @param tableName 表名
     * @param columnsForMap 列映射定义
     */
    public SqliteOpt(Class<T> clazz, String tableName, List<LocalColumnForMap> columnsForMap) {
        this.clazz = clazz;
        this.tableName = tableName;
        this.columns = columnsForMap.stream().map(LocalColumnForMap::getSinkColumn).collect(Collectors.toList());
        dataSource = SqliteConfig.getDataSource();
        log.debug("开始初始化数据源: {} {}", dataSource, tableName);
        // 创建表
        StringBuilder sql = new StringBuilder("create table ").append(tableName)
                .append(" (");
        for (LocalColumn column : columns) {
            sql.append(column.getColumnName()).append(" ").append(column.getDbType()).append(", ");
        }
        sql.delete(sql.length() - 2, sql.length());
        sql.append(");");
        log.debug("创建表的sql: {}", sql);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());

        // 使用 columnsForMap的isKey判断是否是
        String pks = columnsForMap.stream().filter(LocalColumnForMap::isKey).map(m -> m.getSinkColumn().getColumnName()).collect(Collectors.joining(","));
        sql = new StringBuilder("create index idx_").append(StringUtils.replace(pks, ",", "_"))
                .append(" ON ").append(tableName).append("(").append(pks).append(")");
        log.debug("表创建完毕，接下来设置map的key索引: {}", sql);
        DBUtil.executeSql(dataSource, sql.toString());
        pkColumnName = null;
        log.debug("数据源初始化完毕: {} {}", dataSource, tableName);
    }

    /**
     * 添加元素到数据库
     * @param obj 元素
     * @return 添加成功与否
     */
    @Override
    public boolean add(T obj) {
        return DBUtil.add(obj, tableName, columns, dataSource);
    }

    /**
     * 批量添加元素到数据库
     * @param c 元素集合
     * @return 添加成功与否
     */
    @Override
    public boolean addAll(Collection<? extends T> c) {
        return DBUtil.addAll(c, tableName, columns, dataSource);
    }

    /**
     * 移除指定索引的元素
     * @param index 索引
     * @return 移除的元素
     */
    @Override
    public T remove(int index) {
        return DBUtil.remove(index, tableName, pkColumnName, columns, dataSource, clazz);
    }

    /**
     * 清空数据库
     */
    @Override
    public void clear() {
        // 删除表的数据
        StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(";");
        log.debug("清空表的sql: {}", sql);
        DBUtil.executeSql(dataSource, sql.toString());
    }

    /**
     * 关闭数据库连接
     */
    @Override
    public void close() {
        DBUtil.drop(tableName, dataSource);
    }

    /**
     * 获取数据库大小
     * @return 大小
     */
    @Override
    public int size() {
        return DBUtil.size(tableName, dataSource);
    }

    /**
     * 获取指定索引的元素
     *
     * @param index      索引
     * @param removeFlag
     * @return 元素
     */
    @Override
    public T get(int index, boolean removeFlag) {
        return DBUtil.get(index, tableName, columns, pkColumnName, dataSource, clazz, removeFlag);
    }

    /**
     * 设置指定索引的元素
     * @param index 索引
     * @param element 元素
     * @return 原元素
     */
    @Override
    public T set(int index, T element) {
        return DBUtil.set(index, element, tableName, columns, pkColumnName, dataSource);
    }

    /**
     * 获取指定索引的主键值
     * @param index 索引
     * @return 主键值
     */
    @Override
    public long pk(int index) {
        return DBUtil.pk(index, tableName, pkColumnName, dataSource);
    }

    /**
     * 批量查询元素
     * @param fromIndex 开始索引
     * @param toIndex 结束索引
     * @return 元素集合
     */
    @Override
    public List<T> batchQuery(int fromIndex, int toIndex) {
        return DBUtil.batchQuery(fromIndex, toIndex, tableName, columns, pkColumnName, dataSource, clazz);
    }

    /**
     * 创建分组表
     * @param newTableName 新表名
     * @param groupByColumns 分组列
     * @param whereClause 条件
     * @param keyColumn 主键列
     * @param resultColumns 结果列
     * @return 创建成功与否
     */
    @Override
    public boolean createGroupedTable(String newTableName, List<String> groupByColumns, String whereClause, String keyColumn, List<LocalColumn> resultColumns) {
        return DBUtil.createGroupedTable(dataSource, newTableName, keyColumn, resultColumns);
    }

    /**
     * 插入分组数据
     * @param sourceTableName 源表名
     * @param targetTableName 目标表名
     * @param groupByColumns 分组列
     * @param whereClause 条件
     * @param columnForMapList 列映射定义
     * @return 插入成功与否
     */
    @Override
    public boolean insertGroupedData(String sourceTableName, String targetTableName, List<String> groupByColumns, String whereClause, List<LocalColumnForMap> columnForMapList) {
        return DBUtil.insertGroupedData(dataSource, sourceTableName, targetTableName, groupByColumns, whereClause, columnForMapList);
    }

    /**
     * 根据主键获取元素
     * @param keyColumn 主键列
     * @param keyValue 主键值
     * @return 元素
     */
    @Override
    public T getByKey(String keyColumn, Object keyValue) {
        return DBUtil.getByKey(dataSource, tableName, keyColumn, keyValue, columns, clazz);
    }

    /**
     * 根据主键设置元素
     *
     * @param keyColumn 主键列
     * @param key       主键值
     * @param value     元素
     * @param removed   是否被移除，值在方法里面更新
     * @return 原元素
     */
    @Override
    public T putByKey(String keyColumn, String key, T value, AtomicBoolean removed) {
        return DBUtil.putByKey(dataSource, tableName, keyColumn, key, value, columns, removed);
    }

    /**
     * 根据主键移除元素
     * @param keyColumn 主键列
     * @param keyValue 主键值
     * @return 移除成功与否
     */
    @Override
    public boolean removeByKey(String keyColumn, Object keyValue) {
        return DBUtil.removeByKey(dataSource, tableName, keyColumn, keyValue);
    }

    /**
     * 获取所有主键值
     * @param keyColumn 主键列
     * @return 主键值集合
     */
    @Override
    public List<String> getAllKeys(String keyColumn) {
        return DBUtil.getAllKeys(dataSource, tableName, keyColumn);
    }
}

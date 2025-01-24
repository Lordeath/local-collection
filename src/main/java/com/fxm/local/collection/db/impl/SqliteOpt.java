package com.fxm.local.collection.db.impl;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.fxm.local.collection.db.bean.LocalColumnForMap;
import com.fxm.local.collection.db.config.SqliteConfig;
import com.fxm.local.collection.db.inter.IDatabaseOpt;
import com.fxm.local.collection.db.util.ColumnNameUtil;
import com.fxm.local.collection.db.util.DBUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class SqliteOpt<T> implements IDatabaseOpt<T> {

    private final DataSource dataSource;
    // 操作的表名
    @Getter
    private final String tableName;
    private final String pkColumnName;
    private final List<LocalColumn> columns;
    private final Class<T> clazz;

    public SqliteOpt(Class<T> clazz) {
        this.clazz = clazz;
        dataSource = SqliteConfig.getDataSource();
        tableName = "tmp_" + UUID.randomUUID().toString().replace("-", "");
        pkColumnName = "id" + UUID.randomUUID().toString().replace("-", "");
        log.info("开始初始化数据源: {} {}", dataSource, tableName);
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
        log.info("创建表的sql: {}", sql);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());
        log.info("数据源初始化完毕: {} {}", dataSource, tableName);
    }

    public SqliteOpt(Class<T> clazz, String tableName, List<LocalColumnForMap> columnsForMap) {
        this.clazz = clazz;
        this.tableName = tableName;
        this.columns = columnsForMap.stream().map(LocalColumnForMap::getSinkColumn).collect(Collectors.toList());
        dataSource = SqliteConfig.getDataSource();
        log.info("开始初始化数据源: {} {}", dataSource, tableName);
        // 创建表
        StringBuilder sql = new StringBuilder("create table ").append(tableName)
                .append(" (");
        for (LocalColumn column : columns) {
            sql.append(column.getColumnName()).append(" ").append(column.getDbType()).append(", ");
        }
        sql.delete(sql.length() - 2, sql.length());
        sql.append(");");
        log.info("创建表的sql: {}", sql);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());

        // 使用 columnsForMap的isKey判断是否是
        String pks = columnsForMap.stream().filter(LocalColumnForMap::isKey).map(m -> m.getSinkColumn().getColumnName()).collect(Collectors.joining(","));
        sql = new StringBuilder("create index idx_").append(StringUtils.replace(pks, ",", "_"))
                .append(" ON ").append(tableName).append("(").append(pks).append(")");
        log.info("表创建完毕，接下来设置map的key索引: {}", sql);
        DBUtil.executeSql(dataSource, sql.toString());
        pkColumnName = null;
        log.info("数据源初始化完毕: {} {}", dataSource, tableName);
    }

    @Override
    public boolean add(T obj) {
        return DBUtil.add(obj, tableName, columns, dataSource);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return DBUtil.addAll(c, tableName, columns, dataSource);
    }

    @Override
    public T remove(int index) {
        return DBUtil.remove(index, tableName, pkColumnName, columns, dataSource, clazz);
    }


    @Override
    public void clear() {
        // 删除表的数据
        StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(";");
        log.info("清空表的sql: {}", sql);
        DBUtil.executeSql(dataSource, sql.toString());
    }


    @Override
    public void close() {
        DBUtil.extracted(tableName, dataSource);
    }

    @Override
    public int size() {
        return DBUtil.size(tableName, dataSource);
    }


    @Override
    public T get(int index) {
        return DBUtil.get(index, tableName, columns, pkColumnName, dataSource, clazz);
    }


    @Override
    public T set(int index, T element) {
        return DBUtil.set(index, element, tableName, columns, pkColumnName, dataSource);
    }

    @Override
    public long pk(int index) {
        return DBUtil.pk(index, tableName, pkColumnName, dataSource);
    }

    @Override
    public List<T> batchQuery(int fromIndex, int toIndex) {
        return DBUtil.batchQuery(fromIndex, toIndex, tableName, columns, pkColumnName, dataSource, clazz);
    }


    @Override
    public boolean createGroupedTable(String newTableName, List<String> groupByColumns, String whereClause, String keyColumn, List<LocalColumn> resultColumns) {
        return DBUtil.createGroupedTable(dataSource, newTableName, keyColumn, resultColumns);
    }

    @Override
    public boolean insertGroupedData(String sourceTableName, String targetTableName, List<String> groupByColumns, String whereClause, List<LocalColumnForMap> columnForMapList) {
        return DBUtil.insertGroupedData(dataSource, sourceTableName, targetTableName, groupByColumns, whereClause, columnForMapList);
    }

    @Override
    public T getByKey(String keyColumn, Object keyValue) {
        return DBUtil.getByKey(dataSource, tableName, keyColumn, keyValue, columns, clazz);
    }

    @Override
    public T putByKey(String keyColumn, String key, T value) {
        return DBUtil.putByKey(dataSource, tableName, keyColumn, key, value, columns, clazz);
    }

    @Override
    public boolean removeByKey(String keyColumn, Object keyValue) {
        return DBUtil.removeByKey(dataSource, tableName, keyColumn, keyValue);
    }

    @Override
    public List<String> getAllKeys(String keyColumn) {
        return DBUtil.getAllKeys(dataSource, tableName, keyColumn);
    }

}

package com.fxm.local.collection.db.impl;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.fxm.local.collection.db.config.H2Config;
import com.fxm.local.collection.db.config.HSQLDBConfig;
import com.fxm.local.collection.db.inter.IDatabaseOpt;
import com.fxm.local.collection.db.util.ColumnNameUtil;
import com.fxm.local.collection.db.util.DBUtil;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * 通过操作H2来实现对数据的操作，注意，这个类是线程不安全的
 *
 * @param <T>
 */
@Slf4j
public class HSQLDBOpt<T> implements IDatabaseOpt<T> {

    private final DataSource dataSource;
    // 操作的表名
    private final String tableName;
    private final String pkColumnName;
    private final List<LocalColumn> columns;
    private final Class<T> clazz;

    public HSQLDBOpt(Class<T> clazz) {
        this.clazz = clazz;
        dataSource = HSQLDBConfig.getDataSource();
        tableName = "tmp_" + UUID.randomUUID().toString().replace("-", "");
        pkColumnName = "id" + UUID.randomUUID().toString().replace("-", "");
        log.info("开始初始化数据源: {} {}", dataSource, tableName);
        columns = Collections.unmodifiableList(ColumnNameUtil.getFields(clazz));
        // 创建表
        // 1. 获取到表名
        // 2. 获取到列名和类型
        // 3. 创建表
        StringBuilder sql = new StringBuilder("create table ").append(tableName)
                .append(" (").append(pkColumnName).append(" INTEGER PRIMARY KEY IDENTITY");
        for (LocalColumn column : columns) {
            sql.append(", ").append(column.getColumnName()).append(" ").append(column.getDbType());
        }
        sql.append(");");
        log.info("创建表的sql: {}", sql);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());
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
        DBUtil.clear(tableName, dataSource);
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

}
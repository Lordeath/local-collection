package com.fxm.local.collection.db.impl;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.fxm.local.collection.db.config.SqliteConfig;
import com.fxm.local.collection.db.inter.IDatabaseOpt;
import com.fxm.local.collection.db.util.ColumnNameUtil;
import com.fxm.local.collection.db.util.DBUtil;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Slf4j
public class SqliteOpt<T> implements IDatabaseOpt<T> {

    private final DataSource dataSource;
    // 操作的表名
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
        // TODO 创建表
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
//        // 查出原有的数据的id，然后进行更新
//        StringBuilder sql = new StringBuilder("select ").append(pkColumnName).append(" from ")
//                .append(tableName).append(" order by ").append(pkColumnName).append(" limit ").append(index).append(",1;");
//        log.info("查询数据的id的sql: {}", sql);
//        Long id = DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn(pkColumnName, Long.class, "BIGINT", null)), Long.class);
//        if (id == null) {
//            throw new RuntimeException("没有找到对应的数据");
//        }
//        return id;
        return DBUtil.pk(index, tableName, pkColumnName, dataSource);
    }

}

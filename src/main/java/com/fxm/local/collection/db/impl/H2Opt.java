package com.fxm.local.collection.db.impl;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.fxm.local.collection.db.config.H2Config;
import com.fxm.local.collection.db.inter.IDatabaseOpt;
import com.fxm.local.collection.db.util.ColumnNameUtil;
import com.fxm.local.collection.db.util.DBUtil;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Slf4j
public class H2Opt<T> implements IDatabaseOpt<T> {

    private final DataSource dataSource;
    //    private final Connection connection;
    // 操作的表名
    private final String tableName;
    private final String pkColumnName;
    private final List<LocalColumn> columns;
    private final Class<T> clazz;

    public H2Opt(Class<T> clazz) {
        this.clazz = clazz;
        dataSource = H2Config.getDataSource();
        tableName = "tmp_" + UUID.randomUUID().toString().replace("-", "");
        pkColumnName = "id" + UUID.randomUUID().toString().replace("-", "");
        log.info("开始初始化数据源: {} {}", dataSource, tableName);
        columns = Collections.unmodifiableList(ColumnNameUtil.getFields(clazz));
        // TODO 创建表
        // 1. 获取到表名
        // 2. 获取到列名和类型
        // 3. 创建表
        StringBuilder sql = new StringBuilder("create table ").append(tableName)
                .append(" (").append(pkColumnName).append(" BIGINT AUTO_INCREMENT PRIMARY KEY");
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
        // 通过表名和列名,拼接sql
        StringBuilder sql = new StringBuilder("insert into ").append(tableName).append(" (");
        for (LocalColumn column : columns) {
            sql.append(column.getColumnName()).append(", ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(") values (");
        for (LocalColumn column : columns) {
            if (column.getField() == null && columns.size() == 1) {
                sql.append("'").append(obj).append("'").append(", ");
            } else {
                Object value = null;
                column.getField().setAccessible(true);
                try {
                    value = column.getField().get(obj);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                sql.append("'").append(value).append("'").append(", ");
            }
        }
        sql.setLength(sql.length() - 2);
        sql.append(");");
        log.info("添加数据的sql: {}", sql);
        // 执行sql
        return DBUtil.executeSql(dataSource, sql.toString());
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return false;
    }

    @Override
    public void remove(T obj) {
        System.out.println("H2Opt remove");
    }

    @Override
    public T remove(int index) {
        return null;
    }

    @Override
    public void update(T obj) {
        System.out.println("H2Opt update");
    }

    @Override
    public void query(T obj) {
        System.out.println("H2Opt query");
    }

    @Override
    public void queryAll() {
        System.out.println("H2Opt queryAll");
    }

    @Override
    public void clear() {
        System.out.println("H2Opt clear");
    }

    @Override
    public void close() {
        // 删除表的数据
        StringBuilder sql = new StringBuilder("drop table ").append(tableName).append(";");
        log.info("删除表的sql: {}", sql);
        DBUtil.executeSql(dataSource, sql.toString());
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public T get(int index) {
        // 通过主键进行排序，然后查询，limit offset 1
        // 拼接sql
        StringBuilder sql = new StringBuilder("select * from ")
                .append(tableName).append(" order by ").append(pkColumnName).append(" limit 1 offset ").append(index).append(";");
        log.info("查询数据的sql: {}", sql);
        return DBUtil.querySingle(dataSource, sql.toString(), columns, clazz);
    }

    @Override
    public T set(int index, T element) {
        return null;
    }
}

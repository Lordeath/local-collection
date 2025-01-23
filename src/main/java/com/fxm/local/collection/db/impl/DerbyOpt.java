package com.fxm.local.collection.db.impl;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.fxm.local.collection.db.config.DerbyConfig;
import com.fxm.local.collection.db.config.HSQLDBConfig;
import com.fxm.local.collection.db.inter.IDatabaseOpt;
import com.fxm.local.collection.db.util.ColumnNameUtil;
import com.fxm.local.collection.db.util.DBUtil;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.*;

/**
 * 通过操作H2来实现对数据的操作，注意，这个类是线程不安全的
 *
 * @param <T>
 */
@Slf4j
public class DerbyOpt<T> implements IDatabaseOpt<T> {

    private final DataSource dataSource;
    // 操作的表名
    private final String tableName;
    private final String pkColumnName;
    private final List<LocalColumn> columns;
    private final Class<T> clazz;

    public DerbyOpt(Class<T> clazz) {
        this.clazz = clazz;
        dataSource = DerbyConfig.getDataSource();
        tableName = "tmp_" + UUID.randomUUID().toString().replace("-", "");
        pkColumnName = "id" + UUID.randomUUID().toString().replace("-", "");
        log.info("开始初始化数据源: {} {}", dataSource, tableName);
        columns = Collections.unmodifiableList(ColumnNameUtil.getFields(clazz));
        // 创建表
        // 1. 获取到表名
        // 2. 获取到列名和类型
        // 3. 创建表
        StringBuilder sql = new StringBuilder("create table ").append(tableName).append("\n")
                .append(" (").append(pkColumnName).append("\n")
                .append(" INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY").append("\n");
        for (LocalColumn column : columns) {
            sql.append(", ").append(column.getColumnName()).append(" ").append(column.getDbType());
            if (column.getDbType().equalsIgnoreCase("varchar")) {
                sql.append("(").append(4000).append(")");
            }
        }
        sql.append(")");
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
        // 通过index找到id，然后通过id进行删除
        Long id = pk(index);
        // 拼接sql
        StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(" where ").append(pkColumnName).append(" = ").append(id);
        log.info("删除数据的sql: {}", sql);
        T t = get(index);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());
        return t;
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
//        return DBUtil.get(index, tableName, columns, pkColumnName, dataSource, clazz);
        // 通过主键进行排序，然后查询，limit offset 1
        // 拼接sql
        StringBuilder sql = new StringBuilder("select * from ")
                .append(tableName).append(" order by ").append(pkColumnName).append(" OFFSET ").append(index).append(" ROWS FETCH NEXT 1 ROWS ONLY");
        log.info("查询数据的sql: {}", sql);
        return DBUtil.querySingle(dataSource, sql.toString(), columns, clazz);
    }


    @Override
    public T set(int index, T element) {
//        return DBUtil.set(index, element, tableName, columns, pkColumnName, dataSource);
        // 查出原有的数据的id，然后进行更新
//        Long id = pk(index, tableName, pkColumnName, dataSource);
        Long id = pk(index);
        // 拼接sql
        StringBuilder   sql = new StringBuilder("update ").append(tableName).append(" set ");
        for (LocalColumn column : columns) {
            if (column.getField() == null && columns.size() == 1) {
                sql.append(column.getColumnName()).append(" = '").append(element).append("', ");
            } else {
                Object value;
                column.getField().setAccessible(true);
                try {
                    value = column.getField().get(element);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                if (Objects.isNull(value) || value instanceof Number) {
                    sql.append(column.getColumnName()).append(" = ").append(value).append(", ");
                } else {
                    sql.append(column.getColumnName()).append(" = '").append(value).append("', ");
                }
            }
        }
        sql.setLength(sql.length() - 2);
        sql.append(" where ").append(pkColumnName).append(" = ").append(id);
        log.info("更新数据的sql: {}", sql);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());
        return element;
    }

    @Override
    public long pk(int index) {
//        return DBUtil.pk(index, tableName, pkColumnName, dataSource);
        StringBuilder sql = new StringBuilder("select ").append(pkColumnName).append(" from ")
                .append(tableName).append(" order by ").append(pkColumnName).append(" OFFSET ").append(index).append(" ROWS FETCH NEXT 1 ROWS ONLY");
        log.info("查询数据的id的sql: {}", sql);
        Long id = DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn(pkColumnName, Long.class, "BIGINT", null)), Long.class);
        if (id == null) {
            throw new RuntimeException("没有找到对应的数据");
        }
        return id;
    }

}

package com.fxm.local.collection.db.impl;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.fxm.local.collection.db.config.H2Config;
import com.fxm.local.collection.db.inter.IDatabaseOpt;
import com.fxm.local.collection.db.util.ColumnNameUtil;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Slf4j
public class H2Opt<T> implements IDatabaseOpt<T> {

    private final DataSource dataSource;
    // 操作的表名
    private final String tableName;
    private final List<LocalColumn> columns;

    public H2Opt(Class<T> clazz) {
        dataSource = H2Config.getDataSource();
        tableName = UUID.randomUUID().toString().replace("-", "");
        columns = Collections.unmodifiableList(ColumnNameUtil.getFields(clazz));
        // TODO 创建表

        log.info("数据源初始化完毕: {} {}", dataSource, tableName);
    }


    @Override
    public boolean add(T obj) {
//        if (columns.isEmpty()) {
//            // 使用Object的类型，获取到字段，从而创建表
//            columns = ColumnNameUtil.getFields(obj.getClass());
//        }
        return false;
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
        System.out.println("H2Opt close");
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public T get(int index) {
        return null;
    }

    @Override
    public T set(int index, T element) {
        return null;
    }
}

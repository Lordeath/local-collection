package lordeath.local.collection.db.opt.impl;

import lordeath.local.collection.db.bean.LocalColumn;
import lordeath.local.collection.db.bean.LocalColumnForMap;
import lordeath.local.collection.db.config.H2Config;
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
import java.util.stream.Collectors;

/**
 * 通过操作H2来实现对数据的操作，注意，这个类是线程不安全的
 *
 * @param <T> 元素类型
 */
@Slf4j
public class H2Opt<T> implements IDatabaseOpt<T> {

    private final DataSource dataSource;
    // 操作的表名
    @Getter
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
        // 创建表
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

    public H2Opt(Class<T> clazz, String tableName, List<LocalColumnForMap> columnsForMap) {
        this.clazz = clazz;
        this.tableName = tableName;
        this.columns = columnsForMap.stream().map(LocalColumnForMap::getSinkColumn).collect(Collectors.toList());
        dataSource = H2Config.getDataSource();
        log.info("开始初始化数据源: {} {}", dataSource, tableName);
        // 创建表
        // 1. 获取到表名
        // 2. 获取到列名和类型
        // 3. 创建表
        StringBuilder sql = new StringBuilder("create table ").append(tableName)
                .append(" (");
        for (LocalColumnForMap localColumnForMap : columnsForMap) {
            LocalColumn column = localColumnForMap.getSinkColumn();
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

    @Override
    public List<T> batchQuery(int fromIndex, int toIndex) {
        return DBUtil.batchQuery(fromIndex, toIndex, tableName, columns, pkColumnName, dataSource, clazz);
    }

    @Override
    public boolean createGroupedTable(String newTableName, List<String> groupByColumns, String whereClause, String keyColumn, List<LocalColumn> resultColumns) {
        return DBUtil.createGroupedTable(dataSource, newTableName, keyColumn, resultColumns);
    }

    @Override
    public boolean insertGroupedData(String sourceTableName, String targetTableName, List<String> groupByColumns, String whereClause,
                                     List<LocalColumnForMap> columnForMapList) {
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

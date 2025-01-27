package lordeath.local.collection.db.opt.impl;

import lordeath.local.collection.db.bean.LocalColumn;
import lordeath.local.collection.db.bean.LocalColumnForMap;
import lordeath.local.collection.db.config.DerbyConfig;
import lordeath.local.collection.db.opt.inter.IDatabaseOpt;
import lordeath.local.collection.db.util.ColumnNameUtil;
import lordeath.local.collection.db.util.DBUtil;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Derby数据库操作实现类
 * 
 * 通过操作Derby数据库来实现对数据的操作，注意，这个类是线程不安全的
 * 
 * @param <T> 操作的数据类型
 */
@Slf4j
public class DerbyOpt<T> implements IDatabaseOpt<T> {

    private final DataSource dataSource;
    // 操作的表名
    @Getter
    private final String tableName;
    private final String pkColumnName;
    private final List<LocalColumn> columns;
    private final Class<T> clazz;

    /**
     * 使用指定的元素类型构造数据库操作对象
     * 
     * @param clazz 元素类型
     */
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

    /**
     * 使用指定的元素类型、表名和列映射构造数据库操作对象
     * 
     * @param clazz 元素类型
     * @param tableName 表名
     * @param columnsForMap 列映射定义
     */
    public DerbyOpt(Class<T> clazz, String tableName, List<LocalColumnForMap> columnsForMap) {
        this.clazz = clazz;
        this.tableName = tableName;
        this.columns = columnsForMap.stream().map(LocalColumnForMap::getSinkColumn).collect(Collectors.toList());
        dataSource = DerbyConfig.getDataSource();
        log.info("开始初始化数据源: {} {}", dataSource, tableName);
        // 创建表
        StringBuilder sql = new StringBuilder("create table ").append(tableName)
                .append(" (");
        for (LocalColumn column : columns) {
            if (column.getDbType().equalsIgnoreCase("varchar")) {
                sql.append(column.getColumnName()).append(" ").append(column.getDbType()).append("(").append(4000).append(")").append(", ");
            } else {
                sql.append(column.getColumnName()).append(" ").append(column.getDbType()).append(", ");
            }
        }
        sql.delete(sql.length() - 2, sql.length());
        sql.append(")");
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

    /**
     * 添加元素到数据库
     * 
     * @param obj 元素
     * @return 添加成功与否
     */
    @Override
    public boolean add(T obj) {
        return DBUtil.add(obj, tableName, columns, dataSource);
    }

    /**
     * 批量添加元素到数据库
     * 
     * @param c 元素集合
     * @return 添加成功与否
     */
    @Override
    public boolean addAll(Collection<? extends T> c) {
        return DBUtil.addAll(c, tableName, columns, dataSource);
    }

    /**
     * 移除指定索引的元素
     * 
     * @param index 索引
     * @return 移除的元素
     */
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

    /**
     * 清空数据库
     */
    @Override
    public void clear() {
        DBUtil.clear(tableName, dataSource);
    }

    /**
     * 关闭数据库连接
     */
    @Override
    public void close() {
        DBUtil.extracted(tableName, dataSource);
    }

    /**
     * 获取数据库大小
     * 
     * @return 大小
     */
    @Override
    public int size() {
        return DBUtil.size(tableName, dataSource);
    }

    /**
     * 获取指定索引的元素
     * 
     * @param index 索引
     * @return 元素
     */
    @Override
    public T get(int index) {
        // 通过主键进行排序，然后查询，limit offset 1
        // 拼接sql
        StringBuilder sql = new StringBuilder("select * from ")
                .append(tableName).append(" order by ").append(pkColumnName).append(" OFFSET ").append(index).append(" ROWS FETCH NEXT 1 ROWS ONLY");
        log.info("查询数据的sql: {}", sql);
        return DBUtil.querySingle(dataSource, sql.toString(), columns, clazz);
    }

    /**
     * 设置指定索引的元素
     * 
     * @param index 索引
     * @param element 元素
     * @return 元素
     */
    @Override
    public T set(int index, T element) {
        // 查出原有的数据的id，然后进行更新
        Long id = pk(index);
        // 拼接sql
        StringBuilder sql = new StringBuilder("update ").append(tableName).append(" set ");
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

    /**
     * 获取指定索引的主键
     * 
     * @param index 索引
     * @return 主键
     */
    @Override
    public long pk(int index) {
        StringBuilder sql = new StringBuilder("select ").append(pkColumnName).append(" from ")
                .append(tableName).append(" order by ").append(pkColumnName).append(" OFFSET ").append(index).append(" ROWS FETCH NEXT 1 ROWS ONLY");
        log.info("查询数据的id的sql: {}", sql);
        Long id = DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn(pkColumnName, Long.class, "BIGINT", null)), Long.class);
        if (id == null) {
            throw new RuntimeException("没有找到对应的数据");
        }
        return id;
    }

    /**
     * 批量查询数据
     * 
     * @param fromIndex 开始索引
     * @param toIndex 结束索引
     * @return 数据集合
     */
    @Override
    public List<T> batchQuery(int fromIndex, int toIndex) {
        // 通过主键进行排序，然后使用 LIMIT 和 OFFSET 进行批量查询
        StringBuilder sql = new StringBuilder("select * from ")
                .append(tableName)
                .append(" order by ")
                .append(pkColumnName)
                .append(" OFFSET ").append(fromIndex)
                .append(" ROWS FETCH NEXT ").append(toIndex - fromIndex).append(" ROWS ONLY");

        log.info("批量查询数据的sql: {}", sql);

        List<T> result = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql.toString())) {

            while (resultSet.next()) {
                T obj = clazz.getDeclaredConstructor().newInstance();
                for (LocalColumn column : columns) {
                    if (column.getField() != null) {
                        column.getField().setAccessible(true);
                        Object value = resultSet.getObject(column.getColumnName());
                        if (value != null) {
                            column.getField().set(obj, value);
                        }
                    }
                }
                result.add(obj);
            }
        } catch (Exception e) {
            throw new RuntimeException("批量查询数据失败", e);
        }
        return result;
    }

    /**
     * 创建分组表
     * 
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
     * 
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
     * 
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
     * @param key 主键值
     * @param value 元素
     * @return 元素
     */
    @Override
    public T putByKey(String keyColumn, String key, T value) {
        return DBUtil.putByKey(dataSource, tableName, keyColumn, key, value, columns, clazz);
    }

    /**
     * 根据主键移除元素
     * 
     * @param keyColumn 主键列
     * @param keyValue 主键值
     * @return 移除成功与否
     */
    @Override
    public boolean removeByKey(String keyColumn, Object keyValue) {
        return DBUtil.removeByKey(dataSource, tableName, keyColumn, keyValue);
    }

    /**
     * 获取所有主键
     * 
     * @param keyColumn 主键列
     * @return 主键集合
     */
    @Override
    public List<String> getAllKeys(String keyColumn) {
        return DBUtil.getAllKeys(dataSource, tableName, keyColumn);
    }
}

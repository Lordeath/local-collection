package com.fxm.local.collection.db.util;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.fxm.local.collection.db.bean.LocalColumnForMap;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DBUtil {
    public static <T> boolean add(T obj, String tableName, List<LocalColumn> columns, DataSource dataSource) {
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
                Object value;
                column.getField().setAccessible(true);
                try {
                    value = column.getField().get(obj);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                if (Objects.isNull(value) || value instanceof Number) {
                    sql.append(value).append(", ");
                } else {
                    sql.append("'").append(value).append("'").append(", ");
                }
            }
        }
        sql.setLength(sql.length() - 2);
        sql.append(")");
        log.info("添加数据的sql: {}", sql);
        // 执行sql
        return DBUtil.executeSql(dataSource, sql.toString());
    }

    public static <T> boolean addAll(Collection<? extends T> c, String tableName, List<LocalColumn> columns, DataSource dataSource) {
        // 使用批量插入
        StringBuilder sql = new StringBuilder("insert into ").append(tableName).append(" (");
        for (LocalColumn column : columns) {
            sql.append(column.getColumnName()).append(", ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(") values ");
        for (T t : c) {
            sql.append("(");
            for (LocalColumn column : columns) {
                if (column.getField() == null) {
                    sql.append("'").append(t).append("'").append(", ");
                } else {
                    Object value;
                    column.getField().setAccessible(true);
                    try {
                        value = column.getField().get(t);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    sql.append("'").append(value).append("'").append(", ");
                }
            }
            sql.setLength(sql.length() - 2);
            sql.append("), ");
        }
        sql.setLength(sql.length() - 2);
        log.info("批量添加数据的sql: {}", sql);
        // 执行sql
        return DBUtil.executeSql(dataSource, sql.toString());
    }

    public static <T> T remove(int index, String tableName, String pkColumnName, List<LocalColumn> columns, DataSource dataSource, Class<T> clazz) {
        // 通过index找到id，然后通过id进行删除
        Long id = pk(index, tableName, pkColumnName, dataSource);
        // 拼接sql
        StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(" where ").append(pkColumnName).append(" = ").append(id);
        log.info("删除数据的sql: {}", sql);
        T t = get(index, tableName, columns, pkColumnName, dataSource, clazz);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());
        return t;
    }

    public static <T> T get(int index, String tableName, List<LocalColumn> columns, String pkColumnName, DataSource dataSource, Class<T> clazz) {
        // 通过主键进行排序，然后查询，limit offset 1
        // 拼接sql
        StringBuilder sql = new StringBuilder("select * from ")
                .append(tableName).append(" order by ").append(pkColumnName).append(" limit 1 offset ").append(index);
        log.info("查询数据的sql: {}", sql);
        return DBUtil.querySingle(dataSource, sql.toString(), columns, clazz);
    }

    public static <T> List<T> batchQuery(int fromIndex, int toIndex, String tableName, List<LocalColumn> columns,
                                         String pkColumnName, DataSource dataSource, Class<T> clazz) {
        // 通过主键进行排序，然后使用 LIMIT 和 OFFSET 进行批量查询
        StringBuilder sql = new StringBuilder("select * from ")
                .append(tableName)
                .append(" order by ")
                .append(pkColumnName)
                .append(" limit ")
                .append(toIndex - fromIndex)
                .append(" offset ")
                .append(fromIndex);

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

    public static boolean executeSql(DataSource dataSource, String sql) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T querySingle(DataSource dataSource, String sql, List<LocalColumn> columns, Class<T> clazz) {
        // 通过数据源进行查询，返回一个对象
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            if (columns.size() == 1 && columns.get(0).getField() == null) {
                // 说明是简单数据，直接返回
                if (resultSet.next()) {
                    if (clazz == String.class) {
                        return (T) resultSet.getString(columns.get(0).getColumnName());
                    } else if (clazz == Integer.class) {
                        return (T) Integer.valueOf(resultSet.getInt(columns.get(0).getColumnName()));
                    } else if (clazz == Long.class) {
                        return (T) Long.valueOf(resultSet.getLong(columns.get(0).getColumnName()));
                    } else if (clazz == Double.class) {
                        return (T) Double.valueOf(resultSet.getDouble(columns.get(0).getColumnName()));
                    } else if (clazz == Float.class) {
                        return (T) Float.valueOf(resultSet.getFloat(columns.get(0).getColumnName()));
                    } else if (clazz == Boolean.class) {
                        return (T) Boolean.valueOf(resultSet.getBoolean(columns.get(0).getColumnName()));
                    } else if (clazz == Character.class) {
                        return (T) Character.valueOf(resultSet.getString(columns.get(0).getColumnName()).charAt(0));
                    } else {
                        throw new RuntimeException("不支持的类型: " + clazz);
                    }
                }
                return null;
            } else {
                if (resultSet.next()) {
                    T t = clazz.newInstance();
                    // 通过sql的返回结果，使用反射来填充这些字段
                    for (LocalColumn column : columns) {
                        Field field = column.getField();
                        field.setAccessible(true);
                        if (column.getColumnType() == String.class) {
                            field.set(t, resultSet.getString(column.getColumnName()));
                        } else if (column.getColumnType() == Integer.class) {
                            field.set(t, resultSet.getInt(column.getColumnName()));
                        } else if (column.getColumnType() == Long.class) {
                            field.set(t, resultSet.getLong(column.getColumnName()));
                        } else if (column.getColumnType() == Double.class) {
                            field.set(t, resultSet.getDouble(column.getColumnName()));
                        } else if (column.getColumnType() == Float.class) {
                            field.set(t, resultSet.getFloat(column.getColumnName()));
                        } else if (column.getColumnType() == Boolean.class) {
                            field.set(t, resultSet.getBoolean(column.getColumnName()));
                        } else if (column.getColumnType() == Character.class) {
                            field.set(t, resultSet.getString(column.getColumnName()).charAt(0));
                        } else {
                            throw new RuntimeException("不支持的类型: " + column.getColumnType());
                        }
                    }
                    return t;
                }
                return null;
            }

        } catch (SQLException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static Integer size(String tableName, DataSource dataSource) {
        // 查询表的数据量
        StringBuilder sql = new StringBuilder("select count(*) AS count from ").append(tableName);
        log.info("查询数据量的sql: {}", sql);
        return DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn("count", Integer.class, "INT", null)), Integer.class);
    }

    public static void extracted(String tableName, DataSource dataSource) {
        // 删除表的数据
        StringBuilder sql = new StringBuilder("drop table ").append(tableName);
        log.info("删除表的sql: {}", sql);
        DBUtil.executeSql(dataSource, sql.toString());
    }

    public static void clear(String tableName, DataSource dataSource) {
        // 删除表的数据
        StringBuilder sql = new StringBuilder("truncate table ").append(tableName);
        log.info("清空表的sql: {}", sql);
        DBUtil.executeSql(dataSource, sql.toString());
    }

    public static <T> T set(int index, T element, String tableName, List<LocalColumn> columns, String pkColumnName, DataSource dataSource) {
        // 查出原有的数据的id，然后进行更新
        Long id = pk(index, tableName, pkColumnName, dataSource);

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

    public static long pk(int index, String tableName, String pkColumnName, DataSource dataSource) {
        // 查出原有的数据的id，然后进行更新
        StringBuilder sql = new StringBuilder("select ").append(pkColumnName).append(" from ")
                .append(tableName).append(" order by ").append(pkColumnName).append(" limit 1 offset ").append(index);
        log.info("查询数据的id的sql: {}", sql);
        Long id = DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn(pkColumnName, Long.class, "BIGINT", null)), Long.class);
        if (id == null) {
            throw new RuntimeException("没有找到对应的数据");
        }
        return id;
    }

    private static void setParameters(PreparedStatement stmt, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            stmt.setObject(i + 1, params[i]);
        }
    }

    private static <T> T createInstance(ResultSet rs, List<LocalColumn> columns, Class<T> clazz)
            throws Exception {
        T obj = clazz.getDeclaredConstructor().newInstance();
        for (LocalColumn column : columns) {
            if (column.getField() != null) {
                column.getField().setAccessible(true);
                Object value = rs.getObject(column.getColumnName());
                if (value != null) {
                    column.getField().set(obj, value);
                }
            }
        }
        return obj;
    }

    public static boolean createGroupedTable(DataSource dataSource, String tableName,
                                             String keyColumn, List<LocalColumn> resultColumns) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE ").append(tableName).append(" (");

        // 添加key列定义
        createTableSql.append(keyColumn).append(" VARCHAR PRIMARY KEY");

        // 添加其他列定义
        for (LocalColumn column : resultColumns) {
            if (column.getColumnName().equals(keyColumn)) {
                continue;
            }
            String sqlType = getSqlType(column.getField().getType());
            createTableSql.append(", ")
                    .append(column.getColumnName())
                    .append(" ")
                    .append(sqlType);
        }

        createTableSql.append(")");

        log.debug("Creating table with SQL: {}", createTableSql);

        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            // 创建表
            stmt.execute(createTableSql.toString());

            // 创建索引
            String createIndexSql = String.format(
                    "CREATE INDEX idx_%s ON %s(%s)",
                    keyColumn, tableName, keyColumn
            );
            stmt.execute(createIndexSql);

            return true;
        } catch (Exception e) {
            log.error("Failed to create table: {}\ncreateTableSql: \n{}", createTableSql, e.getMessage());
            throw new RuntimeException("Failed to create table", e);
        }
    }

    public static String getSqlType(Class<?> javaType) {
        if (String.class.equals(javaType)) {
            return "VARCHAR";
        } else if (Integer.class.equals(javaType) || int.class.equals(javaType)) {
            return "INTEGER";
        } else if (Long.class.equals(javaType) || long.class.equals(javaType)) {
            return "BIGINT";
        } else if (Double.class.equals(javaType) || double.class.equals(javaType)) {
            return "DOUBLE";
        } else if (Float.class.equals(javaType) || float.class.equals(javaType)) {
            return "FLOAT";
        } else if (Boolean.class.equals(javaType) || boolean.class.equals(javaType)) {
            return "BOOLEAN";
        } else if (Date.class.equals(javaType)) {
            return "TIMESTAMP";
        }
//        return "VARCHAR(255)"; // 默认类型
        throw new UnsupportedOperationException("不支持的数据类型，请联系开发: " + javaType);
    }

    public static boolean insertGroupedData(DataSource dataSource, String sourceTableName,
                                            String targetTableName, List<String> groupByColumns,
                                            String whereClause,
                                            List<LocalColumnForMap> columnForMapList) {
        // 构建INSERT语句
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(targetTableName).append(" (")
        ;

        for (LocalColumnForMap localColumnForMap : columnForMapList) {
            insertSql.append(localColumnForMap.getSinkColumn().getColumnName()).append(", ");
        }
        insertSql.setLength(insertSql.length() - 2);

        insertSql.append(") SELECT ");

        // 构建SELECT子句
        for (LocalColumnForMap localColumnForMap : columnForMapList) {
            insertSql.append(localColumnForMap.getExpression()).append(", ");
        }
        insertSql.setLength(insertSql.length() - 2);
        insertSql.append(" FROM ").append(sourceTableName);
        if (whereClause != null && !whereClause.isEmpty()) {
            insertSql.append(" WHERE ").append(whereClause);
        }

        if (!groupByColumns.isEmpty()) {
            insertSql.append(" GROUP BY ").append(String.join(", ", groupByColumns));
        }

        log.debug("Inserting data with SQL: {}", insertSql);

        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            return stmt.executeUpdate(insertSql.toString()) > 0;
        } catch (Exception e) {
            log.error("Failed to insert data: {}", e.getMessage());
            throw new RuntimeException("Failed to insert grouped data", e);
        }
    }

    public static <T> T getByKey(DataSource dataSource, String tableName, String keyColumn,
                                 Object keyValue, List<LocalColumn> columns, Class<T> clazz) {
        String sql = String.format("SELECT * FROM %s WHERE %s = ?", tableName, keyColumn);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {

            setParameters(stmt, keyValue);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return createInstance(rs, columns, clazz);
                }
            }
        } catch (Exception e) {
            log.error("Failed to get object by key: {}", e.getMessage());
            throw new RuntimeException("Failed to get object by key", e);
        }
        return null;
    }


    public static <K, V> V putByKey(DataSource dataSource, String tableName, String keyColumn, K key, V value, List<LocalColumn> columns, Class<V> clazz) {
        V v = getByKey(dataSource, tableName, keyColumn, key, columns, clazz);
        if (v != null) {
            // update
            removeByKey(dataSource, tableName, keyColumn, key);
        }
        add(value, tableName, columns, dataSource);
        return value;
    }

    public static boolean removeByKey(DataSource dataSource, String tableName, String keyColumn, Object keyValue) {
        String sql = String.format("DELETE FROM %s WHERE %s = ?", tableName, keyColumn);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {

            setParameters(stmt, keyValue);
            return stmt.executeUpdate() > 0;

        } catch (Exception e) {
            throw new RuntimeException("Failed to remove object by key", e);
        }
    }

    public static List<String> getAllKeys(DataSource dataSource, String tableName, String keyColumn) {
        String sql = String.format("SELECT DISTINCT %s FROM %s", keyColumn, tableName);
        List<String> keys = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                keys.add(rs.getString(1));
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to get all keys", e);
        }

        return keys;
    }

    public static boolean createIndex(DataSource dataSource, String tableName, String indexName, String columnName) {
        String sql = String.format("CREATE INDEX %s ON %s(%s)", indexName, tableName, columnName);

        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            return true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create index", e);
        }
    }

    public static boolean createTable(DataSource dataSource, String tableName, List<LocalColumn> columns) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE ").append(tableName).append(" (");

        boolean first = true;
        for (LocalColumn column : columns) {
            if (!first) {
                createTableSql.append(", ");
            }
            createTableSql.append(column.getColumnName())
                    .append(" ")
                    .append(column.getDbType());
            first = false;
        }

        createTableSql.append(")");

        log.debug("Creating table with SQL: {}", createTableSql);

        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql.toString());
            return true;
        } catch (Exception e) {
            log.error("Failed to create table: {}", e.getMessage());
            throw new RuntimeException("Failed to create table", e);
        }
    }

    public static boolean insert(DataSource dataSource, String tableName, Object value, List<LocalColumn> columns) {
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(tableName).append(" (");

        // 构建列名部分
        boolean first = true;
        for (LocalColumn column : columns) {
            if (!first) {
                insertSql.append(", ");
            }
            insertSql.append(column.getColumnName());
            first = false;
        }

        insertSql.append(") VALUES (");

        // 构建参数占位符
        first = true;
        for (int i = 0; i < columns.size(); i++) {
            if (!first) {
                insertSql.append(", ");
            }
            insertSql.append("?");
            first = false;
        }

        insertSql.append(")");

        log.debug("Inserting data with SQL: {}", insertSql);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(insertSql.toString())) {

            // 设置参数值
            int paramIndex = 1;
            for (LocalColumn column : columns) {
                Field field = column.getField();
                field.setAccessible(true);
                Object fieldValue = field.get(value);
                stmt.setObject(paramIndex++, fieldValue);
            }

            return stmt.executeUpdate() > 0;
        } catch (Exception e) {
            log.error("Failed to insert data: {}", e.getMessage());
            throw new RuntimeException("Failed to insert data", e);
        }
    }

}

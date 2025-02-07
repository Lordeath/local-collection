package lordeath.local.collection.db.util;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lordeath.local.collection.db.bean.LocalColumn;
import lordeath.local.collection.db.bean.LocalColumnForMap;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据库工具类
 */
@Slf4j
public class DBUtil {
    /**
     * 添加数据
     *
     * @param obj        数据
     * @param tableName  表名
     * @param columns    列定义
     * @param dataSource 数据源
     * @param <T>        数据类型
     * @return 是否添加成功
     */
    public static <T> boolean add(T obj, String tableName, List<LocalColumn> columns, DataSource dataSource) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        for (LocalColumn column : columns) {
            sql.append(column.getColumnName()).append(", ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            sql.append("?, ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(")");
        log.debug("插入数据的sql: {}", sql);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            int parameterIndex = 1;
            for (LocalColumn column : columns) {
                if (column.getField() == null && columns.size() == 1) {
                    // 特殊情况：如果只有一个列且field为null，直接使用对象本身作为值
                    stmt.setObject(parameterIndex++, obj);
                } else {
                    Field field = column.getField();
                    field.setAccessible(true);
                    Object value = field.get(obj);
                    stmt.setObject(parameterIndex++, value);
                }
            }
            return stmt.executeUpdate() > 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量添加数据
     *
     * @param c          数据
     * @param tableName  表名
     * @param columns    列定义
     * @param dataSource 数据源
     * @param <T>        数据类型
     * @return 是否添加成功
     */
    public static <T> boolean addAll(Collection<? extends T> c, String tableName, List<LocalColumn> columns, DataSource dataSource) {
        if (c == null || c.isEmpty()) {
            return true;
        }
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        for (LocalColumn column : columns) {
            sql.append(column.getColumnName()).append(", ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            sql.append("?, ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(")");
        log.debug("批量插入数据的sql: {}", sql);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            connection.setAutoCommit(false);
            for (T obj : c) {
                int parameterIndex = 1;
                for (LocalColumn column : columns) {
                    if (column.getField() == null && columns.size() == 1) {
                        // 特殊情况：如果只有一个列且field为null，直接使用对象本身作为值
                        stmt.setObject(parameterIndex++, obj);
                    } else {
                        Field field = column.getField();
                        field.setAccessible(true);
                        Object value = field.get(obj);
                        stmt.setObject(parameterIndex++, value);
                    }
                }
                stmt.addBatch();
            }
            int[] results = stmt.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
            return Arrays.stream(results).allMatch(result -> result > 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除数据
     *
     * @param index        索引
     * @param tableName    表名
     * @param pkColumnName 主键列名
     * @param columns      列定义
     * @param dataSource   数据源
     * @param clazz        数据类型
     * @param <T>          数据类型
     * @return 删除的数据
     */
    public static <T> T remove(int index, String tableName, String pkColumnName, List<LocalColumn> columns, DataSource dataSource, Class<T> clazz) {
        // 先获取要删除的数据
        T obj = get(index, tableName, columns, pkColumnName, dataSource, clazz, true);
        if (obj == null) {
            return null;
        }
        // 获取主键值
        Long pk = pk(index, tableName, pkColumnName, dataSource);
        // 删除数据
        String sql = "DELETE FROM " + tableName + " WHERE " + pkColumnName + " = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, pk);
            stmt.executeUpdate();
            return obj;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取数据
     *
     * @param <T>          数据类型
     * @param index        索引
     * @param tableName    表名
     * @param columns      列定义
     * @param pkColumnName 主键列名
     * @param dataSource   数据源
     * @param clazz        数据类型
     * @param removeFlag
     * @return 数据
     */
    public static <T> T get(int index, String tableName, List<LocalColumn> columns, String pkColumnName, DataSource dataSource, Class<T> clazz, boolean removeFlag) {
        if (removeFlag || pkColumnName == null) {
            // 通过主键进行排序，然后查询，limit offset 1
            // 拼接sql
            StringBuilder sql = new StringBuilder("select * from ")
                    .append(tableName);
            if (pkColumnName != null) {
                sql.append(" order by ").append(pkColumnName);
            }
            sql.append(" limit 1 offset ").append(index);
            log.debug("查询数据的sql: {}", sql);
            return DBUtil.querySingle(dataSource, sql.toString(), columns, clazz);
        } else {
            // 如果主键没有修改过，也就是说没有被移除过，那说明get是可以直接用index来作为pk的
            // 注意下标要加1，因为下标从0开始，而自增从1开始
            StringBuilder sql = new StringBuilder("select * from ").append(tableName);
            sql.append(" where ").append(pkColumnName).append(" = ").append(index + 1);
            log.debug("查询数据的sql: {}", sql);
            return DBUtil.querySingle(dataSource, sql.toString(), columns, clazz);
        }
    }

    /**
     * 批量查询数据
     *
     * @param fromIndex    起始索引(包含)
     * @param toIndex      结束索引(不包含)
     * @param tableName    表名
     * @param columns      列定义
     * @param pkColumnName 主键列名
     * @param dataSource   数据源
     * @param clazz        数据类型
     * @param <T>          数据类型
     * @return 数据
     */
    public static <T> List<T> batchQuery(int fromIndex, int toIndex, String tableName, List<LocalColumn> columns,
                                         String pkColumnName, DataSource dataSource, Class<T> clazz) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ")
                .append(tableName)
                .append(" ORDER BY ")
                .append(pkColumnName)
                .append(" LIMIT ? OFFSET ?");

        log.debug("批量查询数据的sql: {}", sql);

        List<T> result = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            stmt.setInt(1, toIndex - fromIndex);
            stmt.setInt(2, fromIndex);
            try (ResultSet resultSet = stmt.executeQuery()) {
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
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * 执行sql
     *
     * @param dataSource 数据源
     * @param sql        sql
     * @return 执行结果
     */
    public static boolean executeSql(DataSource dataSource, String sql) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 查询单个数据
     *
     * @param dataSource 数据源
     * @param sql        sql
     * @param columns    列定义
     * @param clazz      数据类型
     * @param <T>        数据类型
     * @return 数据
     */
    public static <T> T querySingle(DataSource dataSource, String sql, List<LocalColumn> columns, Class<T> clazz) {
        // 通过数据源进行查询，返回一个对象
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            if (columns.size() == 2 && columns.get(1).getField() == null && columns.get(0).getColumnName().startsWith("key_")) {
                // map来获取列时，直接使用列的名称即可
                if (resultSet.next()) {
                    if (clazz == String.class) {
                        return (T) resultSet.getString(columns.get(1).getColumnName());
                    } else if (clazz == Integer.class) {
                        return (T) Integer.valueOf(resultSet.getInt(columns.get(1).getColumnName()));
                    } else if (clazz == Long.class) {
                        return (T) Long.valueOf(resultSet.getLong(columns.get(1).getColumnName()));
                    } else if (clazz == Double.class) {
                        return (T) Double.valueOf(resultSet.getDouble(columns.get(1).getColumnName()));
                    } else if (clazz == Float.class) {
                        return (T) Float.valueOf(resultSet.getFloat(columns.get(1).getColumnName()));
                    } else if (clazz == Boolean.class) {
                        return (T) Boolean.valueOf(resultSet.getBoolean(columns.get(1).getColumnName()));
                    } else if (clazz == Character.class) {
                        return (T) Character.valueOf(resultSet.getString(columns.get(1).getColumnName()).charAt(0));
                    } else if (clazz == Date.class) {
                        Object dateInDb = resultSet.getObject(columns.get(1).getColumnName());
                        return (T) convertObjectToDate(dateInDb);
                    } else if (clazz == BigDecimal.class) {
                        return (T) resultSet.getBigDecimal(columns.get(1).getColumnName());
                    } else {
                        throw new RuntimeException("不支持的类型: " + clazz);
                    }
                }
                return null;
            }
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
                    } else if (clazz == Date.class) {
                        Object dateInDb = resultSet.getObject(columns.get(0).getColumnName());
                        return (T) convertObjectToDate(dateInDb);
                    } else if (clazz == BigDecimal.class) {
                        return (T) resultSet.getBigDecimal(columns.get(0).getColumnName());
                    } else {
                        throw new RuntimeException("不支持的类型: " + clazz);
                    }
                }
                return null;
            } else {
                if (resultSet.next()) {
                    T t = clazz.getDeclaredConstructor().newInstance();
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
                        } else if (column.getColumnType() == Date.class) {
                            Object dateInDb = resultSet.getObject(column.getColumnName());
                            Date date = convertObjectToDate(dateInDb);
                            field.set(t, date);
                        } else if (column.getColumnType() == BigDecimal.class) {
                            field.set(t, resultSet.getBigDecimal(column.getColumnName()));
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
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Date convertObjectToDate(Object dateInDb) {
        Date date = null;
        if (dateInDb instanceof Integer) {
            date = new Date(((Integer) dateInDb));
        } else if (dateInDb instanceof Long) {
            date = new Date(((Long) dateInDb));
        } else if (dateInDb instanceof Date) {
            date = (Date) dateInDb;
        }
        return date;
    }

    /**
     * 查询数据量
     *
     * @param tableName  表名
     * @param dataSource 数据源
     * @return 数据量
     */
    public static Integer size(String tableName, DataSource dataSource) {
        // 查询表的数据量
        StringBuilder sql = new StringBuilder("select count(*) AS count from ").append(tableName);
        log.debug("查询数据量的sql: {}", sql);
        return DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn("count", Integer.class, "INT", null)), Integer.class);
    }


    public static final AtomicInteger dropTableCounter = new AtomicInteger(0);

    /**
     * 删除表
     *
     * @param tableName  表名
     * @param dataSource 数据源
     */
    public static void drop(String tableName, DataSource dataSource) {
        // 删除表的数据
        StringBuilder sql = new StringBuilder("drop table ").append(tableName);
        log.warn("删除表的sql: {}", sql);
        DBUtil.executeSql(dataSource, sql.toString());
        dropTableCounter.incrementAndGet();
    }

    /**
     * 清空表
     *
     * @param tableName  表名
     * @param dataSource 数据源
     */
    public static void clear(String tableName, DataSource dataSource) {
        String sql = "DELETE FROM " + tableName;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 更新数据
     *
     * @param index        索引
     * @param element      数据
     * @param tableName    表名
     * @param columns      列定义
     * @param pkColumnName 主键列名
     * @param dataSource   数据源
     * @param <T>          数据类型
     * @return 数据
     */
    public static <T> T set(int index, T element, String tableName, List<LocalColumn> columns, String pkColumnName, DataSource dataSource) {
        // 获取主键值
        Long pk = pk(index, tableName, pkColumnName, dataSource);

        // 更新数据
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        for (LocalColumn column : columns) {
            sql.append(column.getColumnName()).append(" = ?, ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(" WHERE ").append(pkColumnName).append(" = ?");

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            int parameterIndex = 1;
            for (LocalColumn column : columns) {
                if (column.getField() == null && columns.size() == 1) {
                    // 特殊情况：如果只有一个列且field为null，直接使用对象本身作为值
                    stmt.setObject(parameterIndex++, element);
                } else {
                    Field field = column.getField();
                    field.setAccessible(true);
                    Object value = field.get(element);
                    stmt.setObject(parameterIndex++, value);
                }
            }
            stmt.setLong(parameterIndex, pk);
            stmt.executeUpdate();
            return element;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取主键
     *
     * @param index        索引
     * @param tableName    表名
     * @param pkColumnName 主键列名
     * @param dataSource   数据源
     * @return 主键
     */
    public static long pk(int index, String tableName, String pkColumnName, DataSource dataSource) {
        // 查出原有的数据的id，然后进行更新
        StringBuilder sql = new StringBuilder("SELECT ").append(pkColumnName).append(" FROM ")
                .append(tableName).append(" ORDER BY ").append(pkColumnName).append(" LIMIT 1 OFFSET ").append(index);
        log.debug("查询数据的id的sql: {}", sql);
        Long id = DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn(pkColumnName, Long.class, "BIGINT", null)), Long.class);
        if (id == null) {
            throw new RuntimeException("没有找到对应的数据");
        }
        return id;
    }

    /**
     * setParameters
     *
     * @param stmt   PreparedStatement
     * @param params 参数
     * @throws SQLException 异常
     */
    private static void setParameters(PreparedStatement stmt, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            stmt.setObject(i + 1, params[i]);
        }
    }

    private static <T> T createInstance(ResultSet rs, List<LocalColumn> columns, Class<T> clazz)
            throws Exception {
        if (columns.size() == 2 && columns.get(1).getField() == null && columns.get(0).getColumnName().startsWith("key_")) {
            return (T) rs.getObject(columns.get(1).getColumnName());
        }
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

    /**
     * 创建分组表
     *
     * @param dataSource    数据源
     * @param tableName     表名
     * @param keyColumn     key列名
     * @param resultColumns 结果列
     * @return 是否创建成功
     */
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

    /**
     * 获取SQL类型
     *
     * @param javaType Java类型
     * @return SQL类型
     */
    public static String getSqlType(Class<?> javaType) {
        String VARCHAR = getSqlTypeOrNull(javaType);
        if (VARCHAR != null) {
            return VARCHAR;
        }
        throw new UnsupportedOperationException("不支持的数据类型，请联系开发: " + javaType);
    }

    /**
     * 根据给定的Java类型返回对应的SQL类型，如果未找到则返回null。
     *
     * @param javaType 要检查的Java类型
     * @return SQL类型的字符串，如果未找到则返回null
     */
    public static String getSqlTypeOrNull(Class<?> javaType) {
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
        return null;
    }

    /**
     * 插入分组数据
     *
     * @param dataSource       数据源
     * @param sourceTableName  源表名
     * @param targetTableName  目标表名
     * @param groupByColumns   分组列
     * @param whereClause      where条件
     * @param columnForMapList 列映射
     * @return 是否插入成功
     */
    public static boolean insertGroupedData(DataSource dataSource, String sourceTableName,
                                            String targetTableName, List<String> groupByColumns,
                                            String whereClause,
                                            List<LocalColumnForMap> columnForMapList) {
        // 构建INSERT语句
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(targetTableName).append(" (");

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

        List<Object> params = new ArrayList<>();
        StringBuilder whereBuilder = new StringBuilder();
        if (whereClause != null && !whereClause.isEmpty()) {
            whereBuilder.append(" WHERE ");
            // 解析where子句中的参数，将其替换为?
            String[] parts = whereClause.split("\\s+");
            for (String part : parts) {
                if (part.startsWith("'") && part.endsWith("'")) {
                    params.add(part.substring(1, part.length() - 1));
                    whereBuilder.append("? ");
                } else {
                    whereBuilder.append(part).append(" ");
                }
            }
            insertSql.append(whereBuilder);
        }

        if (!groupByColumns.isEmpty()) {
            insertSql.append(" GROUP BY ").append(String.join(", ", groupByColumns));
        }

        log.debug("Inserting data with SQL: {}", insertSql);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(insertSql.toString())) {

            // 设置where子句中的参数
            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            return stmt.executeUpdate() > 0;
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert grouped data", e);
        }
    }

    /**
     * 获取对象
     *
     * @param dataSource 数据源
     * @param tableName  表名
     * @param keyColumn  key列名
     * @param keyValue   key值
     * @param columns    列定义
     * @param clazz      类型
     * @param <T>        类型
     * @return 对象
     */
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

    /**
     * 根据给定的键向指定表中添加或更新对象。
     *
     * @param <K>        键类型
     * @param <V>        对象类型
     * @param dataSource 数据源
     * @param tableName  表名
     * @param keyColumn  key列名
     * @param key        键值
     * @param value      对象值
     * @param columns    列定义
     * @param removed    是否被移除，值在方法里面更新
     * @return 对象值
     */
    public static <K, V> V putByKey(DataSource dataSource, String tableName, String keyColumn, K key, V value, List<LocalColumn> columns, AtomicBoolean removed) {
        // update
        boolean removedBoolean = removeByKey(dataSource, tableName, keyColumn, key);
        removed.set(removedBoolean);
        addByKey(key, value, tableName, columns, dataSource, keyColumn);
        return value;
    }

    /**
     * 添加数据
     *
     * @param <K>        键的类型
     * @param <V>        对象的类型
     * @param key        键值
     * @param obj        对象值
     * @param tableName  表名
     * @param columns    列定义
     * @param dataSource 数据源
     * @param keyColumn  键列的名称
     * @return 是否添加成功
     */
    public static <K, V> boolean addByKey(K key, V obj, String tableName, List<LocalColumn> columns, DataSource dataSource, String keyColumn) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        for (LocalColumn column : columns) {
            sql.append(column.getColumnName()).append(", ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            sql.append("?, ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(")");
        log.debug("插入数据的sql: {}", sql);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            int parameterIndex = 1;
            for (LocalColumn column : columns) {
                if (column.getField() == null && columns.size() == 1) {
                    // 特殊情况：如果只有一个列且field为null，直接使用对象本身作为值
                    stmt.setObject(parameterIndex++, obj);
                } else if (column.getField() == null && columns.size() == 2 && !keyColumn.equals(column.getColumnName())) {
                    // 特殊情况：如果只有2个列且field为null，直接使用对象本身作为值
                    stmt.setObject(parameterIndex++, obj);
                } else {
                    if (keyColumn.equals(column.getColumnName())) {
                        stmt.setObject(parameterIndex++, key);
                    } else {
                        Field field = column.getField();
                        field.setAccessible(true);
                        Object value = field.get(obj);
                        stmt.setObject(parameterIndex++, value);
                    }
                }
            }
            return stmt.executeUpdate() > 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 通过key删除对象
     *
     * @param dataSource 数据源
     * @param tableName  表名
     * @param keyColumn  key列名
     * @param keyValue   key值
     * @return 是否删除成功
     */
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

    /**
     * 获取所有key
     *
     * @param dataSource 数据源
     * @param tableName  表名
     * @param keyColumn  key列名
     * @return key列表
     */
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
}

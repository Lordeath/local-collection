package com.fxm.local.collection.db.util;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

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
        sql.append(";");
        log.info("批量添加数据的sql: {}", sql);
        // 执行sql
        return DBUtil.executeSql(dataSource, sql.toString());
    }

    public static <T> T remove(int index, String tableName, String pkColumnName, List<LocalColumn> columns, DataSource dataSource, Class<T> clazz) {
        // 通过index找到id，然后通过id进行删除
        Long id;
        StringBuilder sql = new StringBuilder("select ").append(pkColumnName).append(" from ")
                .append(tableName).append(" order by ").append(pkColumnName).append(" limit 1 offset ").append(index).append(";");
        log.info("查询数据的id的sql: {}", sql);
        id = DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn(pkColumnName, Long.class, "BIGINT", null)), Long.class);
        if (id == null) {
            throw new RuntimeException("没有找到对应的数据");
        }
        // 拼接sql
        sql = new StringBuilder("delete from ").append(tableName).append(" where ").append(pkColumnName).append(" = ").append(id).append(";");
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
                .append(tableName).append(" order by ").append(pkColumnName).append(" limit 1 offset ").append(index).append(";");
        log.info("查询数据的sql: {}", sql);
        return DBUtil.querySingle(dataSource, sql.toString(), columns, clazz);
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
        StringBuilder sql = new StringBuilder("select count(*) AS count from ").append(tableName).append(";");
        log.info("查询数据量的sql: {}", sql);
        return DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn("count", Integer.class, "INT", null)), Integer.class);
    }

    public static void extracted(String tableName, DataSource dataSource) {
        // 删除表的数据
        StringBuilder sql = new StringBuilder("drop table ").append(tableName).append(";");
        log.info("删除表的sql: {}", sql);
        DBUtil.executeSql(dataSource, sql.toString());
    }

    public static void clear(String tableName, DataSource dataSource) {
        // 删除表的数据
        StringBuilder sql = new StringBuilder("truncate table ").append(tableName).append(";");
        log.info("清空表的sql: {}", sql);
        DBUtil.executeSql(dataSource, sql.toString());
    }

    public static <T> T set(int index, T element, String tableName, List<LocalColumn> columns, String pkColumnName, DataSource dataSource) {
        // 查出原有的数据的id，然后进行更新
        StringBuilder sql = new StringBuilder("select ").append(pkColumnName).append(" from ")
                .append(tableName).append(" order by ").append(pkColumnName).append(" limit 1 offset ").append(index).append(";");
        log.info("查询数据的id的sql: {}", sql);
        Long id = DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn(pkColumnName, Long.class, "BIGINT", null)), Long.class);
        if (id == null) {
            throw new RuntimeException("没有找到对应的数据");
        }
        // 拼接sql
        sql = new StringBuilder("update ").append(tableName).append(" set ");
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
                sql.append(column.getColumnName()).append(" = '").append(value).append("', ");
            }
        }
        sql.setLength(sql.length() - 2);
        sql.append(" where ").append(pkColumnName).append(" = ").append(id).append(";");
        log.info("更新数据的sql: {}", sql);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());
        return element;
    }

    public static long pk(int index, String tableName, String pkColumnName, DataSource dataSource) {
        // 查出原有的数据的id，然后进行更新
        StringBuilder sql = new StringBuilder("select ").append(pkColumnName).append(" from ")
                .append(tableName).append(" order by ").append(pkColumnName).append(" limit 1 offset ").append(index).append(";");
        log.info("查询数据的id的sql: {}", sql);
        Long id = DBUtil.querySingle(dataSource, sql.toString(), Lists.newArrayList(new LocalColumn(pkColumnName, Long.class, "BIGINT", null)), Long.class);
        if (id == null) {
            throw new RuntimeException("没有找到对应的数据");
        }
        return id;
    }
}

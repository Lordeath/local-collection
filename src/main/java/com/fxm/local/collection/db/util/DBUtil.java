package com.fxm.local.collection.db.util;

import com.fxm.local.collection.db.bean.LocalColumn;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

@Slf4j
public class DBUtil {
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
}

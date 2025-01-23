package com.fxm.local.collection.db.util;

import com.fxm.local.collection.db.bean.LocalColumn;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ColumnNameUtil {
    public static List<LocalColumn> getFields(Class<?> clazz) {
        // 通过class的反射获取到字段名称以及类型
        LocalColumn simpleColumn = getLocalColumns(clazz, "simpleColumn", null);
        if (simpleColumn != null) {
            List<LocalColumn> newArrayList = new ArrayList<>();
            newArrayList.add(simpleColumn);
            return newArrayList;
        }
        List<LocalColumn> list = new ArrayList<>();
        for (Field field : FieldUtils.getAllFields(clazz)) {
            LocalColumn localColumn = getLocalColumns(field.getType(), field.getName(), field);
            if (localColumn != null) {
                list.add(localColumn);
            } else {
                throw new RuntimeException("不支持的类型: " + field.getType());
            }
        }
        return list;
    }

    private static LocalColumn getLocalColumns(Class<?> clazz, String fieldName, Field field) {
        if (clazz == String.class) {
            return new LocalColumn(fieldName, String.class, "VARCHAR", field);
        }
        if (clazz == Integer.class) {
            return new LocalColumn(fieldName, Integer.class, "INT", field);
        }
        if (clazz == Long.class) {
            return new LocalColumn(fieldName, Long.class, "BIGINT", field);
        }
        if (clazz == Double.class) {
            return new LocalColumn(fieldName, Double.class, "DOUBLE", field);
        }
        if (clazz == Float.class) {
            return new LocalColumn(fieldName, Float.class, "FLOAT", field);
        }
        if (clazz == Boolean.class) {
            return new LocalColumn(fieldName, Boolean.class, "BOOLEAN", field);
        }
        if (clazz == Character.class) {
            return new LocalColumn(fieldName, Character.class, "CHAR", field);
        }
        return null;
    }
}

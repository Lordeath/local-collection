package lordeath.local.collection.db.util;

import lordeath.local.collection.db.bean.LocalColumn;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 */
public class ColumnNameUtil {
    /**
     * 获取字段
     *
     * @param clazz 类
     * @return 字段列表
     */
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

    /**
     * 获取字段
     *
     * @param clazz     类
     * @param fieldName 字段名称
     * @param field     字段
     * @return 字段
     */
    private static LocalColumn getLocalColumns(Class<?> clazz, String fieldName, Field field) {
        // 我担心不同的jdk的适配性，所以这里转换成String来做比较
        String className = clazz.getName();
        switch (className) {
            case "java.lang.String":
                return new LocalColumn(fieldName, String.class, "VARCHAR", field);
            case "int":
            case "java.lang.Integer":
                return new LocalColumn(fieldName, Integer.class, "INT", field);
            case "long":
            case "java.lang.Long":
                return new LocalColumn(fieldName, Long.class, "BIGINT", field);
            case "double":
            case "java.lang.Double":
                return new LocalColumn(fieldName, Double.class, "DOUBLE", field);
            case "float":
            case "java.lang.Float":
                return new LocalColumn(fieldName, Float.class, "FLOAT", field);
            case "boolean":
            case "java.lang.Boolean":
                return new LocalColumn(fieldName, Boolean.class, "BOOLEAN", field);
            case "char":
            case "java.lang.Character":
                return new LocalColumn(fieldName, Character.class, "CHAR", field);
            case "date":
            case "java.sql.Date":
            case "java.util.Date":
                return new LocalColumn(fieldName, Date.class, "DATETIME", field);
            case "java.math.BigDecimal":
//                return new LocalColumn(fieldName, BigDecimal.class, "decimal(32,8)", field);
                return new LocalColumn(fieldName, BigDecimal.class, "VARCHAR", field);
            default:
                return null;
        }
    }
}

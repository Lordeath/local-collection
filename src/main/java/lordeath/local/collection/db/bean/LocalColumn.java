package lordeath.local.collection.db.bean;

import lombok.Data;

import java.lang.reflect.Field;

/**
 * 数据库列定义类
 * 用于定义数据库表的列名、类型等信息
 */
@Data
public class LocalColumn {
    private final String columnName;
    private final Class<?> columnType;
    private final String dbType;
    // 用于反射获取字段值
    private final Field field;
}

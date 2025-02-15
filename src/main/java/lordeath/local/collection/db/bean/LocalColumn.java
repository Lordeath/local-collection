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

    /**
     * 数据库列定义类
     * 用于定义数据库表的列名、类型等信息
     *
     * @param columnName 列名
     * @param columnType java的类型
     * @param dbType     数据库里面的字段类型
     * @param field      用于反射的字段
     */
    public LocalColumn(String columnName, Class<?> columnType, String dbType, Field field) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.dbType = dbType;
        this.field = field;
    }
}

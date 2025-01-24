package lordeath.local.collection.db.bean;

import lombok.Data;

import java.lang.reflect.Field;

@Data
public class LocalColumn {
    private final String columnName;
    private final Class<?> columnType;
    private final String dbType;
    // 用于反射获取字段值
    private final Field field;
}

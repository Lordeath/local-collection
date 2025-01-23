package com.fxm.local.collection.db.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.lang.reflect.Field;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LocalColumn {
    private String columnName;
    private Class<?> columnType;
    private String dbType;
    // 用于反射获取字段值
    private Field field;
}

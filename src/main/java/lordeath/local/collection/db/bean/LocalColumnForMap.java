package lordeath.local.collection.db.bean;

import lombok.Data;

/**
 * 数据库列映射定义类
 * 用于定义源表和目标表之间的列映射关系
 */
@Data
public class LocalColumnForMap {
    // 目标的对象的相关字段信息
    private LocalColumn sinkColumn;
    // 表达式，比如 avg(age) AS age
    private String expression;
    private boolean isKey = false;

    /**
     * 数据库列映射定义类
     * 用于定义源表和目标表之间的列映射关系
     */
    public LocalColumnForMap() {
    }
}
